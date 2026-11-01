using System.Diagnostics;
using System.Text;
using RabbitMQ.Client;

// 24: memory and disk alarms block every publishing connection
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// connection named "alarm-demo" (visible in rabbitmqctl list_connections)
await using var connection = await factory.CreateConnectionAsync("alarm-demo");

// clock for the "ms" column
var clock = Stopwatch.StartNew();

// completed when the broker blocks the connection
var blocked = new TaskCompletionSource();

// completed when the broker unblocks it
var unblocked = new TaskCompletionSource();

// the broker sent connection.blocked: publishes wait (Reason = "low on memory" / "low on disk")
connection.ConnectionBlockedAsync += (_, ea) =>
{
    Console.WriteLine($"  {clock.ElapsedMilliseconds,5} ms  EVENT connection.blocked: {ea.Reason}");

    blocked.TrySetResult();

    return Task.CompletedTask;
};

// the alarm cleared: waiting publishes continue
connection.ConnectionUnblockedAsync += (_, _) =>
{
    Console.WriteLine($"  {clock.ElapsedMilliseconds,5} ms  EVENT connection.unblocked");

    unblocked.TrySetResult();

    return Task.CompletedTask;
};

// confirms ON: Publish() returns only when the broker stored the message
var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

// channel in confirm mode
await using var channel = await connection.CreateChannelAsync(options);

// start clean
await channel.QueueDeleteAsync("alarms");

// durable queue for the 3 test messages
await channel.QueueDeclareAsync("alarms", durable: true, exclusive: false, autoDelete: false);

// no alarm: confirmed in a few ms
await Publish("1) normal");

// ---- memory alarm
Console.WriteLine("\nrabbitmqctl set_vm_memory_high_watermark absolute 1MB");

// memory limit 1 MB, below current use -> memory alarm (demo only)
Rabbitmqctl("set_vm_memory_high_watermark", "absolute", "1MB");

// start a publish; it can't finish while the connection is blocked
var stuck = Publish("2) during memory alarm");

// wait for the connection.blocked event (max 10 s)
await blocked.Task.WaitAsync(TimeSpan.FromSeconds(10));

// connection state is now "blocked"
Console.WriteLine($"  connection state: {Rabbitmqctl("list_connections", "state", "--no-table-headers")}");

// active alarms: memory on rabbit@rabbitmq
Console.WriteLine($"  alarms: {Rabbitmqctl("eval", "rabbit_alarm:get_alarms().")}");

// 2 s later the publish is still waiting
await Task.Delay(2000);

Console.WriteLine($"  publish 2 completed after 2 s? {stuck.IsCompleted}");

Console.WriteLine("\nrabbitmqctl set_vm_memory_high_watermark 0.6");

// back to 60% of RAM -> alarm cleared
Rabbitmqctl("set_vm_memory_high_watermark", "0.6");

// wait for connection.unblocked
await unblocked.Task.WaitAsync(TimeSpan.FromSeconds(10));

// now publish 2 gets its confirm
await stuck;

// ---- disk alarm
// fresh tasks for the disk alarm
blocked = new TaskCompletionSource();

unblocked = new TaskCompletionSource();

Console.WriteLine("\nrabbitmqctl set_disk_free_limit 1000GB");

// require 1000 GB free disk -> disk alarm
Rabbitmqctl("set_disk_free_limit", "1000GB");

// this publish waits like the memory one
var stuckOnDisk = Publish("3) during disk alarm");

// wait for connection.blocked (max 30 s)
await blocked.Task.WaitAsync(TimeSpan.FromSeconds(30));

Console.WriteLine("\nrabbitmqctl set_disk_free_limit 50MB");

// back to the default 50 MB -> alarm cleared
Rabbitmqctl("set_disk_free_limit", "50MB");

// wait for connection.unblocked
await unblocked.Task.WaitAsync(TimeSpan.FromSeconds(30));

// publish 3 gets its confirm
await stuckOnDisk;

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
var stats = await channel.QueueDeclarePassiveAsync("alarms");

Console.WriteLine($"\nqueue 'alarms' holds {stats.MessageCount} messages (nothing lost, only delayed)");

// clean up
await channel.QueueDeleteAsync("alarms");

async Task Publish(string label)
{
    var started = clock.ElapsedMilliseconds;

    Console.WriteLine($"  {started,5} ms  publish {label} ...");

    // confirm mode: this await blocks while an alarm is active
    await channel.BasicPublishAsync("", "alarms", mandatory: true, basicProperties: new BasicProperties { Persistent = true }, body: Encoding.UTF8.GetBytes(label));

    Console.WriteLine($"  {clock.ElapsedMilliseconds,5} ms  publish {label} confirmed (waited {clock.ElapsedMilliseconds - started} ms)");
}

// run "docker exec rabbitmq rabbitmqctl -q <args>" and return its stdout
static string Rabbitmqctl(params string[] arguments)
{
    var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };

    foreach (var argument in new[] { "exec", "rabbitmq", "rabbitmqctl", "-q" }.Concat(arguments))
        info.ArgumentList.Add(argument);

    using var process = Process.Start(info)!;

    var output = process.StandardOutput.ReadToEnd().Trim();

    process.WaitForExit();

    return output;
}
