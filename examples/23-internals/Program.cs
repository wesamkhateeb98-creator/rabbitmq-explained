using System.Diagnostics;
using RabbitMQ.Client;

// 23: inside the broker - Erlang processes and the message store
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// data folder inside the container (one subfolder per vhost)
const string Data = "/var/lib/rabbitmq/mnesia/rabbit@rabbitmq/msg_stores/vhosts";

// ---- Part A: connections, channels and queues are Erlang processes
Console.WriteLine("Part A: Erlang processes on the node");

// process count before opening anything
var baseline = ProcessCount();

Console.WriteLine($"  baseline                 : {baseline} processes");

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// count again: the difference = processes per connection
var afterConnection = ProcessCount();

Console.WriteLine($"  + 1 connection           : {afterConnection} (+{afterConnection - baseline})");

// keep the channels open while counting
var channels = new List<IChannel>();

// 50 channels on one connection
for (var i = 0; i < 50; i++)
    channels.Add(await connection.CreateChannelAsync());

// difference / 50 = processes per channel
var afterChannels = ProcessCount();

Console.WriteLine($"  + 50 channels            : {afterChannels} (+{afterChannels - afterConnection}, {(afterChannels - afterConnection) / 50.0:0.#} per channel)");

// 10 classic queues (each one is its own Erlang process)
for (var i = 0; i < 10; i++)
    await channels[0].QueueDeclareAsync($"internals.q{i}", durable: true, exclusive: false, autoDelete: false);

// difference / 10 = processes per queue
var afterQueues = ProcessCount();

Console.WriteLine($"  + 10 classic queues      : {afterQueues} (+{afterQueues - afterChannels}, {(afterQueues - afterChannels) / 10.0:0.#} per queue)");

// clean up the 10 queues
for (var i = 0; i < 10; i++)
    await channels[0].QueueDeleteAsync($"internals.q{i}");

// close the 50 channels
foreach (var channel in channels)
    await channel.CloseAsync();

// ---- Part B: where persistent messages are written
Console.WriteLine("\nPart B: on-disk size (KB) of the message store and the queue indexes");

// confirms ON: each publish waits for the broker ack
var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

// channel in confirm mode
await using var publisher = await connection.CreateChannelAsync(options);

foreach (var queue in new[] { "internals.small", "internals.large" })
{
    // start clean
    await publisher.QueueDeleteAsync(queue);

    // durable queue: persistent messages go to disk
    await publisher.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
}

// sizes before any message
PrintDisk("empty queues");

// 100 B < 4096 B -> stored inside the queue index
await Publish("internals.small", count: 1000, size: 100);

PrintDisk("+ 1000 x 100 B  -> internals.small");

// 10 KB >= 4096 B -> stored in the shared msg_store_persistent
await Publish("internals.large", count: 1000, size: 10_000);

PrintDisk("+ 1000 x 10 KB  -> internals.large");

// clean up
foreach (var queue in new[] { "internals.small", "internals.large" })
    await publisher.QueueDeleteAsync(queue);

async Task Publish(string queue, int count, int size)
{
    // Persistent = true: write to disk
    var persistent = new BasicProperties { Persistent = true };

    // "size" zero bytes
    var body = new byte[size];

    var batch = new List<ValueTask>();

    // start every publish without awaiting...
    for (var i = 0; i < count; i++)
        batch.Add(publisher.BasicPublishAsync("", queue, mandatory: false, basicProperties: persistent, body: body));

    // ...then await all the confirms
    foreach (var publish in batch)
        await publish;
}

void PrintDisk(string step)
{
    var store = Shell($"du -sk {Data}/*/msg_store_persistent | cut -f1");

    // disk size in KB of the per-queue indexes
    var indexes = Shell($"du -sk {Data}/*/queues | cut -f1");

    Console.WriteLine($"  {step,-34}: msg_store_persistent {store,6} KB | queues (indexes) {indexes,6} KB");
}

int ProcessCount() =>
    int.Parse(Docker("rabbitmqctl", "eval", "erlang:system_info(process_count)."));

// run a shell command inside the container
string Shell(string command) =>
    Docker("sh", "-c", command);

// run "docker exec rabbitmq <command>" and return its stdout
static string Docker(params string[] command)
{
    var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true };

    foreach (var argument in new[] { "exec", "rabbitmq" }.Concat(command))
        info.ArgumentList.Add(argument);

    using var process = Process.Start(info)!;

    var output = process.StandardOutput.ReadToEnd().Trim();

    process.WaitForExit();

    return output;
}
