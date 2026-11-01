using System.Diagnostics;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 26: single active consumer, priority queue, consumer timeout
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// admin channel: setup and publishing
await using var admin = await connection.CreateChannelAsync();

// ---- Part A: single active consumer - one consumer gets everything, the other is standby
Console.WriteLine("Part A: single active consumer");

// start clean
await admin.QueueDeleteAsync("sac.orders");

// x-single-active-consumer: only one consumer gets messages; the others wait as standby
await admin.QueueDeclareAsync("sac.orders", durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { ["x-single-active-consumer"] = true });

// two consumers, each on its own channel
// channel A has no "await using": it is closed by hand below to trigger the failover
var channelA = await connection.CreateChannelAsync();

// channel B: the standby consumer, closed at the end of the program
await using var channelB = await connection.CreateChannelAsync();

// first to subscribe = the active consumer
await Subscribe(channelA, "consumer-A");

// standby: receives nothing while A is alive
await Subscribe(channelB, "consumer-B");

// order-1..3 -> all to consumer-A
await PublishOrders(1, 3);

await Task.Delay(500);

Console.WriteLine("  consumer-A stops (channel closed)");

// A leaves -> the broker activates B
await channelA.CloseAsync();

// order-4..6 -> all to consumer-B
await PublishOrders(4, 6);

await Task.Delay(500);

// ---- Part B: priority queue - higher priority is delivered first
Console.WriteLine("\nPart B: priority queue (x-max-priority = 10)");

// start clean
await admin.QueueDeleteAsync("prio.jobs");

// x-max-priority 10: priority queue with levels 0..10
await admin.QueueDeclareAsync("prio.jobs", durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { ["x-max-priority"] = 10 });

// published in this order, each with its priority
(string Job, byte Priority)[] jobs = [("report (1)", 1), ("password reset (9)", 9), ("invoice (5)", 5), ("2FA code (9)", 9)];

// Priority in the message properties decides the delivery order
foreach (var (job, priority) in jobs)
    await admin.BasicPublishAsync("", "prio.jobs", mandatory: false, basicProperties: new BasicProperties { Priority = priority }, body: Encoding.UTF8.GetBytes(job));

Console.WriteLine($"  published in order : {string.Join(", ", jobs.Select(j => j.Job))}");

var order = new List<string>();

// read back: highest priority first, FIFO inside the same priority
while (await admin.BasicGetAsync("prio.jobs", autoAck: true) is { } message)
    order.Add(Encoding.UTF8.GetString(message.Body.Span));

Console.WriteLine($"  delivered in order : {string.Join(", ", order)}");

// ---- Part C: consumer timeout - an unacked delivery older than the timeout closes the channel
Console.WriteLine("\nPart C: consumer timeout (x-consumer-timeout = 5000 ms)");

// start clean
await admin.QueueDeleteAsync("timeout.jobs");

// x-consumer-timeout 5000: an unacked delivery older than 5 s closes the consumer's channel
await admin.QueueDeclareAsync("timeout.jobs", durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { ["x-consumer-timeout"] = 5000 });

// one job the consumer will never ack
await admin.BasicPublishAsync("", "timeout.jobs", Encoding.UTF8.GetBytes("slow job"));

// clock for the "ms" column
var clock = Stopwatch.StartNew();

// completed when the broker closes the channel
var closed = new TaskCompletionSource();

// channel for the consumer that never acks
await using var stuck = await connection.CreateChannelAsync();

// fires when the broker closes the channel (406 PRECONDITION_FAILED)
stuck.ChannelShutdownAsync += (_, ea) =>
{
    Console.WriteLine($"  {clock.ElapsedMilliseconds,6} ms  channel closed by broker: {ea.ReplyCode} {ea.ReplyText}");

    closed.TrySetResult();

    return Task.CompletedTask;
};

// consumer that receives but never acks
var forgetful = new AsyncEventingBasicConsumer(stuck);

forgetful.ReceivedAsync += (_, ea) =>
{
    Console.WriteLine($"  {clock.ElapsedMilliseconds,6} ms  got '{Encoding.UTF8.GetString(ea.Body.Span)}' and never ack it");

    return Task.CompletedTask;
};

// autoAck false: the delivery stays unacked -> timeout
await stuck.BasicConsumeAsync("timeout.jobs", autoAck: false, consumer: forgetful);

// the broker checks timeouts about once a minute, so wait up to 90 s
await closed.Task.WaitAsync(TimeSpan.FromSeconds(90));

// let the broker requeue the message
await Task.Delay(200);

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
Console.WriteLine($"  queue 'timeout.jobs' ready again: {(await admin.QueueDeclarePassiveAsync("timeout.jobs")).MessageCount} message");

// clean up
foreach (var queue in new[] { "sac.orders", "prio.jobs", "timeout.jobs" })
    await admin.QueueDeleteAsync(queue);

async Task Subscribe(IChannel channel, string name)
{
    // helper: consumer that prints and acks each order
    var consumer = new AsyncEventingBasicConsumer(channel);

    consumer.ReceivedAsync += async (_, ea) =>
    {
        Console.WriteLine($"  {name} <- {Encoding.UTF8.GetString(ea.Body.Span)}");

        // ack = done, delete it
        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
    };

    // subscribe to sac.orders; manual acks
    await channel.BasicConsumeAsync("sac.orders", autoAck: false, consumer: consumer);
}

async Task PublishOrders(int from, int to)
{
    // publish order-{from}..order-{to} via the default exchange
    for (var i = from; i <= to; i++)
        await admin.BasicPublishAsync("", "sac.orders", Encoding.UTF8.GetBytes($"order-{i}"));
}
