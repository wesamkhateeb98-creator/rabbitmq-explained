using System.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 29: measure how each setting changes throughput (msg/s) on this machine
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// one TCP connection for every scenario
await using var connection = await factory.CreateConnectionAsync();

// confirms ON: each publish task completes when the broker acks
var confirmOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

// "plain" channel: no confirms (fire and forget)
await using var plain = await connection.CreateChannelAsync();

// "confirmed" channel: publishes wait for broker acks
await using var confirmed = await connection.CreateChannelAsync(confirmOptions);

// message bodies: 100 B and 10 KB of zeros
var small = new byte[100];

var large = new byte[10_000];

Console.WriteLine($"machine: {Environment.ProcessorCount} cores, RabbitMQ in Docker on the same host\n");

Console.WriteLine($"{"publish scenario",-58} {"messages",8} {"msg/s",9}");

// each line = one publish test: queue type, message count, body, persistent, confirm style
await PublishScenario("1) classic, transient, no confirms", "classic", 20_000, small, persistent: false, confirm: Confirm.None);

await PublishScenario("2) classic, persistent, await each confirm", "classic", 2_000, small, persistent: true, confirm: Confirm.EachOne);

await PublishScenario("3) classic, persistent, confirms in windows of 1000", "classic", 20_000, small, persistent: true, confirm: Confirm.Window);

await PublishScenario("4) quorum,  persistent, confirms in windows of 1000", "quorum", 20_000, small, persistent: true, confirm: Confirm.Window);

await PublishScenario("5) stream,  persistent, confirms in windows of 1000", "stream", 20_000, small, persistent: true, confirm: Confirm.Window);

await PublishScenario("6) classic, persistent, windows of 1000, 10 KB body", "classic", 20_000, large, persistent: true, confirm: Confirm.Window);

Console.WriteLine($"\n{"consume scenario (manual ack, 20 000 waiting messages)",-58} {"prefetch",8} {"msg/s",9}");

// consume test: same 20 000 messages, 4 prefetch values
foreach (var prefetch in new ushort[] { 1, 10, 100, 1000 })
    await ConsumeScenario(prefetch, 20_000);

async Task PublishScenario(string name, string type, int count, byte[] body, bool persistent, Confirm confirm)
{
    // empty queue of the requested type
    var queue = await FreshQueue(type);

    // no confirms -> plain channel, otherwise the confirmed channel
    var channel = confirm == Confirm.None ? plain : confirmed;

    // persistent: true = written to disk before the ack, false = memory only
    var properties = new BasicProperties { Persistent = persistent };

    // publishes in flight, not yet awaited
    var window = new List<ValueTask>();

    // time the whole scenario
    var watch = Stopwatch.StartNew();

    for (var i = 0; i < count; i++)
    {
        // start the publish (not awaited yet)
        var publish = channel.BasicPublishAsync("", queue, mandatory: false, basicProperties: properties, body: body);

        // slowest: wait for each confirm before the next publish
        if (confirm == Confirm.EachOne)
        {
            await publish;
        }
        else
        {
            // windows: keep up to 1000 publishes in flight...
            window.Add(publish);

            // ...then await all their confirms together
            // await the last partial window
            if (window.Count == 1000)
                await Flush(window);
        }
    }

    await Flush(window);

    // without confirms, wait until the broker has really enqueued everything
    // passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
    if (confirm == Confirm.None)
        while ((await plain.QueueDeclarePassiveAsync(queue)).MessageCount < count)
            await Task.Delay(5);

    watch.Stop();

    Console.WriteLine($"{name,-58} {count,8} {count / watch.Elapsed.TotalSeconds,9:N0}");

    // clean up
    await plain.QueueDeleteAsync(queue);
}

async Task ConsumeScenario(ushort prefetch, int count)
{
    // fill a classic queue with "count" messages first
    var queue = await FreshQueue("classic");

    var properties = new BasicProperties { Persistent = false };

    var window = new List<ValueTask>();

    for (var i = 0; i < count; i++)
    {
        // fast fill: confirms in windows of 1000
        window.Add(confirmed.BasicPublishAsync("", queue, mandatory: false, basicProperties: properties, body: small));

        if (window.Count == 1000)
            await Flush(window);
    }

    await Flush(window);

    // consumer channel
    await using var channel = await connection.CreateChannelAsync();

    // prefetch = max unacked messages; the variable under test
    await channel.BasicQosAsync(0, prefetch, false);

    var received = 0;

    var done = new TaskCompletionSource();

    // consumer object: the broker pushes messages to it
    var consumer = new AsyncEventingBasicConsumer(channel);

    consumer.ReceivedAsync += async (_, ea) =>
    {
        // manual ack per message
        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

        if (++received == count)
            done.TrySetResult();
    };

    // time from subscribe until the last ack
    var watch = Stopwatch.StartNew();

    // start consuming; autoAck false = manual acks
    await channel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer);

    // wait until all messages are acked
    await done.Task;

    watch.Stop();

    Console.WriteLine($"{"",-58} {prefetch,8} {count / watch.Elapsed.TotalSeconds,9:N0}");

    // clean up
    await plain.QueueDeleteAsync(queue);
}

async Task<string> FreshQueue(string type)
{
    var name = $"perf.{type}";

    // helper: delete and recreate "perf.<type>"
    await plain.QueueDeleteAsync(name);

    // x-queue-type: "classic", "quorum" or "stream"
    var arguments = new Dictionary<string, object?> { ["x-queue-type"] = type };

    // durable: quorum queues and streams require it
    await plain.QueueDeclareAsync(name, durable: true, exclusive: false, autoDelete: false, arguments: arguments);

    return name;
}

// await every pending publish (= its confirm), then empty the window
static async Task Flush(List<ValueTask> window)
{
    foreach (var publish in window)
        await publish;

    window.Clear();
}

// None = no confirms, EachOne = await each, Window = await 1000 at a time
enum Confirm { None, EachOne, Window }
