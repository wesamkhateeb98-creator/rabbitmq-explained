using System.Diagnostics;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// 13: publisher confirms - the broker acks (or nacks) every publish
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// with tracking enabled, BasicPublishAsync waits for the confirm and throws on nack/return
var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

// channel in confirm mode
await using var channel = await connection.CreateChannelAsync(options);

// start clean: delete queues from a previous run
await channel.QueueDeleteAsync("confirms");

await channel.QueueDeleteAsync("confirms.full");

// durable queue for the normal and throughput tests
await channel.QueueDeclareAsync("confirms", durable: true, exclusive: false, autoDelete: false);

// max 2 messages; "reject-publish" = refuse new ones (nack) instead of dropping the oldest
var limit = new Dictionary<string, object?> { ["x-max-length"] = 2, ["x-overflow"] = "reject-publish" };

// the queue that fills up after 2 messages
await channel.QueueDeclareAsync("confirms.full", durable: true, exclusive: false, autoDelete: false, arguments: limit);

// Persistent = true: written to disk before the broker acks
var persistent = new BasicProperties { Persistent = true };

// 1) normal publish -> ack
// returns when the broker acks m1
await channel.BasicPublishAsync("", "confirms", mandatory: true, basicProperties: persistent, body: Encoding.UTF8.GetBytes("m1"));

Console.WriteLine("1) publish to 'confirms'            -> ack");

// 2) queue full with reject-publish -> nack
for (var i = 1; i <= 3; i++)
{
    try
    {
        // f1, f2 -> ack; f3 -> queue full -> nack -> throws PublishException
        await channel.BasicPublishAsync("", "confirms.full", mandatory: true, basicProperties: persistent, body: Encoding.UTF8.GetBytes($"f{i}"));

        Console.WriteLine($"2) publish f{i} to 'confirms.full' (max 2) -> ack");
    }
    catch (PublishException ex)
    {
        Console.WriteLine($"2) publish f{i} to 'confirms.full' (max 2) -> nack ({ex.GetType().Name}, seq {ex.PublishSequenceNumber})");
    }
}

// 3) mandatory + no matching queue -> returned
try
{
    // mandatory + no queue with this name -> returned -> throws PublishReturnException
    await channel.BasicPublishAsync("", "no-such-queue", mandatory: true, basicProperties: persistent, body: Encoding.UTF8.GetBytes("lost?"));
}
catch (PublishReturnException ex)
{
    Console.WriteLine($"3) publish to 'no-such-queue'       -> returned {ex.ReplyCode} {ex.ReplyText}");
}

// 4) throughput: await each confirm vs. await a batch of confirms
const int Count = 2_000;

// measure 2000 publishes, each awaited before the next
var watch = Stopwatch.StartNew();

// mandatory true = return the message if no queue matches; await = wait for this message's confirm
for (var i = 0; i < Count; i++)
    await channel.BasicPublishAsync("", "confirms", mandatory: true, basicProperties: persistent, body: Encoding.UTF8.GetBytes($"one-{i}"));

Console.WriteLine($"4) {Count} publishes, await one by one   -> {watch.ElapsedMilliseconds} ms");

watch.Restart();

var batch = new List<ValueTask>();

// start 2000 publishes without awaiting...
for (var i = 0; i < Count; i++)
    batch.Add(channel.BasicPublishAsync("", "confirms", mandatory: true, basicProperties: persistent, body: Encoding.UTF8.GetBytes($"batch-{i}")));

// ...then await all their confirms
foreach (var publish in batch)
    await publish;

Console.WriteLine($"4) {Count} publishes, await as a batch   -> {watch.ElapsedMilliseconds} ms");

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
var stats = await channel.QueueDeclarePassiveAsync("confirms");

Console.WriteLine($"   queue 'confirms' holds {stats.MessageCount} messages");
