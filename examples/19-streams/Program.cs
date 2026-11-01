using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 19: a stream is an append-only log: reading does not remove, consumers pick an offset
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// start clean
await channel.QueueDeleteAsync("events");

// x-queue-type "stream" = append-only log; reading does not delete
var arguments = new Dictionary<string, object?> { ["x-queue-type"] = "stream" };

// streams must be durable
await channel.QueueDeclareAsync("events", durable: true, exclusive: false, autoDelete: false, arguments: arguments);

// append event-0..event-4 (offsets 0..4)
for (var i = 0; i < 5; i++)
    await channel.BasicPublishAsync("", "events", Encoding.UTF8.GetBytes($"event-{i}"));

Console.WriteLine("appended event-0..event-4 to stream 'events'");

// offset "first" = from the beginning -> 5 events
await Read("first", expected: 5);

// offset 3 = start at the 4th event -> event-3, event-4
await Read(3L, expected: 2);

// read again from the start: the 5 events are still there
await Read("first", expected: 5);

async Task Read(object offset, int expected)
{
    // streams require manual ack and a prefetch
    await using var reader = await connection.CreateChannelAsync();

    // prefetch 100 (required for stream consumers over AMQP)
    await reader.BasicQosAsync(0, 100, false);

    var got = new List<string>();

    // completed when "expected" events are read
    var done = new TaskCompletionSource();

    // consumer object: the broker pushes messages to it
    var consumer = new AsyncEventingBasicConsumer(reader);

    // runs once per event
    consumer.ReceivedAsync += async (_, ea) =>
    {
        // the broker puts each message's offset in header x-stream-offset
        var position = ea.BasicProperties.Headers!["x-stream-offset"];

        got.Add($"{Encoding.UTF8.GetString(ea.Body.Span)}@{position}");

        // ack only moves the prefetch window; the event stays in the stream
        await reader.BasicAckAsync(ea.DeliveryTag, multiple: false);

        if (got.Count == expected)
            done.TrySetResult();
    };

    // where to start: "first", "last", "next" or an offset number
    var consumeArguments = new Dictionary<string, object?> { ["x-stream-offset"] = offset };

    // the offset goes in the consumer arguments
    await reader.BasicConsumeAsync("events", autoAck: false, consumerTag: "", noLocal: false, exclusive: false, arguments: consumeArguments, consumer: consumer);

    // wait max 5 s
    await done.Task.WaitAsync(TimeSpan.FromSeconds(5));

    Console.WriteLine($"  consumer offset={offset,-5} -> {string.Join(", ", got)}");
}
