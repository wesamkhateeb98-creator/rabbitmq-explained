using System.Diagnostics;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 16: retry with exponential backoff (500, 1000, 2000 ms), then a parking lot
//   work --fail--> work.retry (per-message TTL) --expired, dead-lettered--> work
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// after 3 retries the message goes to the parking lot
const int MaxRetries = 3;

// start clean
foreach (var queue in new[] { "work", "work.retry", "work.parking-lot" })
    await channel.QueueDeleteAsync(queue);

// main queue the consumer reads
await channel.QueueDeclareAsync("work", durable: true, exclusive: false, autoDelete: false);

// when a message expires in "work.retry", send it via "" back to "work"
var retryArguments = new Dictionary<string, object?>
{
    ["x-dead-letter-exchange"] = "",
    ["x-dead-letter-routing-key"] = "work",
};

// waiting room with no consumers: messages sit there until their TTL
await channel.QueueDeclareAsync("work.retry", durable: true, exclusive: false, autoDelete: false, arguments: retryArguments);

// messages that failed every retry, for a human to check
await channel.QueueDeclareAsync("work.parking-lot", durable: true, exclusive: false, autoDelete: false);

// clock for the "ms" column
var clock = Stopwatch.StartNew();

var finished = 0;

// completed when both orders are finished (OK or parked)
var done = new TaskCompletionSource();

// consumer object: the broker pushes messages to it
var consumer = new AsyncEventingBasicConsumer(channel);

// runs once per delivery from "work"
consumer.ReceivedAsync += async (_, ea) =>
{
    var order = Encoding.UTF8.GetString(ea.Body.Span);

    // x-retry header = how many retries so far (missing = 0)
    var attempt = ea.BasicProperties.Headers?["x-retry"] is int retry ? retry : 0;

    // order-42 fails twice then works; order-99 always fails
    var success = order == "order-42" && attempt >= 2;

    var time = $"{clock.ElapsedMilliseconds,5} ms";

    if (success)
    {
        Console.WriteLine($"{time}  {order} attempt {attempt}: OK");

        finished++;
    }
    else if (attempt >= MaxRetries)
    {
        // out of retries: park the message
        await channel.BasicPublishAsync("", "work.parking-lot", ea.Body);

        Console.WriteLine($"{time}  {order} attempt {attempt}: FAILED -> parking lot");

        finished++;
    }
    else
    {
        // exponential backoff: 500, 1000, 2000 ms
        var delay = 500 * (int)Math.Pow(2, attempt);

        var properties = new BasicProperties
        {
            // Expiration = per-message TTL in ms (string); x-retry = next attempt number
            Persistent = true,
            Expiration = delay.ToString(),
            Headers = new Dictionary<string, object?> { ["x-retry"] = attempt + 1 },
        };

        // wait "delay" ms in work.retry, then dead-lettered back to "work"
        await channel.BasicPublishAsync("", "work.retry", mandatory: false, basicProperties: properties, body: ea.Body);

        Console.WriteLine($"{time}  {order} attempt {attempt}: FAILED -> retry in {delay} ms");
    }

    // always ack the original: a copy already went to retry or parking lot
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

    if (finished == 2)
        done.TrySetResult();
};

// manual acks: autoAck false
await channel.BasicConsumeAsync("work", autoAck: false, consumer: consumer);

// order-42 works on attempt 2; order-99 always fails
await channel.BasicPublishAsync("", "work", Encoding.UTF8.GetBytes("order-42"));

await channel.BasicPublishAsync("", "work", Encoding.UTF8.GetBytes("order-99"));

// wait max 15 s
await done.Task.WaitAsync(TimeSpan.FromSeconds(15));

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
var parked = await channel.QueueDeclarePassiveAsync("work.parking-lot");

Console.WriteLine($"parking lot holds {parked.MessageCount} message(s)");
