using System.Text;
using RabbitMQ.Client;

// 15: three ways a message gets dead-lettered: expired, rejected, maxlen
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// start clean: delete objects from a previous run
foreach (var queue in new[] { "orders.ttl", "orders.rejected", "orders.limited", "dead-letters" })
    await channel.QueueDeleteAsync(queue);

await channel.ExchangeDeleteAsync("dlx");

// dead-letter exchange + the queue that collects dead letters
// fanout: every dead letter reaches every bound queue
await channel.ExchangeDeclareAsync("dlx", ExchangeType.Fanout);

// the queue that collects all dead letters
await channel.QueueDeclareAsync("dead-letters", durable: false, exclusive: false, autoDelete: false);

// bind "dead-letters" to "dlx" (fanout ignores the key)
await channel.QueueBindAsync("dead-letters", "dlx", "");

// messages older than 1000 ms -> "dlx"
await Declare("orders.ttl", new() { ["x-message-ttl"] = 1000, ["x-dead-letter-exchange"] = "dlx" });

// nacked/rejected messages (requeue false) -> "dlx"
await Declare("orders.rejected", new() { ["x-dead-letter-exchange"] = "dlx" });

// max 1 message; the oldest overflow -> "dlx"
await Declare("orders.limited", new() { ["x-max-length"] = 1, ["x-dead-letter-exchange"] = "dlx" });

// expired: nobody reads it within 1000 ms
await channel.BasicPublishAsync("", "orders.ttl", Encoding.UTF8.GetBytes("A (waits too long)"));

// rejected: consumer nacks without requeue
await channel.BasicPublishAsync("", "orders.rejected", Encoding.UTF8.GetBytes("B (bad payload)"));

// pull B without acking it
var bad = await channel.BasicGetAsync("orders.rejected", autoAck: false);

// nack with requeue: false = reject -> dead-lettered (reason "rejected")
await channel.BasicNackAsync(bad!.DeliveryTag, multiple: false, requeue: false);

// maxlen: the oldest message is pushed out of a full queue
await channel.BasicPublishAsync("", "orders.limited", Encoding.UTF8.GetBytes("C1 (oldest)"));

await channel.BasicPublishAsync("", "orders.limited", Encoding.UTF8.GetBytes("C2 (newest)"));

Console.WriteLine("published A -> orders.ttl, B -> orders.rejected, C1 + C2 -> orders.limited");

// wait past the 1000 ms TTL so A expires
await Task.Delay(1500);

// read every dead letter
while (await channel.BasicGetAsync("dead-letters", autoAck: true) is { } message)
{
    var headers = message.BasicProperties.Headers!;

    // the broker adds x-first-death-reason: "expired", "rejected" or "maxlen"
    var reason = Encoding.UTF8.GetString((byte[])headers["x-first-death-reason"]!);

    // x-first-death-queue = the queue the message died in
    var from = Encoding.UTF8.GetString((byte[])headers["x-first-death-queue"]!);

    Console.WriteLine($"  dead-letters <- {Encoding.UTF8.GetString(message.Body.Span),-20} reason={reason,-9} from={from}");
}

// C2 (newest) is still there
var limited = await channel.BasicGetAsync("orders.limited", autoAck: true);

Console.WriteLine($"  orders.limited still holds {Encoding.UTF8.GetString(limited!.Body.Span)}");

// helper: declare a queue with extra x-* arguments
async Task Declare(string queue, Dictionary<string, object?> arguments) =>
    await channel.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: false, arguments: arguments);
