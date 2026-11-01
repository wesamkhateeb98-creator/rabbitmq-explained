using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

// 18: quorum queues - replicated with Raft, always durable, with a poison-message delivery limit
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// start clean
await channel.QueueDeleteAsync("orders.quorum");

await channel.QueueDeleteAsync("orders.poison");

// queue for messages that hit the delivery limit
await channel.QueueDeclareAsync("orders.poison", durable: true, exclusive: false, autoDelete: false);

var arguments = new Dictionary<string, object?>
{
    // replicated queue (Raft) instead of classic
    ["x-queue-type"] = "quorum",
    // after 2 failed deliveries (nack/requeue) the message is dead-lettered
    ["x-delivery-limit"] = 2,
    // dead letters go via "" to queue "orders.poison"
    ["x-dead-letter-exchange"] = "",
    ["x-dead-letter-routing-key"] = "orders.poison",
};

// quorum queues must be durable
await channel.QueueDeclareAsync("orders.quorum", durable: true, exclusive: false, autoDelete: false, arguments: arguments);

Console.WriteLine("declared quorum queue 'orders.quorum' (delivery-limit 2, dead letters -> 'orders.poison')");

// a quorum queue must be durable
// separate channel: the error below closes it
await using (var probe = await connection.CreateChannelAsync())
{
    try
    {
        // durable: false + quorum -> 406 PRECONDITION_FAILED
        await probe.QueueDeclareAsync("orders.not-durable", durable: false, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
    }
    catch (OperationInterruptedException ex)
    {
        Console.WriteLine($"declare non-durable quorum queue -> {ex.ShutdownReason?.ReplyCode} {ex.ShutdownReason?.ReplyText}");
    }
}

// one message the consumer will always fail on
await channel.BasicPublishAsync("", "orders.quorum", Encoding.UTF8.GetBytes("poison order"));

// the consumer crashes on this message every time and requeues it
// worker channel
await using var worker = await connection.CreateChannelAsync();

// prefetch 1: one message at a time
await worker.BasicQosAsync(0, 1, false);

// consumer object: the broker pushes messages to it
var consumer = new AsyncEventingBasicConsumer(worker);

// runs once per delivery
consumer.ReceivedAsync += async (_, ea) =>
{
    // x-delivery-count = how many times the quorum queue delivered it before
    var count = ea.BasicProperties.Headers?.TryGetValue("x-delivery-count", out var value) == true ? value : 0;

    Console.WriteLine($"  delivery: redelivered={ea.Redelivered,-5} x-delivery-count={count} -> nack(requeue: true)");

    // nack + requeue: the delivery count goes up each time
    await worker.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
};

// manual acks: autoAck false
await worker.BasicConsumeAsync("orders.quorum", autoAck: false, consumer: consumer);

BasicGetResult? poison = null;

// poll "orders.poison" every 100 ms (max 5 s) until the message arrives
for (var i = 0; i < 50 && poison is null; i++)
{
    await Task.Delay(100);

    // pull one message (null if empty); autoAck true = the broker removes it on delivery
    poison = await channel.BasicGetAsync("orders.poison", autoAck: true);
}

// reason = "delivery_limit"
var reason = Encoding.UTF8.GetString((byte[])poison!.BasicProperties.Headers!["x-first-death-reason"]!);

Console.WriteLine($"orders.poison <- '{Encoding.UTF8.GetString(poison.Body.Span)}' reason={reason}");
