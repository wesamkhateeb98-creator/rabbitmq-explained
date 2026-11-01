using System.Text;
using RabbitMQ.Client;

// 17: at-least-once delivery means duplicates; an idempotent consumer ignores them
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// start clean
await channel.ExchangeDeleteAsync("payments");

// fanout: both consumers' queues get every payment
await channel.ExchangeDeclareAsync("payments", ExchangeType.Fanout);

foreach (var queue in new[] { "payments.naive", "payments.idempotent" })
{
    // start clean
    await channel.QueueDeleteAsync(queue);

    // create the queue
    await channel.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: false);

    // bind it to "payments" (fanout ignores the key)
    await channel.QueueBindAsync(queue, "payments", "");
}

// the producer resends pay-1 because its first confirm was lost (timeout)
(string Id, int Amount)[] payments = [("pay-1", 100), ("pay-2", 50), ("pay-1", 100)];

foreach (var (id, amount) in payments)
{
    // MessageId = stable business id: the resend of pay-1 has the same id
    var properties = new BasicProperties { MessageId = id };

    // publish to the fanout exchange
    await channel.BasicPublishAsync("payments", "", mandatory: false, basicProperties: properties, body: Encoding.UTF8.GetBytes(amount.ToString()));
}

Console.WriteLine("published pay-1 (100), pay-2 (50), pay-1 (100) again");

// naive consumer: applies every message
var naiveBalance = 0;

// pull messages one by one until empty
while (await channel.BasicGetAsync("payments.naive", autoAck: false) is { } message)
{
    naiveBalance += int.Parse(Encoding.UTF8.GetString(message.Body.Span));

    // ack = done, delete it
    await channel.BasicAckAsync(message.DeliveryTag, multiple: false);
}

Console.WriteLine($"naive consumer      balance = {naiveBalance}");

// idempotent consumer: remembers processed ids (in production: a unique key in the DB)
var processed = new HashSet<string>();

var balance = 0;

// pull messages one by one until empty
while (await channel.BasicGetAsync("payments.idempotent", autoAck: false) is { } message)
{
    var id = message.BasicProperties.MessageId!;

    // Add returns false if the id was seen -> duplicate, skip it
    if (processed.Add(id))
        balance += int.Parse(Encoding.UTF8.GetString(message.Body.Span));
    else
        Console.WriteLine($"  {id} already processed -> skip, ack");

    // ack duplicates too, so they leave the queue
    await channel.BasicAckAsync(message.DeliveryTag, multiple: false);
}

Console.WriteLine($"idempotent consumer balance = {balance}");
