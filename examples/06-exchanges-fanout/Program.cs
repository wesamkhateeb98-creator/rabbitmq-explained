using System.Text;
using RabbitMQ.Client;

// 06: a fanout exchange copies every message to every bound queue
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

string[] queues = ["signup.email", "signup.audit"];

// start clean: delete the exchange from a previous run
await channel.ExchangeDeleteAsync("signups");

// fanout exchange: copies every message to ALL bound queues
await channel.ExchangeDeclareAsync("signups", ExchangeType.Fanout);

foreach (var queue in queues)
{
    // start clean
    await channel.QueueDeleteAsync(queue);

    // one queue per subscriber (email, audit)
    await channel.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: false);

    // fanout ignores the routing key, so bind with ""
    await channel.QueueBindAsync(queue, "signups", routingKey: "");
}

// the routing key is ignored by fanout: "anything" still reaches both queues
await channel.BasicPublishAsync("signups", "", Encoding.UTF8.GetBytes("user 7 signed up"));

// second message, different key: same result, both queues get it
await channel.BasicPublishAsync("signups", "anything", Encoding.UTF8.GetBytes("user 9 signed up"));

Console.WriteLine("published 2 messages to fanout exchange 'signups'");

foreach (var queue in queues)
{
    // pull messages one by one until the queue is empty (null)
    while (await channel.BasicGetAsync(queue, autoAck: true) is { } message)
        Console.WriteLine($"  {queue,-13} <- {Encoding.UTF8.GetString(message.Body.Span)}");
}
