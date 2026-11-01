using System.Text;
using RabbitMQ.Client;

// 02: producer -> exchange -> binding -> queue -> consumer
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open one TCP connection to the broker
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// start clean: delete objects from a previous run
await channel.ExchangeDeleteAsync("shop");

await channel.QueueDeleteAsync("billing");

// exchange "shop" (direct): routes by exact routing key
await channel.ExchangeDeclareAsync("shop", ExchangeType.Direct);

// queue "billing" stores messages until a consumer reads them
await channel.QueueDeclareAsync("billing", durable: false, exclusive: false, autoDelete: false);

// binding = rule: "shop" sends key "order.created" to "billing"
await channel.QueueBindAsync(queue: "billing", exchange: "shop", routingKey: "order.created");

Console.WriteLine("binding: exchange 'shop' --[order.created]--> queue 'billing'");

// mandatory: true asks the broker to return messages that match no binding
var returned = new TaskCompletionSource();

// fires when the broker sends back an unroutable mandatory message (312 NO_ROUTE)
channel.BasicReturnAsync += (_, ea) =>
{
    Console.WriteLine($"returned: key '{ea.RoutingKey}' -> {ea.ReplyCode} {ea.ReplyText} ({Encoding.UTF8.GetString(ea.Body.Span)})");

    returned.SetResult();

    return Task.CompletedTask;
};

// matches the binding -> stored in "billing"
await channel.BasicPublishAsync("shop", "order.created", mandatory: true, body: Encoding.UTF8.GetBytes("order #1"));

Console.WriteLine("published: key 'order.created'   body 'order #1'");

// no binding for this key -> returned to us
await channel.BasicPublishAsync("shop", "order.cancelled", mandatory: true, body: Encoding.UTF8.GetBytes("order #2"));

Console.WriteLine("published: key 'order.cancelled' body 'order #2'");

// wait for the return of order #2
await returned.Task;

// pull one message (no consumer); autoAck: true = delete it on read
var message = await channel.BasicGetAsync("billing", autoAck: true);

Console.WriteLine($"queue 'billing' -> consumer got '{Encoding.UTF8.GetString(message!.Body.Span)}'");

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
var billing = await channel.QueueDeclarePassiveAsync("billing");

Console.WriteLine($"queue 'billing' now holds {billing.MessageCount} messages");
