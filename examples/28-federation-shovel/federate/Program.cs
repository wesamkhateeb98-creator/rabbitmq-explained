using System.Text;
using RabbitMQ.Client;

// 28 federate: publish to rabbit1, read the same data from rabbit2
// no setup here: the brokers import exchanges, queue, binding, upstream and policy from definitions.json at boot
var rabbit1 = new ConnectionFactory { HostName = "localhost", Port = 5711, UserName = "admin", Password = "admin" };

var rabbit2 = new ConnectionFactory { HostName = "localhost", Port = 5712, UserName = "admin", Password = "admin" };

// one connection to each broker
await using var connection1 = await rabbit1.CreateConnectionAsync();

await using var connection2 = await rabbit2.CreateConnectionAsync();

// rabbit1 channel in confirm mode: each publish returns once rabbit1 has the message
await using var channel1 = await connection1.CreateChannelAsync(new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true));

await using var channel2 = await connection2.CreateChannelAsync();

// the link created this queue on rabbit1: it buffers what rabbit2 asked for (binding order.*)
// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
var linkQueue = await channel1.QueueDeclarePassiveAsync("federation: orders -> rabbit@rabbit2");

Console.WriteLine($"rabbit1: link queue '{linkQueue.QueueName}', consumers = {linkQueue.ConsumerCount} (rabbit2)");

// publish to exchange "orders" on rabbit1; the routing key decides what crosses
(string Key, string Body)[] messages = [("order.created", "order-1"), ("order.paid", "order-1"), ("payment.failed", "card-7"), ("order.created", "order-2")];

foreach (var (key, body) in messages)
{
    await channel1.BasicPublishAsync("orders", key, Encoding.UTF8.GetBytes(body));

    Console.WriteLine($"rabbit1: publish orders  key {key,-15} body {body}");
}

// poll rabbit2's queue every 250 ms (max 10 s) until the 3 "order.*" messages arrive
// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
for (var i = 0; i < 40 && (await channel2.QueueDeclarePassiveAsync("orders.copy")).MessageCount < 3; i++)
    await Task.Delay(250);

// pull every message from rabbit2 until the queue is empty
while (await channel2.BasicGetAsync("orders.copy", autoAck: true) is { } message)
    Console.WriteLine($"rabbit2: orders.copy <- key {message.RoutingKey,-15} body {Encoding.UTF8.GetString(message.Body.Span)}");

// payment.failed matched no binding on rabbit1 (the link queue only binds order.*), so rabbit1 dropped it
Console.WriteLine("payment.failed never reached rabbit2: no binding for it, rabbit1 dropped it");
