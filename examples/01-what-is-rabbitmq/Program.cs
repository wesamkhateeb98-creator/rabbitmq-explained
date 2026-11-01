using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 01: the broker keeps messages while the consumer is offline.
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open one TCP connection to the broker
await using var connection = await factory.CreateConnectionAsync();

// confirms: each publish returns only after the broker has stored the message
var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

// a channel = lightweight session inside the connection; all commands go through it
await using var channel = await connection.CreateChannelAsync(options);

// start clean: delete the queue left by a previous run
await channel.QueueDeleteAsync("orders");

// create queue "orders" if missing (durable: false = lost on broker restart)
await channel.QueueDeclareAsync("orders", durable: false, exclusive: false, autoDelete: false);

// Producer: the consumer is not running yet
string[] orders = ["order-1: 2 x keyboard", "order-2: 1 x monitor", "order-3: 3 x mouse"];

foreach (var order in orders)
{
    // default exchange "" + routingKey "orders" = deliver straight to queue "orders"
    await channel.BasicPublishAsync(exchange: "", routingKey: "orders", body: Encoding.UTF8.GetBytes(order));

    Console.WriteLine($"[producer] sent     {order}");
}

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
var queue = await channel.QueueDeclarePassiveAsync("orders");

Console.WriteLine($"[broker]   queue 'orders' holds {queue.MessageCount} messages, consumers: {queue.ConsumerCount}");

// Consumer: starts later and still gets every order
var received = 0;

var done = new TaskCompletionSource();

// consumer object: the broker pushes messages to it
var consumer = new AsyncEventingBasicConsumer(channel);

// runs once per delivered message; ea.Body = the message bytes
consumer.ReceivedAsync += (_, ea) =>
{
    Console.WriteLine($"[consumer] received {Encoding.UTF8.GetString(ea.Body.Span)}");

    // all 3 orders received -> let the program finish
    if (++received == orders.Length)
        done.SetResult();

    return Task.CompletedTask;
};

// start receiving; autoAck: true = broker deletes each message as soon as it sends it
await channel.BasicConsumeAsync("orders", autoAck: true, consumer: consumer);

// wait until the handler has seen every order
await done.Task;
