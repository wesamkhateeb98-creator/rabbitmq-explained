using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 04: the smallest producer and consumer
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel: every command below goes through it
await using var channel = await connection.CreateChannelAsync();

// create queue "hello" if missing (safe to call many times)
await channel.QueueDeclareAsync("hello", durable: false, exclusive: false, autoDelete: false);

// Send
var body = Encoding.UTF8.GetBytes("Hello World!");

// default exchange "" delivers to the queue named by the routing key
await channel.BasicPublishAsync(exchange: "", routingKey: "hello", body: body);

Console.WriteLine($" [x] Sent 'Hello World!' ({body.Length} bytes)");

// Receive
// completed by the handler when the message arrives
var done = new TaskCompletionSource();

// consumer object: the broker pushes messages to it
var consumer = new AsyncEventingBasicConsumer(channel);

// runs once per message; DeliveryTag = message number on this channel
consumer.ReceivedAsync += (_, ea) =>
{
    Console.WriteLine($" [x] Received '{Encoding.UTF8.GetString(ea.Body.Span)}' (delivery tag {ea.DeliveryTag})");

    done.SetResult();

    return Task.CompletedTask;
};

// start receiving; autoAck: true = broker deletes the message as soon as it sends it
await channel.BasicConsumeAsync("hello", autoAck: true, consumer: consumer);

// wait until the message is received
await done.Task;
