using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// 11: the default exchange "" routes by queue name and cannot be bound
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// start clean
await channel.QueueDeleteAsync("inbox");

// declaring a queue auto-binds it to "" with routing key = queue name
await channel.QueueDeclareAsync("inbox", durable: false, exclusive: false, autoDelete: false);

// completed when the unroutable message comes back
var returned = new TaskCompletionSource();

// fires when a mandatory message matches no queue (312 NO_ROUTE)
channel.BasicReturnAsync += (_, ea) =>
{
    Console.WriteLine($"  returned: key '{ea.RoutingKey}' -> {ea.ReplyCode} {ea.ReplyText}");

    returned.SetResult();

    return Task.CompletedTask;
};

// "" + "inbox" -> queue "inbox", no binding needed
await channel.BasicPublishAsync("", "inbox", mandatory: true, body: Encoding.UTF8.GetBytes("hi inbox"));

Console.WriteLine("publish exchange '' key 'inbox'  (a queue exists with that name)");

// no queue named "nobody" -> returned to us
await channel.BasicPublishAsync("", "nobody", mandatory: true, body: Encoding.UTF8.GetBytes("hi nobody"));

Console.WriteLine("publish exchange '' key 'nobody' (no such queue)");

// wait for the return
await returned.Task;

// pull one message from "inbox"; autoAck: true = delete on read
var message = await channel.BasicGetAsync("inbox", autoAck: true);

Console.WriteLine($"  inbox <- '{Encoding.UTF8.GetString(message!.Body.Span)}'");

// explicit bindings to the default exchange are refused
try
{
    // broker answers 403 ACCESS_REFUSED and closes the channel
    await channel.QueueBindAsync("inbox", "", "custom-key");
}
catch (OperationInterruptedException ex)
{
    Console.WriteLine($"bind 'inbox' to '' -> {ex.ShutdownReason?.ReplyCode} {ex.ShutdownReason?.ReplyText}");
}
