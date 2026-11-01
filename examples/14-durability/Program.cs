using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// 14: what survives a broker restart?
//   dotnet run -- publish   ->   docker compose restart rabbitmq   ->   dotnet run -- check
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// step 1 (dotnet run -- publish): create queues and messages
if (args.FirstOrDefault() == "publish")
{
    // open a channel for the commands below
    await using var channel = await connection.CreateChannelAsync();

    // start clean
    await channel.QueueDeleteAsync("durable-queue");

    await channel.QueueDeleteAsync("transient-queue");

    // durable: true = the queue definition survives a restart
    await channel.QueueDeclareAsync("durable-queue", durable: true, exclusive: false, autoDelete: false);

    // durable: false = the queue is gone after a restart
    await channel.QueueDeclareAsync("transient-queue", durable: false, exclusive: false, autoDelete: false);

    // Persistent = true: message written to disk
    var persistent = new BasicProperties { Persistent = true };

    // Persistent = false: message kept in memory only
    var transient = new BasicProperties { Persistent = false };

    // args: exchange "", key, mandatory false, properties, body -> survives
    await channel.BasicPublishAsync("", "durable-queue", false, persistent, Encoding.UTF8.GetBytes("persistent message"));

    // durable queue but transient message -> message lost on restart
    await channel.BasicPublishAsync("", "durable-queue", false, transient, Encoding.UTF8.GetBytes("transient message"));

    // persistent message in a transient queue -> lost with the queue
    await channel.BasicPublishAsync("", "transient-queue", false, persistent, Encoding.UTF8.GetBytes("persistent message"));

    Console.WriteLine("durable-queue   <- persistent message, transient message");

    Console.WriteLine("transient-queue <- persistent message");

    return;
}

// step 2 (dotnet run -- check, after the restart): what is left?
foreach (var queue in new[] { "durable-queue", "transient-queue" })
{
    // new channel per queue: a 404 closes the channel
    await using var channel = await connection.CreateChannelAsync();

    try
    {
        // passive declare = check only, never creates; throws 404 if the queue did not survive the restart
        await channel.QueueDeclarePassiveAsync(queue);

        var bodies = new List<string>();

        // read every message left in the queue
        while (await channel.BasicGetAsync(queue, autoAck: true) is { } message)
            bodies.Add(Encoding.UTF8.GetString(message.Body.Span));

        Console.WriteLine($"{queue,-15} exists, messages: [{string.Join(", ", bodies)}]");
    }
    catch (OperationInterruptedException ex)
    {
        Console.WriteLine($"{queue,-15} gone ({ex.ShutdownReason?.ReplyCode} {ex.ShutdownReason?.ReplyText})");
    }
}
