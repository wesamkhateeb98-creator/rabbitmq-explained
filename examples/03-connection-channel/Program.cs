using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// 03: one TCP connection, many lightweight channels
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// one TCP connection; the name "demo-03" shows in the management UI
await using var connection = await factory.CreateConnectionAsync("demo-03");

Console.WriteLine($"connection '{connection.ClientProvidedName}' -> {connection.Endpoint}");

var channels = new List<IChannel>();

for (var i = 0; i < 3; i++)
{
    // open a new channel on the same connection (numbered 1, 2, 3)
    var channel = await connection.CreateChannelAsync();

    channels.Add(channel);

    Console.WriteLine($"  opened channel #{channel.ChannelNumber}");
}

// A channel-level error closes only that channel
try
{
    // passive declare = "does this queue exist?"; it doesn't -> broker closes this channel with 404 NOT_FOUND
    await channels[1].QueueDeclarePassiveAsync("does-not-exist");
}
catch (OperationInterruptedException ex)
{
    Console.WriteLine($"channel #{channels[1].ChannelNumber} error: {ex.ShutdownReason?.ReplyCode} {ex.ShutdownReason?.ReplyText}");
}

// IsOpen: channel 2 is now False, channels 1 and 3 stay True
foreach (var channel in channels)
    Console.WriteLine($"  channel #{channel.ChannelNumber} open: {channel.IsOpen}");

Console.WriteLine($"connection open: {connection.IsOpen}");
