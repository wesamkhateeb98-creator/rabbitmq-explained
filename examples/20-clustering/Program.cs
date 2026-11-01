using System.Text;
using RabbitMQ.Client;

// 20: a client that knows every node, and a quorum queue replicated on 3 nodes
//   dotnet run -- publish   ->   docker stop <leader node>   ->   dotnet run -- consume
// no HostName: the endpoints below say where to connect
var factory = new ConnectionFactory();

// the 3 cluster nodes (host ports 5681..5683)
AmqpTcpEndpoint[] nodes = [new("localhost", 5681), new("localhost", 5682), new("localhost", 5683)];

// the client shuffles the endpoints and connects to the first one that answers
await using var connection = await factory.CreateConnectionAsync(nodes);

Console.WriteLine($"connected to {connection.Endpoint}");

// confirms ON: each publish waits for the broker ack
var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

// channel in confirm mode
await using var channel = await connection.CreateChannelAsync(options);

// step 1 (dotnet run -- publish)
if (args.FirstOrDefault() == "publish")
{
    // start clean
    await channel.QueueDeleteAsync("cluster-orders");

    // quorum queue: replicated on all 3 nodes, one leader
    var arguments = new Dictionary<string, object?> { ["x-queue-type"] = "quorum" };

    // quorum queues must be durable
    await channel.QueueDeclareAsync("cluster-orders", durable: true, exclusive: false, autoDelete: false, arguments: arguments);

    // Persistent = true: stored on disk on each replica
    var persistent = new BasicProperties { Persistent = true };

    // confirmed only when a majority (2 of 3) of replicas stored it
    for (var i = 1; i <= 3; i++)
        await channel.BasicPublishAsync("", "cluster-orders", mandatory: true, basicProperties: persistent, body: Encoding.UTF8.GetBytes($"order-{i}"));

    Console.WriteLine("published order-1..order-3 (confirmed by a quorum of replicas)");

    return;
}

var got = new List<string>();

// step 2 (dotnet run -- consume, after stopping the leader): read every message
while (await channel.BasicGetAsync("cluster-orders", autoAck: true) is { } message)
    got.Add(Encoding.UTF8.GetString(message.Body.Span));

Console.WriteLine($"consumed: {string.Join(", ", got)}");
