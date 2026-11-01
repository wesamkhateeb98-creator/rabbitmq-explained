using System.Text;
using RabbitMQ.Client;

// 07: a direct exchange routes by exact routing-key match
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// queue -> the routing keys it binds with
var bindings = new Dictionary<string, string[]>
{
    ["logs.errors"] = ["error"],
    ["logs.all"] = ["info", "warning", "error"],
};

// start clean: delete the exchange from a previous run
await channel.ExchangeDeleteAsync("logs");

// direct exchange: routing key must equal a binding key exactly
await channel.ExchangeDeclareAsync("logs", ExchangeType.Direct);

foreach (var (queue, keys) in bindings)
{
    // start clean
    await channel.QueueDeleteAsync(queue);

    // create the queue
    await channel.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: false);

    // one binding per key: "logs.all" gets 3 bindings
    foreach (var key in keys)
        await channel.QueueBindAsync(queue, "logs", key);

    Console.WriteLine($"bind {queue,-11} <- {string.Join(", ", keys)}");
}

(string Key, string Text)[] messages =
[
    ("info", "disk 40%"),
    ("warning", "disk 85%"),
    ("error", "disk full"),
    ("debug", "cache hit"),
];

// "debug" has no binding -> dropped (mandatory false)
foreach (var (key, text) in messages)
    await channel.BasicPublishAsync("logs", key, Encoding.UTF8.GetBytes($"[{key}] {text}"));

Console.WriteLine($"published {messages.Length} messages");

foreach (var queue in bindings.Keys)
{
    // pull messages one by one until the queue is empty (null)
    while (await channel.BasicGetAsync(queue, autoAck: true) is { } message)
        Console.WriteLine($"  {queue,-11} <- {Encoding.UTF8.GetString(message.Body.Span)}");
}
