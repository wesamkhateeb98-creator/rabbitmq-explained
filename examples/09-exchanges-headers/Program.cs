using System.Text;
using RabbitMQ.Client;

// 09: a headers exchange routes on header values, not on the routing key
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// x-match "all" = every header must match, "any" = at least one
var bindings = new Dictionary<string, Dictionary<string, object?>>
{
    ["pdf-reports"] = new() { ["x-match"] = "all", ["format"] = "pdf", ["type"] = "report" },
    ["pdf-or-urgent"] = new() { ["x-match"] = "any", ["format"] = "pdf", ["priority"] = "high" },
};

// start clean: delete the exchange from a previous run
await channel.ExchangeDeleteAsync("documents");

// headers exchange: routes by message headers, ignores the routing key
await channel.ExchangeDeclareAsync("documents", ExchangeType.Headers);

foreach (var (queue, arguments) in bindings)
{
    // start clean
    await channel.QueueDeleteAsync(queue);

    // create the queue
    await channel.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: false);

    // the match rules go in the binding arguments; routing key unused
    await channel.QueueBindAsync(queue, "documents", routingKey: "", arguments: arguments);

    Console.WriteLine($"bind {queue,-13} <- {string.Join(", ", arguments.Select(a => $"{a.Key}={a.Value}"))}");
}

(string File, Dictionary<string, object?> Headers)[] documents =
[
    ("monthly.pdf", new() { ["format"] = "pdf", ["type"] = "report" }),
    ("invoice-1.pdf", new() { ["format"] = "pdf", ["type"] = "invoice" }),
    ("alert.csv", new() { ["format"] = "csv", ["priority"] = "high" }),
    ("data.csv", new() { ["format"] = "csv", ["type"] = "report" }),
];

foreach (var (file, headers) in documents)
{
    // the headers the exchange compares with each binding
    var properties = new BasicProperties { Headers = headers };

    // routing key "" because the headers decide
    await channel.BasicPublishAsync("documents", "", mandatory: false, basicProperties: properties, body: Encoding.UTF8.GetBytes(file));

    Console.WriteLine($"publish {file,-13} headers {string.Join(", ", headers.Select(h => $"{h.Key}={h.Value}"))}");
}

foreach (var queue in bindings.Keys)
{
    var got = new List<string>();

    // pull messages one by one until the queue is empty (null)
    while (await channel.BasicGetAsync(queue, autoAck: true) is { } message)
        got.Add(Encoding.UTF8.GetString(message.Body.Span));

    Console.WriteLine($"  {queue,-13} <- {string.Join(", ", got)}");
}
