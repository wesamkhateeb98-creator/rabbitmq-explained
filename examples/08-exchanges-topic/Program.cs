using System.Text;
using RabbitMQ.Client;

// 08: a topic exchange matches dotted keys: * = exactly one word, # = zero or more words
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// queue -> binding pattern (* = one word, # = zero or more words)
var bindings = new Dictionary<string, string>
{
    ["eu-orders"] = "order.eu.*",
    ["created"] = "order.*.created",
    ["us-everything"] = "order.us.#",
    ["audit"] = "#",
};

// start clean: delete the exchange from a previous run
await channel.ExchangeDeleteAsync("events");

// topic exchange: matches dot-separated keys against wildcard patterns
await channel.ExchangeDeclareAsync("events", ExchangeType.Topic);

foreach (var (queue, pattern) in bindings)
{
    // start clean
    await channel.QueueDeleteAsync(queue);

    // create the queue
    await channel.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: false);

    // bind the queue with its pattern
    await channel.QueueBindAsync(queue, "events", pattern);

    Console.WriteLine($"bind {queue,-13} <- {pattern}");
}

string[] keys = ["order.eu.created", "order.eu.cancelled", "order.us.created", "order.us.created.priority", "payment.eu.failed"];

// routing key = the event name; each queue whose pattern matches gets a copy
foreach (var key in keys)
    await channel.BasicPublishAsync("events", key, Encoding.UTF8.GetBytes(key));

Console.WriteLine($"published {keys.Length} messages");

foreach (var queue in bindings.Keys)
{
    var got = new List<string>();

    // pull messages one by one until the queue is empty (null)
    while (await channel.BasicGetAsync(queue, autoAck: true) is { } message)
        got.Add(Encoding.UTF8.GetString(message.Body.Span));

    Console.WriteLine($"  {queue,-13} <- {string.Join(", ", got)}");
}
