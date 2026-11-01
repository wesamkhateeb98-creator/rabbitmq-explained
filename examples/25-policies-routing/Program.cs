using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

// 25: policies, operator policies, alternate exchange, exchange-to-exchange bindings
// client for the management HTTP API on port 15672
using var http = new HttpClient { BaseAddress = new Uri("http://localhost:15672/api/") };

// HTTP Basic auth as admin guest:guest
http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes("guest:guest")));

// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await connection.CreateChannelAsync();

// ---- Part A: a policy changes queues without redeclaring them
Console.WriteLine("Part A: policy + operator policy on '^policy\\.' queues");

// start clean
await channel.QueueDeleteAsync("policy.orders");

// plain queue, no x-* arguments: the policy adds the limits
await channel.QueueDeclareAsync("policy.orders", durable: true, exclusive: false, autoDelete: false);

var policy = new Dictionary<string, object>
{
    // regex on queue names: matches "policy.orders"
    ["pattern"] = "^policy\\.",
    ["apply-to"] = "queues",
    // higher priority wins when several policies match
    ["priority"] = 1,
    // keep max 3 messages, each lives 2000 ms
    ["definition"] = new Dictionary<string, object> { ["max-length"] = 3, ["message-ttl"] = 2000 },
};

// PUT /api/policies/%2F/orders-limits: create the policy in vhost "/" (%2F)
(await http.PutAsJsonAsync("policies/%2F/orders-limits", policy)).EnsureSuccessStatusCode();

Console.WriteLine("  policy orders-limits      : max-length 3, message-ttl 2000 ms");

// wait until the queue shows the policy
await PrintEffective();

// 5 published, max-length 3 -> ready = 3
await PublishAndCount(5);

// wait past the 2 s TTL
await Task.Delay(2500);

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
Console.WriteLine($"  after 2.5 s (TTL 2 s)     : ready = {(await channel.QueueDeclarePassiveAsync("policy.orders")).MessageCount}");

// operator policy (set by the admin): max-length 2; merged with the user policy, the lower value wins
(await http.PutAsJsonAsync("operator-policies/%2F/ops-cap", new Dictionary<string, object> { ["pattern"] = "^policy\\.", ["definition"] = new Dictionary<string, object> { ["max-length"] = 2 }, ["apply-to"] = "queues" })).EnsureSuccessStatusCode();

Console.WriteLine("  operator policy ops-cap   : max-length 2 (the stricter value wins)");

// wait until the queue shows both policies
await PrintEffective("ops-cap");

// 5 published, max-length 2 -> ready = 2
await PublishAndCount(5);

// clean up both policies and the queue
await http.DeleteAsync("policies/%2F/orders-limits");

await http.DeleteAsync("operator-policies/%2F/ops-cap");

await channel.QueueDeleteAsync("policy.orders");

// ---- Part B: alternate exchange catches unroutable messages
Console.WriteLine("\nPart B: alternate exchange");

// start clean
await channel.ExchangeDeleteAsync("ae.orders");

await channel.ExchangeDeleteAsync("ae.unrouted");

// fanout exchange that collects unroutable messages
await channel.ExchangeDeclareAsync("ae.unrouted", ExchangeType.Fanout);

// alternate-exchange: messages "ae.orders" can't route go to "ae.unrouted" instead of being dropped
await channel.ExchangeDeclareAsync("ae.orders", ExchangeType.Direct, arguments: new Dictionary<string, object?> { ["alternate-exchange"] = "ae.unrouted" });

// only key "order.created" is bound
await Queue("ae.billing", "ae.orders", "order.created");

// catch-all queue behind the alternate exchange
await Queue("ae.unrouted.q", "ae.unrouted", "");

// routed normally -> ae.billing
await channel.BasicPublishAsync("ae.orders", "order.created", Encoding.UTF8.GetBytes("order.created"));

// no binding -> alternate exchange -> ae.unrouted.q
await channel.BasicPublishAsync("ae.orders", "order.refunded", Encoding.UTF8.GetBytes("order.refunded (no binding)"));

// print what each queue got
await Drain("ae.billing");

await Drain("ae.unrouted.q");

// ---- Part C: exchange-to-exchange binding
Console.WriteLine("\nPart C: exchange-to-exchange binding  e2e.all --[eu.#]--> e2e.eu --[#]--> e2e.eu.q");

// start clean
await channel.ExchangeDeleteAsync("e2e.all");

await channel.ExchangeDeleteAsync("e2e.eu");

// two topic exchanges: e2e.all (everything) and e2e.eu (EU only)
await channel.ExchangeDeclareAsync("e2e.all", ExchangeType.Topic);

await channel.ExchangeDeclareAsync("e2e.eu", ExchangeType.Topic);

// exchange-to-exchange binding: e2e.all forwards keys "eu.#" to e2e.eu
await channel.ExchangeBindAsync(destination: "e2e.eu", source: "e2e.all", routingKey: "eu.#");

// queue on e2e.eu gets all it receives ("#")
await Queue("e2e.eu.q", "e2e.eu", "#");

// "us.order.created" doesn't match eu.# -> never reaches e2e.eu
foreach (var key in new[] { "eu.order.created", "us.order.created", "eu.payment.failed" })
    await channel.BasicPublishAsync("e2e.all", key, Encoding.UTF8.GetBytes(key));

// e2e.eu.q <- the 2 EU keys
await Drain("e2e.eu.q");

// management stats refresh every 5 s, so poll until the expected policies show up
async Task PrintEffective(string? operatorPolicy = null)
{
    while (true)
    {
        // GET /api/queues/%2F/policy.orders: queue details incl. the applied policies
        var queue = await http.GetFromJsonAsync<JsonElement>("queues/%2F/policy.orders");

        // ready when the merged definition exists and the expected operator policy is applied
        var ready = queue.TryGetProperty("effective_policy_definition", out var effective)
            && queue.GetProperty("operator_policy").ToString() == (operatorPolicy ?? "");

        if (ready)
        {
            Console.WriteLine($"  effective definition      : {effective}");

            return;
        }

        await Task.Delay(500);
    }
}

async Task PublishAndCount(int count)
{
    // purge = delete all ready messages
    await channel.QueuePurgeAsync("policy.orders");

    // publish m1..m{count}; max-length drops the oldest beyond the limit
    for (var i = 1; i <= count; i++)
        await channel.BasicPublishAsync("", "policy.orders", Encoding.UTF8.GetBytes($"m{i}"));

    // let the broker apply the limit
    await Task.Delay(200);

    // passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
    Console.WriteLine($"  published {count}               : ready = {(await channel.QueueDeclarePassiveAsync("policy.orders")).MessageCount}");
}

async Task Queue(string queue, string exchange, string key)
{
    // helper: recreate a queue and bind it; first delete the one from a previous run
    await channel.QueueDeleteAsync(queue);

    // durable false = gone after a restart; exclusive false = any connection can use it; autoDelete false = kept with no consumers
    await channel.QueueDeclareAsync(queue, durable: false, exclusive: false, autoDelete: false);

    // bind queue to exchange with the key
    await channel.QueueBindAsync(queue, exchange, key);
}

async Task Drain(string queue)
{
    var got = new List<string>();

    // helper: pull every message until the queue is empty
    while (await channel.BasicGetAsync(queue, autoAck: true) is { } message)
        got.Add(Encoding.UTF8.GetString(message.Body.Span));

    Console.WriteLine($"  {queue,-14} <- {string.Join(", ", got)}");
}
