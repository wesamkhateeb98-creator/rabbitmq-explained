using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

// 21: management HTTP API, Prometheus metrics, vhost + least-privilege user
// client for the management HTTP API on port 15672
using var http = new HttpClient { BaseAddress = new Uri("http://localhost:15672/api/") };

// HTTP Basic auth as admin guest:guest
http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes("guest:guest")));

// GET /api/overview: broker and Erlang versions, totals
var overview = await http.GetFromJsonAsync<JsonElement>("overview");

Console.WriteLine($"broker: RabbitMQ {overview.GetProperty("rabbitmq_version")}, Erlang {overview.GetProperty("erlang_version")}");

// 1) isolate the app: vhost "shop", user "shop-app" limited to names starting with "app."
// PUT /api/vhosts/shop: create vhost "shop" (an isolated namespace)
(await http.PutAsJsonAsync("vhosts/shop", new { })).EnsureSuccessStatusCode();

// create user "shop-app"; tags "" = no management UI access
(await http.PutAsJsonAsync("users/shop-app", new { password = "demo-only-password", tags = "" })).EnsureSuccessStatusCode();

// 3 regexes on resource names: configure (declare/delete), write (publish), read (consume)
(await http.PutAsJsonAsync("permissions/shop/shop-app", new { configure = "^app\\.", write = "^(app\\..*|amq\\.default)$", read = "^app\\." })).EnsureSuccessStatusCode();

// publishing via the default exchange "" needs write access to "amq.default"
Console.WriteLine("created vhost 'shop', user 'shop-app': configure/read ^app\\., write ^(app\\..*|amq\\.default)$");

// 2) connect as the app user
// connect as that user to that vhost
var factory = new ConnectionFactory { HostName = "localhost", VirtualHost = "shop", UserName = "shop-app", Password = "demo-only-password" };

// open the TCP connection as "shop-app"
await using (var connection = await factory.CreateConnectionAsync())
{
    // open a channel for the commands below
    await using var channel = await connection.CreateChannelAsync();

    // allowed: name matches ^app\.
    await channel.QueueDeclareAsync("app.orders", durable: true, exclusive: false, autoDelete: false);

    // publish via "" (needs write on amq.default)
    for (var i = 0; i < 3; i++)
        await channel.BasicPublishAsync("", "app.orders", Encoding.UTF8.GetBytes($"order-{i}"));

    Console.WriteLine("  declare 'app.orders' -> ok, published 3 messages");

    // 3) Prometheus endpoint (aggregated metrics), read while this connection is open
    // plain HTTP client, no auth needed for metrics
    using var metrics = new HttpClient();

    // Prometheus text format: one "name value" line per metric
    var text = await metrics.GetStringAsync("http://localhost:15692/metrics");

    // the 3 metrics to print
    string[] wanted = ["rabbitmq_queues ", "rabbitmq_connections ", "rabbitmq_queue_messages_ready "];

    Console.WriteLine("  prometheus :15692/metrics");

    foreach (var line in text.Split('\n').Where(l => wanted.Any(l.StartsWith)))
        Console.WriteLine($"    {line}");

    try
    {
        // not allowed: "orders" doesn't match ^app\. -> 403 ACCESS_REFUSED
        await channel.QueueDeclareAsync("orders", durable: true, exclusive: false, autoDelete: false);
    }
    catch (OperationInterruptedException ex)
    {
        Console.WriteLine($"  declare 'orders'     -> {ex.ShutdownReason?.ReplyCode} {ex.ShutdownReason?.ReplyText}");
    }
}

// 4) management API queue stats (refreshed every 5 s)
// wait for the stats refresh
await Task.Delay(6000);

// GET /api/queues/shop: every queue in the vhost with its stats
var queues = await http.GetFromJsonAsync<JsonElement>("queues/shop");

foreach (var queue in queues.EnumerateArray())
    Console.WriteLine($"management API: vhost shop, queue {queue.GetProperty("name")}, messages {queue.GetProperty("messages")}, type {queue.GetProperty("type")}");

// cleanup
// remove the demo user and vhost (deleting the vhost deletes its queues)
await http.DeleteAsync("users/shop-app");

await http.DeleteAsync("vhosts/shop");

Console.WriteLine("cleanup: deleted user 'shop-app' and vhost 'shop'");
