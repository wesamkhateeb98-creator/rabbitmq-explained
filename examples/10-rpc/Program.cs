using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 10: request/reply over queues with reply_to + correlation_id
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// one TCP connection shared by server and client channels
await using var connection = await factory.CreateConnectionAsync();

// ---- Server: reads "rpc_queue", replies to the queue named in reply_to
// server channel
await using var server = await connection.CreateChannelAsync();

// start clean
await server.QueueDeleteAsync("rpc_queue");

// the queue the server reads requests from
await server.QueueDeclareAsync("rpc_queue", durable: false, exclusive: false, autoDelete: false);

// server consumer: the broker pushes requests to it
var serverConsumer = new AsyncEventingBasicConsumer(server);

// runs once per request
serverConsumer.ReceivedAsync += async (_, ea) =>
{
    var n = int.Parse(Encoding.UTF8.GetString(ea.Body.Span));

    var result = Fibonacci(n);

    Console.WriteLine($"  [server] {ea.BasicProperties.CorrelationId}: fib({n}) = {result} -> reply to {ea.BasicProperties.ReplyTo![..12]}...");

    // copy the request's CorrelationId so the client can match the reply
    var replyProperties = new BasicProperties { CorrelationId = ea.BasicProperties.CorrelationId };

    // send the answer to the client's ReplyTo queue via the default exchange
    await server.BasicPublishAsync("", ea.BasicProperties.ReplyTo!, mandatory: false, basicProperties: replyProperties, body: Encoding.UTF8.GetBytes(result.ToString()));

    // request handled -> delete it from rpc_queue
    await server.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

// start serving; autoAck false = ack after replying
await server.BasicConsumeAsync("rpc_queue", autoAck: false, consumer: serverConsumer);

// ---- Client: private reply queue, one pending task per correlation id
// client channel
await using var client = await connection.CreateChannelAsync();

// no name = broker creates "amq.gen-..." (exclusive, auto-delete)
var replyQueue = (await client.QueueDeclareAsync()).QueueName;

// correlationId -> task waiting for its reply
var pending = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

// client consumer on the reply queue
var clientConsumer = new AsyncEventingBasicConsumer(client);

// find the waiting request by CorrelationId and complete it
clientConsumer.ReceivedAsync += (_, ea) =>
{
    if (pending.TryRemove(ea.BasicProperties.CorrelationId!, out var reply))
        reply.SetResult(Encoding.UTF8.GetString(ea.Body.Span));

    return Task.CompletedTask;
};

// listen on the reply queue
await client.BasicConsumeAsync(replyQueue, autoAck: true, consumer: clientConsumer);

var requestId = 0;

foreach (var n in new[] { 10, 20, 30 })
{
    var correlationId = $"req-{++requestId}";

    // completed when the reply with this id arrives
    var reply = new TaskCompletionSource<string>();

    pending[correlationId] = reply;

    // CorrelationId = request id; ReplyTo = where the server must answer
    var properties = new BasicProperties { CorrelationId = correlationId, ReplyTo = replyQueue };

    Console.WriteLine($"[client] {correlationId}: request fib({n})");

    // send the request to the server's queue
    await client.BasicPublishAsync("", "rpc_queue", mandatory: false, basicProperties: properties, body: Encoding.UTF8.GetBytes(n.ToString()));

    // wait for the reply, max 5 s (throws TimeoutException)
    var result = await reply.Task.WaitAsync(TimeSpan.FromSeconds(5));

    Console.WriteLine($"[client] {correlationId}: got {result}");
}

static long Fibonacci(int n) => n < 2 ? n : Fibonacci(n - 1) + Fibonacci(n - 2);
