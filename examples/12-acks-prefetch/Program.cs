using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 12: manual acks + prefetch (QoS)
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var connection = await factory.CreateConnectionAsync();

// admin channel: setup, publishing and reading queue stats
await using var admin = await connection.CreateChannelAsync();

// start clean
await admin.QueueDeleteAsync("jobs");

// create queue "jobs"
await admin.QueueDeclareAsync("jobs", durable: false, exclusive: false, autoDelete: false);

// publish job-1..job-5 via the default exchange
for (var i = 1; i <= 5; i++)
    await admin.BasicPublishAsync("", "jobs", Encoding.UTF8.GetBytes($"job-{i}"));

Console.WriteLine("published job-1..job-5");

// ---- Part A: prefetch 2 and a consumer that never acks
Console.WriteLine("\nPart A: prefetch = 2, consumer never acks");

// separate channel for the consumer that never acks
var lazy = await connection.CreateChannelAsync();

// prefetch: max 2 unacked messages (prefetchSize 0 = no byte limit, global false = per consumer)
await lazy.BasicQosAsync(prefetchSize: 0, prefetchCount: 2, global: false);

// consumer object: the broker pushes messages to it
var lazyConsumer = new AsyncEventingBasicConsumer(lazy);

// receives but never acks -> after 2 messages the broker stops sending
lazyConsumer.ReceivedAsync += (_, ea) =>
{
    Console.WriteLine($"  got {Encoding.UTF8.GetString(ea.Body.Span)} (not acked)");

    return Task.CompletedTask;
};

// autoAck: false = messages stay "unacked" until acked
await lazy.BasicConsumeAsync("jobs", autoAck: false, consumer: lazyConsumer);

// give the broker time to deliver
await Task.Delay(500);

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
var stats = await admin.QueueDeclarePassiveAsync("jobs");

Console.WriteLine($"  queue: ready = {stats.MessageCount}, unacked = {5 - stats.MessageCount}");

// closing the channel returns its 2 unacked messages to the queue
await lazy.CloseAsync();

// give the broker time to requeue
await Task.Delay(200);

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
stats = await admin.QueueDeclarePassiveAsync("jobs");

Console.WriteLine($"  channel closed -> unacked messages requeued, ready = {stats.MessageCount}");

// ---- Part B: ack on success, nack + requeue on failure
Console.WriteLine("\nPart B: ack on success, nack(requeue) when job-3 fails once");

// worker channel for Part B
await using var worker = await connection.CreateChannelAsync();

// same prefetch: max 2 unacked at a time
await worker.BasicQosAsync(prefetchSize: 0, prefetchCount: 2, global: false);

var acked = 0;

// completed when all 5 jobs are acked
var done = new TaskCompletionSource();

// consumer object: the broker pushes messages to it
var consumer = new AsyncEventingBasicConsumer(worker);

// runs once per delivery
consumer.ReceivedAsync += async (_, ea) =>
{
    var job = Encoding.UTF8.GetString(ea.Body.Span);

    // Redelivered = true when this message was delivered before and not acked
    if (job == "job-3" && !ea.Redelivered)
    {
        Console.WriteLine($"  {job} redelivered={ea.Redelivered,-5} -> FAILED, nack(requeue: true)");

        // nack = "failed"; requeue: true = put it back for another try
        await worker.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);

        return;
    }

    Console.WriteLine($"  {job} redelivered={ea.Redelivered,-5} -> ack");

    // ack = "done", the broker deletes it and sends the next one
    await worker.BasicAckAsync(ea.DeliveryTag, multiple: false);

    if (++acked == 5)
        done.SetResult();
};

// manual acks: autoAck false
await worker.BasicConsumeAsync("jobs", autoAck: false, consumer: consumer);

// wait for 5 acks, max 5 s
await done.Task.WaitAsync(TimeSpan.FromSeconds(5));

// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
stats = await admin.QueueDeclarePassiveAsync("jobs");

Console.WriteLine($"  all acked, ready = {stats.MessageCount}");
