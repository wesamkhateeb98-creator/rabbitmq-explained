using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 05: two workers compete for tasks on one queue
// where the broker runs (port 5672, user guest by default)
var factory = new ConnectionFactory { HostName = "localhost" };

// one TCP connection shared by the setup channel and both workers
await using var connection = await factory.CreateConnectionAsync();

// channel used for setup and publishing
await using var setup = await connection.CreateChannelAsync();

// start clean: delete the queue from a previous run
await setup.QueueDeleteAsync("tasks");

// the shared work queue
await setup.QueueDeclareAsync("tasks", durable: false, exclusive: false, autoDelete: false);

const int TaskCount = 6;

// thread-safe log of "worker did task" lines
var handled = new ConcurrentQueue<string>();

// counts down to 0 when all 6 tasks are acked
var done = new CountdownEvent(TaskCount);

foreach (var worker in new[] { "worker-A", "worker-B" })
{
    // each worker gets its own channel
    var channel = await connection.CreateChannelAsync();

    // consumer object: the broker pushes messages to it
    var consumer = new AsyncEventingBasicConsumer(channel);

    // runs once per task delivered to this worker
    consumer.ReceivedAsync += async (_, ea) =>
    {
        var task = Encoding.UTF8.GetString(ea.Body.Span);

        // simulate 100 ms of work
        await Task.Delay(100);

        handled.Enqueue($"{worker} did {task}");

        // ack = "done, delete it"; multiple: false = only this message
        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

        // one more task finished
        done.Signal();
    };

    // autoAck: false = message stays "unacked" until BasicAckAsync; redelivered if the worker dies
    await channel.BasicConsumeAsync("tasks", autoAck: false, consumer: consumer);
}

// send task-1..task-6; the broker round-robins them between the 2 workers
for (var i = 1; i <= TaskCount; i++)
    await setup.BasicPublishAsync("", "tasks", Encoding.UTF8.GetBytes($"task-{i}"));

Console.WriteLine($"published {TaskCount} tasks to queue 'tasks'");

// wait for all 6 acks (max 10 s)
done.Wait(TimeSpan.FromSeconds(10));

foreach (var line in handled.Order())
    Console.WriteLine($"  {line}");
