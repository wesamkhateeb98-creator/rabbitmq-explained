using System.Text;
using RabbitMQ.Client;

// 28 shovel: publish to rabbit1, read the same data from rabbit2
// no setup here: the brokers import the queues and the shovel from definitions.json at boot
var rabbit1 = new ConnectionFactory { HostName = "localhost", Port = 5721, UserName = "admin", Password = "admin" };

var rabbit2 = new ConnectionFactory { HostName = "localhost", Port = 5722, UserName = "admin", Password = "admin" };

// one connection to each broker
await using var connection1 = await rabbit1.CreateConnectionAsync();

await using var connection2 = await rabbit2.CreateConnectionAsync();

// rabbit1 channel in confirm mode: each publish returns once rabbit1 has the message
await using var channel1 = await connection1.CreateChannelAsync(new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true));

await using var channel2 = await connection2.CreateChannelAsync();

// the shovel is a consumer on rabbit1's queue "jobs"
// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
Console.WriteLine($"rabbit1: queue jobs, consumers = {(await channel1.QueueDeclarePassiveAsync("jobs")).ConsumerCount} (the shovel)");

// publish job-1..job-3 to queue "jobs" on rabbit1 (default exchange)
for (var i = 1; i <= 3; i++)
{
    await channel1.BasicPublishAsync("", "jobs", Encoding.UTF8.GetBytes($"job-{i}"));

    Console.WriteLine($"rabbit1: publish jobs <- job-{i}");
}

// poll rabbit2's queue every 250 ms (max 10 s) until the 3 jobs arrive
// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
for (var i = 0; i < 40 && (await channel2.QueueDeclarePassiveAsync("jobs")).MessageCount < 3; i++)
    await Task.Delay(250);

// the shovel acked them on rabbit1 after rabbit2 confirmed: moved, not copied
// passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
Console.WriteLine($"rabbit1: jobs = {(await channel1.QueueDeclarePassiveAsync("jobs")).MessageCount} messages left");

// pull every message from rabbit2 until the queue is empty
while (await channel2.BasicGetAsync("jobs", autoAck: true) is { } message)
    Console.WriteLine($"rabbit2: jobs <- {Encoding.UTF8.GetString(message.Body.Span)}");
