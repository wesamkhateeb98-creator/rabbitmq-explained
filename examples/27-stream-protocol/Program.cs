using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

// 27: the native stream protocol (port 5552): offset tracking and super streams
// native stream protocol client: localhost:5552, user guest
var system = await StreamSystem.Create(new StreamSystemConfig());

// ---- Part A: server-side offset tracking
Console.WriteLine("Part A: consumer 'billing' stores its offset on the broker");

// start clean: delete the stream from a previous run
if (await system.StreamExists("orders.stream"))
    await system.DeleteStream("orders.stream");

// create the stream; segments older than 7 days are deleted
await system.CreateStream(new StreamSpec("orders.stream") { MaxAge = TimeSpan.FromDays(7) });

// completed when all 5 sends are confirmed
var confirmed = new TaskCompletionSource();

var confirmations = 0;

// producer for one stream
var producer = await Producer.Create(new ProducerConfig(system, "orders.stream")
{
    // called by the broker for each message: Confirmed = stored
    ConfirmationHandler = confirmation =>
    {
        if (confirmation.Status == ConfirmationStatus.Confirmed && Interlocked.Increment(ref confirmations) == 5)
            confirmed.TrySetResult();

        return Task.CompletedTask;
    },
});

// send order-0..order-4 (offsets 0..4); Send doesn't wait for the confirm
for (var i = 0; i < 5; i++)
    await producer.Send(new Message(Encoding.UTF8.GetBytes($"order-{i}")));

// wait for the 5 confirms (max 10 s)
await confirmed.Task.WaitAsync(TimeSpan.FromSeconds(10));

Console.WriteLine("  produced order-0..order-4, 5 confirms");

// close the producer
await producer.Close();

// first run: read 3 messages, store the offset of the last one, stop
// start at offset 0, read 3 messages, store offset 2
await Read("billing", new OffsetTypeFirst(), take: 3, store: true);

// ask the broker which offset "billing" stored -> 2
var stored = await system.QueryOffset("billing", "orders.stream");

Console.WriteLine($"  broker: stored offset for 'billing' = {stored}");

// restart: resume after the stored offset
// start at offset 3 (stored + 1) -> order-3, order-4
await Read("billing", new OffsetTypeOffset(stored + 1), take: 2, store: false);

// ---- Part B: super stream = one logical stream split into partitions
Console.WriteLine("\nPart B: super stream 'invoices' with 3 partitions, routed by customer id");

// start clean
if (await system.SuperStreamExists("invoices"))
    await system.DeleteSuperStream("invoices");

// super stream = 1 logical stream made of invoices-0, invoices-1, invoices-2
await system.CreateSuperStream(new PartitionsSuperStreamSpec("invoices", 3));

var superProducer = await Producer.Create(new ProducerConfig(system, "invoices")
{
    // hash of MessageId (customer id) picks the partition: one customer -> always the same partition
    SuperStreamConfig = new SuperStreamConfig { Routing = message => message.Properties.MessageId.ToString()! },
});

// 8 invoices for 4 customers (customer-0..3)
for (var i = 0; i < 8; i++)
{
    var customer = $"customer-{i % 4}";

    await superProducer.Send(new Message(Encoding.UTF8.GetBytes($"invoice-{i}")) { Properties = new Properties { MessageId = customer } });
}

// partition name -> the invoices it delivered
var partitions = new ConcurrentDictionary<string, ConcurrentQueue<string>>();

var received = 0;

// completed when all 8 invoices are read
var allReceived = new TaskCompletionSource();

// consumer on every partition of the super stream
var superConsumer = await Consumer.Create(new ConsumerConfig(system, "invoices")
{
    // IsSuperStream = read all partitions; Reference = consumer name for offset tracking
    IsSuperStream = true,
    Reference = "invoice-printer",
    // start each partition from the beginning
    OffsetSpec = new OffsetTypeFirst(),
    // "stream" = the partition this message came from
    MessageHandler = (stream, _, _, message) =>
    {
        var body = Encoding.UTF8.GetString(message.Data.Contents.ToArray());

        partitions.GetOrAdd(stream, _ => new()).Enqueue($"{body}({message.Properties.MessageId})");

        if (Interlocked.Increment(ref received) == 8)
            allReceived.TrySetResult();

        return Task.CompletedTask;
    },
});

// wait max 10 s
await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

foreach (var (partition, messages) in partitions.OrderBy(p => p.Key))
    Console.WriteLine($"  {partition,-11} <- {string.Join(", ", messages)}");

// close everything
await superConsumer.Close();

await superProducer.Close();

await system.Close();

async Task Read(string reference, IOffsetType offset, int take, bool store)
{
    var count = 0;

    var done = new TaskCompletionSource();

    // helper: consumer on "orders.stream"
    var consumer = await Consumer.Create(new ConsumerConfig(system, "orders.stream")
    {
        // consumer name: the stored offset is saved under it
        Reference = reference,
        // where to start: first, or a given offset
        OffsetSpec = offset,
        MessageHandler = async (_, rawConsumer, context, message) =>
        {
            // ignore messages after the first "take"
            if (count >= take)
                return;

            Console.WriteLine($"  {reference} read {Encoding.UTF8.GetString(message.Data.Contents.ToArray())} @ offset {context.Offset}");

            if (++count == take)
            {
                // save "processed up to this offset" on the broker
                if (store)
                    await rawConsumer.StoreOffset(context.Offset);

                done.TrySetResult();
            }
        },
    });

    // wait max 10 s
    await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

    // stop the consumer
    await consumer.Close();

    Console.WriteLine(store ? "  consumer stopped (offset stored)" : "  consumer stopped");
}
