using System.Text;
using MQTTnet;
using MQTTnet.Protocol;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using RabbitMQ.Client;

// 30: one broker, three protocols: AMQP 1.0, MQTT 5 and AMQP 0-9-1 see the same messages
// AMQP 0-9-1 connection (the classic client) on port 5672
var factory = new ConnectionFactory { HostName = "localhost" };

// open the TCP connection
await using var amqp091 = await factory.CreateConnectionAsync();

// open a channel for the commands below
await using var channel = await amqp091.CreateChannelAsync();

// ---- Part A: AMQP 1.0 (RabbitMQ.AMQP.Client) publishes, AMQP 0-9-1 consumes
Console.WriteLine("Part A: AMQP 1.0 -> quorum queue -> AMQP 0-9-1");

// AMQP 1.0 client, default localhost:5672 (same port; the broker detects the protocol)
var environment = AmqpEnvironment.Create(ConnectionSettingsBuilder.Create().Build());

// AMQP 1.0 connection
var amqp10 = await environment.CreateConnectionAsync();

// AMQP 1.0 management API: declare/delete queues over the same connection
var management = amqp10.Management();

// start clean
await management.Queue("amqp10.orders").DeleteAsync();

// declare a quorum queue
await management.Queue("amqp10.orders").Type(QueueType.QUORUM).DeclareAsync();

// publisher that targets the queue address directly (no exchange/key)
var publisher = await amqp10.PublisherBuilder().Queue("amqp10.orders").BuildAsync();

for (var i = 1; i <= 2; i++)
{
    // each publish returns the broker's outcome (Accepted = stored)
    var result = await publisher.PublishAsync(new AmqpMessage($"order-{i}"));

    Console.WriteLine($"  AMQP 1.0 publish order-{i} -> outcome {result.Outcome.State}");
}

// read the same messages with the AMQP 0-9-1 client
while (await channel.BasicGetAsync("amqp10.orders", autoAck: true) is { } message)
    Console.WriteLine($"  AMQP 0-9-1 got '{Encoding.UTF8.GetString(message.Body.Span)}'");

// close the publisher, delete the queue, close the AMQP 1.0 connection
await publisher.CloseAsync();

await management.Queue("amqp10.orders").DeleteAsync();

await amqp10.CloseAsync();

// ---- Part B: MQTT topic sensors/room1/temp == AMQP routing key sensors.room1.temp on amq.topic
Console.WriteLine("\nPart B: MQTT <-> AMQP 0-9-1 through the amq.topic exchange");

// start clean
await channel.QueueDeleteAsync("mqtt.readings");

// AMQP 0-9-1 queue for the MQTT readings
await channel.QueueDeclareAsync("mqtt.readings", durable: true, exclusive: false, autoDelete: false);

// MQTT publishes go to amq.topic; "/" in the topic becomes "." in the routing key
await channel.QueueBindAsync("mqtt.readings", "amq.topic", "sensors.*.temp");

// MQTT client (MQTTnet)
var mqttFactory = new MqttClientFactory();

using var sensor = mqttFactory.CreateMqttClient();

// MQTT 5 connection to the MQTT plugin on port 1883
var options = new MqttClientOptionsBuilder()
    .WithTcpServer("localhost", 1883)
    .WithCredentials("guest", "guest")
    .WithClientId("sensor-room1")
    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
    .Build();

// connect as client id "sensor-room1"
await sensor.ConnectAsync(options);

// MQTT subscriber for alerts that an AMQP app publishes
var alert = new TaskCompletionSource<string>();

// fires for each message on a subscribed topic
sensor.ApplicationMessageReceivedAsync += ea =>
{
    alert.TrySetResult($"topic '{ea.ApplicationMessage.Topic}' payload '{ea.ApplicationMessage.ConvertPayloadToString()}'");

    return Task.CompletedTask;
};

// subscribe "alerts/#" (MQTT wildcard # = any levels) = binding "alerts.#" on amq.topic
await sensor.SubscribeAsync("alerts/#", MqttQualityOfServiceLevel.AtLeastOnce);

foreach (var (topic, value) in new[] { ("sensors/room1/temp", "21.5"), ("sensors/room2/temp", "23.0"), ("sensors/room1/humidity", "40") })
{
    // QoS 1 (AtLeastOnce) = the broker acks each publish
    var message = new MqttApplicationMessageBuilder()
        .WithTopic(topic)
        .WithPayload(value)
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .Build();

    // topic sensors/room1/temp -> routing key sensors.room1.temp on amq.topic
    await sensor.PublishAsync(message);

    Console.WriteLine($"  MQTT publish {topic,-23} = {value}");
}

// give the broker time to route
await Task.Delay(500);

// only the 2 "temp" readings match sensors.*.temp
while (await channel.BasicGetAsync("mqtt.readings", autoAck: true) is { } reading)
    Console.WriteLine($"  AMQP 0-9-1 got key '{reading.RoutingKey}' body '{Encoding.UTF8.GetString(reading.Body.Span)}'");

// AMQP 0-9-1 publish; key alerts.fire.floor2 = MQTT topic alerts/fire/floor2
await channel.BasicPublishAsync("amq.topic", "alerts.fire.floor2", Encoding.UTF8.GetBytes("evacuate"));

Console.WriteLine("  AMQP 0-9-1 publish amq.topic key 'alerts.fire.floor2'");

// the MQTT subscriber receives it (max 5 s)
Console.WriteLine($"  MQTT got {await alert.Task.WaitAsync(TimeSpan.FromSeconds(5))}");

// clean up
await sensor.DisconnectAsync();

await channel.QueueDeleteAsync("mqtt.readings");
