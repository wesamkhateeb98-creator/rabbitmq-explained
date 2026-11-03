# 28 · Federation & Shovel (multi-cluster)

A cluster should stay inside one low-latency network ([20](20-clustering.md)). To connect regions, data centers, or an old and a new cluster across a WAN, link **independent** brokers with **Federation** or **Shovel**. Both are plugins, and both are set up with **configuration, not code**.

| | Cluster | Federation | Shovel |
|---|---|---|---|
| Nodes | One logical broker | Independent brokers | Independent brokers |
| Network | Low-latency LAN | WAN-tolerant | WAN-tolerant |
| Moves | Replicates (quorum/streams) | Only what downstream binds | Everything in the source queue |
| Source after the move | n/a | Upstream queues keep their own copy | Source queue is emptied (moved, not copied) |
| Direction | n/a | Downstream pulls | Configurable source → destination |
| Typical use | High availability | Multi-region pub/sub | Migrations, bridging, draining |

Both examples use the same shape: **publish to rabbit1, read the same data from rabbit2**.

| Example | Folder | Ports (AMQP / UI) |
|---|---|---|
| Part 1 · Federation | [examples/28-federation-shovel/federate/](../examples/28-federation-shovel/federate/) | rabbit1 5711 / 15711, rabbit2 5712 / 15712 |
| Part 2 · Shovel | [examples/28-federation-shovel/shovel/](../examples/28-federation-shovel/shovel/) | rabbit1 5721 / 15721, rabbit2 5722 / 15722 |

Each folder has the same files:

| File | Role |
|---|---|
| `docker-compose.yml` | Runs rabbit1 + rabbit2 and mounts the plugins and definitions. rabbit2 is healthy only when its link is `running` |
| `load-definitions.conf` | `load_definitions = /etc/rabbitmq/definitions.json`: import the file at boot |
| `rabbit1/definitions.json` | rabbit1's user and topology |
| `rabbit2/definitions.json` | rabbit2's user and topology **+ the link** (rabbit2 owns it and connects to rabbit1) |
| `Program.cs` | Publishes to rabbit1, reads from rabbit2. No setup code |

---

## Part 1 · Federation

**Definition:** a **federated exchange** on a downstream broker (rabbit2) receives the messages published to the same-named exchange on an upstream broker (rabbit1). It pulls only the routing keys that rabbit2's own bindings ask for.

**Why it matters:** region B gets *some* events of region A (here `order.*`, not `payment.*`) without a stretched cluster. Producers in A don't know B exists.

### Example

Routing table: published to `rabbit1` exchange `orders`. Federation forwards only keys that `rabbit2` has bindings for (`order.*` → `orders.copy`); `rabbit1` holds the link queue `federation: orders -> rabbit@rabbit2`.

| Routing key | Body | Matches `order.*`? | In `rabbit2` `orders.copy`? |
|---|---|---|---|
| `order.created` | order-1 | ✅ | ✅ |
| `order.paid` | order-1 | ✅ | ✅ |
| `payment.failed` | card-7 | ❌ | ❌ never crossed (dropped on `rabbit1`) |
| `order.created` | order-2 | ✅ | ✅ |

### Diagram

```mermaid
flowchart LR
  subgraph R1["rabbit1 (upstream)"]
    P[Producer] --> X1{{"orders"}}
    X1 -->|"order.* (binding copied from rabbit2)"| FQ[("federation: orders -> rabbit@rabbit2")]
  end
  subgraph R2["rabbit2 (downstream)"]
    X2{{"orders"}} -->|"order.*"| Q[("orders.copy")] --> C[Consumer]
  end
  FQ ==>|"federation link (AMQP, pulled by rabbit2)"| X2
```

```mermaid
sequenceDiagram
  participant R2 as rabbit2
  participant R1 as rabbit1
  participant P as Program.cs
  R2->>R1: boot: policy federate-orders -> connect to upstream rabbit1
  R2->>R1: declare queue "federation: orders -> rabbit@rabbit2", bind order.*
  Note over R2: federation_status = running -> healthy
  P->>R1: publish orders (order.created, order.paid, payment.failed, order.created)
  R1->>R1: route: order.* -> link queue, payment.failed -> no match, dropped
  R1-->>R2: link consumes 3 messages, republishes to rabbit2 "orders"
  P->>R2: BasicGet orders.copy -> 3 messages
```

### Code

[examples/28-federation-shovel/federate/](../examples/28-federation-shovel/federate/): the link part of [rabbit2/definitions.json](../examples/28-federation-shovel/federate/rabbit2/definitions.json):

```json
"bindings": [
  { "source": "orders", "vhost": "/", "destination": "orders.copy", "destination_type": "queue", "routing_key": "order.*", "arguments": {} }
],
"parameters": [
  { "component": "federation-upstream", "vhost": "/", "name": "rabbit1", "value": { "uri": "amqp://admin:admin@rabbit1" } }
],
"policies": [
  { "vhost": "/", "name": "federate-orders", "pattern": "^orders$", "apply-to": "exchanges", "priority": 0, "definition": { "federation-upstream": "rabbit1" } }
]
```

**Config walkthrough** (JSON has no comments, so it is explained here):

| Key | What it does | Why |
|---|---|---|
| `bindings` → `orders` → `orders.copy`, key `order.*` | rabbit2's own binding | Federation copies it to rabbit1, so only `order.*` crosses |
| `component: federation-upstream`, `name: rabbit1` | Upstream URI, with the hostname from compose | **Where** to pull from |
| `policies` → `pattern ^orders$`, `federation-upstream: rabbit1` | Exchange `orders` federates from upstream `rabbit1` | **What** to federate; this creates the link |
| `users` → `admin` + `password_hash` | SHA-256 hash of `admin` | `guest` can't log in from another host |
| compose healthcheck `rabbitmqctl -q federation_status \| grep -q running` | `up --wait` returns only when the link runs | Messages published before the link exists are not federated |

[Program.cs](../examples/28-federation-shovel/federate/Program.cs):

```csharp
// the link created this queue on rabbit1: it buffers what rabbit2 asked for (binding order.*)
var linkQueue = await channel1.QueueDeclarePassiveAsync("federation: orders -> rabbit@rabbit2");

// publish to exchange "orders" on rabbit1; the routing key decides what crosses
await channel1.BasicPublishAsync("orders", key, Encoding.UTF8.GetBytes(body));

// pull every message from rabbit2 until the queue is empty
while (await channel2.BasicGetAsync("orders.copy", autoAck: true) is { } message)
    Console.WriteLine(...);
```

```
$ docker compose -f examples/28-federation-shovel/federate/docker-compose.yml up -d --wait
 Container federate-rabbit2-1 Healthy
$ dotnet run --project examples/28-federation-shovel/federate
rabbit1: link queue 'federation: orders -> rabbit@rabbit2', consumers = 1 (rabbit2)
rabbit1: publish orders  key order.created   body order-1
rabbit1: publish orders  key order.paid      body order-1
rabbit1: publish orders  key payment.failed  body card-7
rabbit1: publish orders  key order.created   body order-2
rabbit2: orders.copy <- key order.created   body order-1
rabbit2: orders.copy <- key order.paid      body order-1
rabbit2: orders.copy <- key order.created   body order-2
payment.failed never reached rabbit2: no binding for it, rabbit1 dropped it
$ docker compose -f examples/28-federation-shovel/federate/docker-compose.yml down
```

### ✅ Correct vs ❌ wrong usage

| | Usage | Why |
|---|---|---|
| ✅ | Federation for "region B wants some events of region A" | Only bound routing keys cross the WAN |
| ✅ | The link defined on the **downstream** broker | Downstream pulls; the upstream needs no change |
| ✅ | A dedicated link user with minimal permissions | Its credentials sit in the URI |
| ❌ | One cluster stretched across regions | Partitions and Raft latency; federate instead |
| ❌ | Federating both directions on the same exchange without `max-hops` | Messages can loop (the default `max-hops` is 1) |

### Pitfalls

1. **Asynchronous:** rabbit2 can lag behind rabbit1. While the link is down, messages buffer in the upstream link queue, so set `max-length` or `message-ttl` on the upstream (`"value": {"uri": ..., "max-hops": 1, "message-ttl": 3600000}`).
2. **Messages published before the link exists are not federated:** the link queue doesn't exist yet. Hence the healthcheck.
3. **The link queue's name is fixed:** `federation: <exchange> -> <downstream node>`. Monitor its depth on the upstream.
4. **Plugins:** `rabbitmq_federation` must be enabled on the downstream broker (the demo enables it on both).
5. **Bindings drive everything:** a new binding on rabbit2 is copied to rabbit1 automatically. A deleted one stops that key from crossing.

---

## Part 2 · Shovel

**Definition:** a **shovel** is a built-in consumer + publisher. It consumes from a source queue (here rabbit1 `jobs`) and republishes to a destination (here rabbit2 `jobs`), acking the source only after the destination confirms.

**Why it matters:** it moves *everything* in a queue to another broker. Use it to migrate to a new cluster, drain a queue, or bridge two systems, with no consumer code to write and at-least-once delivery.

### Example

Before/after: shovel `rabbit1-to-rabbit2` consumes `rabbit1` queue `jobs` and republishes to `rabbit2` queue `jobs`.

| Queue | Before | After publishing `job-1, job-2, job-3` to `rabbit1` |
|---|---|---|
| `rabbit1` `jobs` | `[]`, 1 consumer (the shovel) | `[]` (moved, not copied) |
| `rabbit2` `jobs` | `[]` | `[job-1, job-2, job-3]` |

### Diagram

```mermaid
flowchart LR
  subgraph R1["rabbit1 (source)"]
    P[Producer] --> Q1[("jobs")]
  end
  subgraph R2["rabbit2 (destination, owns the shovel)"]
    S(["shovel rabbit1-to-rabbit2"]) --> Q2[("jobs")] --> C[Consumer]
  end
  Q1 ==>|"consume (ack after confirm)"| S
```

```mermaid
sequenceDiagram
  participant S as shovel (on rabbit2)
  participant R1 as rabbit1 jobs
  participant R2 as rabbit2 jobs
  participant P as Program.cs
  S->>R1: boot: connect + consume jobs
  Note over S: shovel_status = running -> healthy
  P->>R1: publish job-1..job-3
  R1->>S: deliver job-1
  S->>R2: publish job-1
  R2-->>S: confirm
  S->>R1: ack job-1 (deleted on rabbit1)
  Note over S,R2: same for job-2, job-3
  P->>R2: BasicGet jobs -> job-1, job-2, job-3
```

### Code

[examples/28-federation-shovel/shovel/](../examples/28-federation-shovel/shovel/): the shovel part of [rabbit2/definitions.json](../examples/28-federation-shovel/shovel/rabbit2/definitions.json):

```json
"parameters": [
  { "component": "shovel", "vhost": "/", "name": "rabbit1-to-rabbit2", "value": {
      "src-protocol": "amqp091", "src-uri": "amqp://admin:admin@rabbit1", "src-queue": "jobs",
      "dest-protocol": "amqp091", "dest-uri": "amqp://", "dest-queue": "jobs"
  } }
]
```

**Config walkthrough:**

| Key | What it does | Why |
|---|---|---|
| `component: shovel`, `name: rabbit1-to-rabbit2` | Dynamic shovel stored in rabbit2 | Survives restarts; shown in `rabbitmqctl shovel_status` |
| `src-uri: amqp://admin:admin@rabbit1`, `src-queue: jobs` | Where to consume from | rabbit1's queue `jobs` |
| `dest-uri: amqp://`, `dest-queue: jobs` | Empty URI = the local broker (rabbit2) | No credentials needed for the local side |
| `*-protocol: amqp091` | AMQP 0-9-1 on both sides (`amqp10` also exists) | Matches the brokers' main protocol |
| default `ack-mode: on-confirm` | Ack on rabbit1 only after rabbit2 confirms | At-least-once: nothing is lost if a broker dies mid-move |
| compose healthcheck `rabbitmqctl -q shovel_status \| grep -q running` | `up --wait` returns only when the shovel runs | The demo reads rabbit2 right after publishing |

[Program.cs](../examples/28-federation-shovel/shovel/Program.cs):

```csharp
// the shovel is a consumer on rabbit1's queue "jobs"
Console.WriteLine($"... consumers = {(await channel1.QueueDeclarePassiveAsync("jobs")).ConsumerCount} (the shovel)");

// publish job-1..job-3 to queue "jobs" on rabbit1 (default exchange)
await channel1.BasicPublishAsync("", "jobs", Encoding.UTF8.GetBytes($"job-{i}"));

// pull every message from rabbit2 until the queue is empty
while (await channel2.BasicGetAsync("jobs", autoAck: true) is { } message)
    Console.WriteLine(...);
```

```
$ docker compose -f examples/28-federation-shovel/shovel/docker-compose.yml up -d --wait
 Container shovel-rabbit2-1 Healthy
$ dotnet run --project examples/28-federation-shovel/shovel
rabbit1: queue jobs, consumers = 1 (the shovel)
rabbit1: publish jobs <- job-1
rabbit1: publish jobs <- job-2
rabbit1: publish jobs <- job-3
rabbit1: jobs = 0 messages left
rabbit2: jobs <- job-1
rabbit2: jobs <- job-2
rabbit2: jobs <- job-3
$ docker compose -f examples/28-federation-shovel/shovel/docker-compose.yml down
```

### ✅ Correct vs ❌ wrong usage

| | Usage | Why |
|---|---|---|
| ✅ | Migrating a queue to a new cluster: add `"src-delete-after": "queue-length"` | Moves the backlog present at start, then the shovel deletes itself |
| ✅ | Keep the default `ack-mode: on-confirm` | At-least-once across brokers |
| ✅ | Shovels in `definitions.json`, kept in git | Reproducible; no setup code |
| ❌ | A shovel when the source must keep its messages | A shovel **moves** them; use Federation or a fanout for copies |
| ❌ | `ack-mode: no-ack` for important data | Faster, but a crash mid-move loses messages |

### Pitfalls

1. **`src-delete-after: queue-length` at boot:** with an empty source queue, the shovel moves 0 messages and deletes itself at once. The demo keeps it running instead.
2. **Competing consumers:** the shovel is one more consumer on the source. Other consumers on rabbit1 `jobs` would take part of the messages.
3. **Duplicates are possible:** if the link drops after the destination confirm but before the source ack, a message is moved twice. Make consumers idempotent ([17](17-idempotency.md)).
4. **`guest` can't be used remotely:** the source URI needs a real user (the demo uses `admin/admin`, demo-only).
5. **Plugins:** `rabbitmq_shovel` must be enabled on the broker that runs the shovel (here rabbit2).

## Next

➡ [29 · Performance tuning](29-performance.md)
