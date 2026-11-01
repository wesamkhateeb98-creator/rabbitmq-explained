# RabbitMQ Explained: project notes

## Docs ↔ code
- Every topic = `docs/NN-<topic>.md` + `examples/NN-<topic>/Program.cs`.
- **Explain new code in the doc.** When an example is added or changed, its doc's `## Code` section must have:
  1. a link to the `Program.cs`,
  2. a short C# snippet of the key calls,
  3. a **Code walkthrough** table, one row per key call:

     | Code | What it does | Why |
     |---|---|---|
     | `BasicQosAsync(0, 2, false)` | Max 2 un-acked messages per consumer | Fair dispatch, no hoarding |

  4. the real `dotnet run` output (run it; never invent numbers).
- Keep the snippet and the walkthrough in sync with `Program.cs` (same variable names).

## Explain code in comments
- Every API call in a doc snippet **and** in `Program.cs` gets a `//` comment directly above it (no blank line between) saying **what it does** in plain words, with real values.
- Explain each argument that isn't obvious (`durable`, `autoAck`, `mandatory`, `x-*` arguments, `passive`, ...).
- Never assume the reader knows the method name. Example:

  ```csharp
  // passive declare = check only, never creates; returns MessageCount / ConsumerCount, 404 if missing
  var stats = await channel.QueueDeclarePassiveAsync("jobs");
  ```
