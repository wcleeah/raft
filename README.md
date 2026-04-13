# Raft
An implementation of the Raft paper, no external libraries, raw TCP communication, with deterministic testing.

## Project Structure

## Raft Core
Lives under `internal/core`. These are the essential component of what a Raft implementation has. That includes:
- `internal/core/appendentry.go` -> logic for handling append only logs, accepting a store dependency for persistence purpose
- `internal/core/logger.go` -> not quite Raft related, but logger for the whole application 
- `internal/core/raftstate.go` -> the heart and soul of the Raft implementation, election handling, append entry index tracking etc. All the states and their handling lives here
- `internal/core/store.go`, `internal/core/timer.go`, `internal/core/transport.go` -> interfaces for dependencies, essential for running the node, but not core Raft coponents

## RPC
Lives under `internal/rpc`. The directory stores all the protocol definitions, with decode and encode support.
- `internal/rpc/procedure.go` -> all the protocol definitions.

## Runtime
Lives under `internal/runtime`. This is the brain of the Raft implementation, it wires up all the dependencies and the Raft core together. Also handles state event change, timer tick, RPC coordination. 
- `internal/runtime/brain.go` -> The brain of the Raft implementations, does all of the above
- `internal/runtime/server/` -> Implementations of all the dependencies interfaces, which is wired up to be used by Brain.

## Testing

## Running It
The server is config file based, checkout the example configs under `examples/three-node/`, to start the servers:
```bash
go run ./cmd/server -config examples/three-node/node1.json
go run ./cmd/server -config examples/three-node/node2.json
go run ./cmd/server -config examples/three-node/node3.json
```

There is a client command for sending command as well:
```bash
go run ./cmd/client add -addr 127.0.0.1:9001 -delta 3
go run ./cmd/client minus -addr 127.0.0.1:9002 -delta 1
go run ./cmd/client flip -addr 127.0.0.1:9003
```
Note:
- The client follows one redirect automatically if it hits a follower that knows the current leader.
- The example configs use a startup grace on election timeouts so you can bring the three nodes up by hand without the first node racing through many failed election terms immediately.

Server config format:
```json
{
  "id": "node1",
  "addr": "127.0.0.1:9001",
  "store_path": "examples/three-node/data/node1.log",
  "log_level": "info",
  "transport": {
    "read_timeout_ms": 10000,
    "write_timeout_ms": 10000
  },
  "timers": {
    "heartbeat": {
      "min_ms": 100,
      "max_ms": 100
    },
    "election_timeout": {
      "min_ms": 500,
      "max_ms": 900,
      "startup_grace_ms": 5000
    },
    "wait_before_election": {
      "min_ms": 150,
      "max_ms": 250
    },
    "restart_election": {
      "min_ms": 300,
      "max_ms": 450
    }
  },
  "peers": [
    {
      "id": "node2",
      "addr": "127.0.0.1:9002"
    },
    {
      "id": "node3",
      "addr": "127.0.0.1:9003"
    }
  ]
}
```
Note:
- `peers` should exclude the current node.
- `store_path` is created on demand by the server command.
- Each timer supports `min_ms` and `max_ms`; fixed intervals are just `min_ms == max_ms`.
- `startup_grace_ms` is applied only to the first tick of that timer.
- The example configs intentionally use slightly different election timings per node.

## Learnings
- Deterministic testing: fair to say the current project structure is not suitable for a full fledge deterministic testing. The best way to do is a single event loop architecture.
- However, I decided to go with the sparse concurrency architecture knowing this, to gain more experience and exposure on concurrency development especially in Go, i learnt a lot.
  - sync.Cond is really a underrated feature, but it is also very easily misused. Mutex, channel cover most of the cases.
  - Select + Channel really is a headache, i developed a love hate relationship with the pair.
  - The best way to solve concurrency problem is not by slapping a mutex and call it a day, identifying which path is unsafe for access, with a well defined boundary is more important.

## Improvements
- There are many cases my `tests/internal/runtime/brain_test.go` has not covered, i should add them.
- The runtime and core are still entangled a bit, state machine should be a dependency, runtime bytes reading should not assume byte layout.
