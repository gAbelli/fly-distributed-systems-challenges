# Fly.io Distributed System Challenges

These are my solutions to the distributed system challenges by Fly.io, available [here](https://fly.io/dist-sys/).
All the programs are written in Go and have been tested against the [maelstrom](https://github.com/jepsen-io/maelstrom) workloads.

## Challenge 1: Echo

This is just a test program, nothing particular to say.

## Challenge 2: Unique ID Generation

In a distributed system, there are many possible strategies to assign unique IDs. Some of them are easier to implement but don't actually provide 100% certainty of never generating a duplicate ID (for example you could just generate a long enough random string and hope that it is unique), while others are safer.
As far as I know, one of the most successful algorithms is Twitter's [Snowflake](https://developer.twitter.com/en/docs/twitter-ids), which guarantees uniqueness of the IDs and also makes sure that an alphabetic sorting of the IDs will correspond almost exactly to a chronological sorting.

In this case, every server has a unique ID, and we are also under the assumption that the servers are never shut down (even though there might be network partitions). Hence, a simple approach is to have each server generate IDs incrementally (starting from 0 and growing), and prefixing this number with the server ID. This is enough to pass the tests, but again I would probably use a Snowflake-style algorithm in a real-world system.

## Challenge 3: Broadcast

### 3a: Single-Node Broadcast

This is a very simple exercise because we can just store all messages in memory. Some important considerations are the following:

- Since we don't care about the order of the messages, and the messages are guaranteed to be unique, we can store them in a set instead of an array.
- Since the RPCs can happen concurrently, we should protect this set with a lock or some other form of concurrent-safe access to the data structure. In Go it's very idiomatic to use channels, but in this case having a simple lock is probably the most straight-forward approach.

### 3b: Multi-Node Broadcast

This one is much more interesting. The idea is that we have to broadcast messages between the nodes using a gossip algorithm, but we are free to decide how to do it and we have no particular restrictions.
Obviously, one would implement this system differently depending on the use case. For example, you could desire to optimize

- the total number of information exchanges between the nodes
- the latency between the moment in which the first node receives the message and the moment in which all the nodes have seen it
- the load on each specific node

For this first version of the algorithm, we will use the topology given to us by maelstrom, so we won't have control over the load on each individual node. Since we are not required to have all messages being propagated instantly to all nodes, we can propagate messages every N milliseconds, where N is a parameter that we can tune as we wish, instead of sending an RPC for every new message received by a node. This choice increases the latency, but drastically reduces the total number of messages exchanged.
Also, an easy optimization is to keep track of the messages that have been propagated (and acknowledged) by every other node that our server can communicate with, and avoid sending them the same messages more than once.

### 3c: Fault Tolerant Broadcast

Our solution from the previous exercise is already fault tolerant because messages are re-sent if they are not acknowledged.

### 3d: Efficient Broadcast

In order to achieve lower latency and a higher number of messages per operation, a good idea is to use a different topology from the one that maelstrom suggests us. The idea is that we want to reduce the maximum distance between two nodes in the graph, and also reduce the amount of cycles (to avoid duplication). A topology that satisfies the desired property is one in which there is only one "master" node that can communicate with everybody, and all other nodes are "slaves" that can only communicate with the master. This topology technically provides eventual consistency, because if in the end all network partitions are eliminated, all nodes are able to sync with each other.
This configuration is able to achieve a median latency below 270 ms and a maximum latency below 390 ms on my PC, as well as a number of messages per operations of about 12.

Of course, in a real-world system, this configuration would not be ideal because it is not fault tolerant, and also puts too much pressure on a single node. One possible fix would be to subdivide the graph into N clusters, and elect one master node in each cluster. All master nodes could then be able to communicate with each other, while slaves would only be able to send and receive messages from the master of their cluster.

### 3e: Efficient Broadcast, Part II

Our previous system already achieves all the desired performance metrics.

## 4: Grow-Only Counter

Having access to a sequential key-value store, it's quite easy to implement a grow-only counter. In fact, we can associate to each server a key in the key-value store (corresponding to the server's ID) and the value associated with this key will simply represent the counter of the server. Then, whenever you want to read the total counter, you can simply query the key-value store to get the partial counts from all the servers, and then add them up to get the result.
One problem with this approach is the following. Suppose a client sends an `add` RPC to server 1, and immediately after sends a `read` RPC to server 2. If server 1 was slow to communicate with the key-value store, the increment would not be registered. A solution to this problem would be to have each server read the counters for other servers not directly from the key-value store, but from the other servers themselves, by sending them an RPC. However, this drastically increases the latency of the system, since the latency for a client request would be bound by the maximum latency for the connection between two servers.

## 5: Kafka-Style Log

### 5a: Single-Node Kafka-Style Log

In this case everything is easy because we can store everything in an array in memory. The offsets will simply represent the positions of the elements in the array.

### 5b: Multi-Node Kafka-Style Log

Having multiple servers trying to write values associated with the same keys is complicated if there are many concurrent writes. Therefore, we solve the problem at its root by associating each key with a single server by using hash partitioning. So if a server gets a `send` request for a key that it is not responsible for, it simply forward the request to the correct server.
Of course, this method sometimes sacrifices availability for simplicity of the implementation. If we wanted to do something more intelligent, we could proceed as follows. First of all, we make sure that every server can write values associated to every key, avoiding concurrency issues by always doing Compare-And-Swap operations to make sure that everything is correct. However, each node preferably forwards requests to the correct node as we said before. Only if the latter does not respond, the former updates the value himself.

### 5c: Efficient Kafka-Style Log

We already discussed optimizations in the previous exercise.

## 6: Totally-Available Transactions

See [mini-etcd](https://github.com/gAbelli/mini-etcd).
