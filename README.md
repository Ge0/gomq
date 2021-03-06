# GOMQ

**GOMQ** (**G**reat **O**verpowered **M**essage **Q**ueuer) is a message broker
written in Go using `gRPC` and `redis` to connect simulaneous publishers
and subscribers.

# Content

Among the directories, there are:

  * `gomq_server`: the server itself, probably the most relevant part.
  * `routeguide`: the described protocol in protobuff used by gRPC.
  * `gomq_client`: just an example of a client for gomq.
  * `gomq_consumer`: just an example of a consumer for gomq.
  * `gomq_publisher`: just an example of a publisher for gomq.

# How it works

## Configuration

You can configure a few settings like the redis server address, password and
database to use as well as the port you want the server to bind to. Take
a look at the default.json file to see how the configuration is structured.

You can provide the server with your own configuration by setting the
`--config` flag as an argument to the `gomq_server`.

## Remote Procedure Call (RPC)

The Message Queuer provides clients with four RPC functions:

  * **Publish()**: will publish some payload to a key
  * **Subscribe()**: subscribe to a key
  * **Unsubscribe()**: unsubscribe from a key
  * **Observe()**: fetch payloads from subscribed keys

The client has to pull itself the server, since `gRPC` does not support (yet?)
direct calls from server to client.

## Redis usage

The message queuer uses a whole database in redis. It also uses prefixes to
store relevant information:

  * `CONSUMERS_`: list of subscribed consumers for a key. For example,
  if the key is named `FOO`, then the `CONSUMERS_FOO` redis key will contain
  the list of the consumers which have subscribed to `FOO`.
  * `SUBSCRIPTIONS_`: list of subscribed keys for a consumer. For example,
  if the consumer id is `BAR`, then the `SUBSCRIPTIONS_BAR` redis key will
  contain the list of the keys the consumer has subscribed to.
  * `PEERS_`: list of different peers with a name consumer id. The message
  queuer actually supports multiple clients with a same consumer id. Hence,
  if the consumer id is `BAR`, then the `PEERS_BAR` redis key will contain
  the lsit of the peers which shares the same consumer id. A peer is actually
  represented by `remote_addr:port`.
  * `QUEUE_`: the main queue of messages for a given key. If the key is `FOO`,
  then the published messages to this key will be enqueued to the `QUEUE_FOO`
  redis key (which references a list).
  * `MESSAGES_`: prefix of dedicated queue for a given tuple of (key,
  consumer id, peer). if the key is `FOO`, the consumer id is `BAR` and the
  peer is `127.0.0.1:5467`, then the `MESSAGES_FOO_BAR_127.0.0.1:5467` redis
  key will contain the queue of messages published to `FOO`, dedicated to the
  client whose consumer id is `BAR` and remote peer information is
  `127.0.0.1:5467`.
  * `SUBSCRIBED_KEYS`: list of keys that have at least one subscriber.

## Queues

When a message is published, it is enqueued to what is called the
**main queue** (e.g. `QUEUE_KEY`). The elements of this queue will then be
dequeued to every dedicated clients queues
(e.g. `QUEUE_KEY_BAR_127.0.0.1:1234`).

If there is no subscribers, the main queue is not dequeued. As a consequence,
one can publish a paylaod to a key when there is no subscriber at all. When
a client will subscribe to the key, then the message will be dequeued from the
main queue to the dedicated queue of the client.

# Roadmap

  * **Enhance integration tests**: The server should be spawned in a neat way
  before testing its features.
  * **Handle client timeout/disconnection**: There is currently no client
  disconnection nor timeout management, thus leaving artifacts in the redis
  database.
