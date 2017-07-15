# GOMQ

**GOMQ** (**G**reat **O**verpowered **M**essage **Q**ueuer) is a message broker
written in Go using `gRPC` and `redis` to connect simulaneous publishers
and subscribers.

# How it works

## Remote Procedure Call (RPC)

The Message Queuer provides clients with four RPC functions:

  * **Publish()**: will publish some payload to a key
  * **Subscribe()**: subscribe to a key
  * **Unsubscribe()**: unsubscribe from a key
  * **Observe()**: fetch payloads from subscribed keys

The client has to pull itself the server, since `gRPC` does not support (yet?)
direct call from server to client.

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
  * `LASTACTION_`: prefix for a given peer, contain the timestamp of its last
  call to Observe(). If the client has not observed for 30 seconds, it is
  considered to be a timeout and the peer is disconnected.
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
