# agde

> A general decentralized real-time sync library supporting text and binary.

`agde` is a set of _libraries_ to handle _syncing_ clients in a network.

It can sync whenever, like Git, but doesn't store a history. This means the storage it uses is much lower. It's also faster, and merging is automatic. This enables real-time cooperation.

# Components

## den

`den` is the low-level underlying difference algorithm. It works on _general_ data, both binary and text.

It supports both syncing a local file with a remote's using a cheap, rsync-esque algo and obtaining a diff between local files to share with others.

## agde

`agde` takes the local diff function of [`den`](#den) and builds a framework around it to provide _asynchronous_ arrival of events.
This enables unreliable networks (such as the internet) to be used in a live environment, without any data getting lost.
It also takes care of validating data integrity.

The architecture is a _decentralized pier-to-pier network_.
The implementer can choose to transmit data via _WebSockets_, _WebRTC_, or _any other_ (semi-reliable) protocol.

For most cases, you should use [`agde-io`](#agde-io), which takes care of most of the heavy-lifting.

## agde-io

This builds a async runtime to simplify accessing files and network IO with other clients.
`agde-io` also takes care of all the types of messages and when to call check-up functions. This is all configurable.
