# agde

> A general decentralized real-time sync library supporting text and binary.

`agde` is a set of _libraries_ to handle _syncing_ clients in a network.

It can sync whenever, like Git, but doesn't store a history. This means the storage it uses is much lower. It's also faster, and merging is automatic. This enables real-time cooperation.

# Components

## dach

`dach` is the low-level underlying difference algorithm. It works on _general_ data, both binary and text.

It supports both syncing a local file with a remote's using a cheap, rsync-esque algo and obtaining a diff between local files to share with others.

## agde

`agde` takes the local diff function of [`dach`](#dach) and builds a framework around it to provide _asynchronous_ arrival of events.
This enables unreliable networks (such as the internet) to be used in a live environment, without any data getting lost.
It also takes care of validating data integrity.

The architecture is a _decentralized pier-to-pier network_.
The implementer can choose to transmit data via _WebSockets_, _WebRTC_, or _any other_ (semi-reliable) protocol.

For most cases, you should use [`agde-io`](#agde-io), which takes care of most of the heavy-lifting.

## agde-io

This builds a async runtime to simplify accessing files and network IO with other clients.
`agde-io` also takes care of all the types of messages and when to call check-up functions. This is all configurable.

## agde-tokio

A native binary implementation of agde-io using Tokio and WebSockets. This might be expanded in the future to include WebRTC.
Useful for desktop applications and servers. It's currently lacking [clean detection](#clean-detection).

## agde-web

An implementation of agde-io on the web, requiring the user to give JS functions for saving, reading, and listing resources.
It uses WebSockets for communication. This might be expanded in the future to include WebRTC.

## agde-web.js

An implementation of agde-web using localforage to store the data.

# Clean detection

To know when to push and pull changes, agde benefits from knowing when the
resources are up to date and modifiable by agde (e.g. written to disk).

On the web, this is achieved by manually saving the data and calling `commit_and_send` on a regular interval.
