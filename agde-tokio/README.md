# agde-tokio

An application for connecting to a network of clients sharing up-to-date data.
This is the building stone for anything regarding live data - cooperative document writing; chatting; synchronization between devices.

## Server

This binary builds on [Tokio](https://tokio.rs) to produce a stable application, useful as a always-on client (i.e. a server).
This is needed, as when only one client is on the network, no one else get the updates. This server acts as a client which always listens and informs volatile clients what they missed when offline.
