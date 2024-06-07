# raft-kv

A distributed kv storage system use raft and bitcask.

**This project is under rapid development and aims to provide a reference implementation for a distributed key-value storage system. Please note that the API and project structure are subject to significant changes as development progresses. This project is intended for reference purposes only.**

The implementation of Raft partially refer to [ToniXWD's impl](https://github.com/ToniXWD/MIT6.5840)

## Key Features

* **Raft**: The project utilizes the Raft consensus algorithm to ensure distributed consistency across nodes. Raft is known for its simplicity and reliability in achieving consensus in a distributed system.
* **Bitcask**: The key-value storage model is based on Bitcask, which provides efficient, log-structured storage for write-intensive workloads. Bitcask is designed for high performance with fast writes and reads, making it ideal for key-value storage.

## Getting Start

look up [server_test.go](./server/server_test.go) and considered it as an example.
