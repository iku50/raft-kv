# raft-kv

A distributed kv storage system use raft and bitcask.

**This project is under rapid development and aims to provide a reference implementation for a distributed key-value storage system. Please note that the API and project structure are subject to significant changes as development progresses. This project is intended for reference purposes only.**

The implementation of Raft partially refer to [ToniXWD's impl](https://github.com/ToniXWD/MIT6.5840)

## Key Features

* **Raft**: The project utilizes the Raft consensus algorithm to ensure distributed consistency across nodes. Raft is known for its simplicity and reliability in achieving consensus in a distributed system.
* **Bitcask**: The key-value local storage model is based on Bitcask, which provides efficient, log-structured storage for write-intensive workloads. Bitcask is designed for high performance with fast writes and reads, making it ideal for key-value storage.
* **HTTP call method**: Support for simple HTTP call methods, please refer to the following curl for specific API.

  ```shell
  # put key-value
  curl -X POST 'http://127.0.0.1:8080/outspace' -H "Content-Type: application/json" -d '{"value":"outof file"}'
  # return {"success":true}

  # get value
  curl 'http://127.0.0.1:8080/outspace'
  # return {"value":"outof file"}

  # get key
  curl -X DELETE 'http://127.0.0.1:8080/outspace'
  # return {"success":true}

  ```

## Getting Start

```shell
go run app/main.go
```

data will be store in data/ dir
