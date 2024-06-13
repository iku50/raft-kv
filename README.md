# raft-kv

A distributed kv storage system use raft and bitcask.

~~This project is under rapid development and aims to provide a reference implementation for a distributed key-value storage system. Please note that the API and project structure are subject to significant changes as development progresses. This project is intended for reference purposes only.~~

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

## Benchmark

the benchmark of bitcask:

```plaintext
goos: darwin
goarch: arm64
pkg: raft-kv/bitcask
BenchmarkGet
BenchmarkGet/128B
BenchmarkGet/128B-8         	  634050	      1729 ns/op	  74.05 MB/s
BenchmarkGet/256B
BenchmarkGet/256B-8         	  656062	      1727 ns/op	 148.23 MB/s
BenchmarkGet/512B
BenchmarkGet/512B-8         	  627208	      1761 ns/op	 290.70 MB/s
BenchmarkGet/1K
BenchmarkGet/1K-8           	  604882	      1917 ns/op	 534.15 MB/s
BenchmarkGet/2K
BenchmarkGet/2K-8           	  530575	      2328 ns/op	 879.86 MB/s
BenchmarkGet/4K
BenchmarkGet/4K-8           	  427911	      2563 ns/op	1598.40 MB/s
BenchmarkGet/8K
BenchmarkGet/8K-8           	  336121	      3498 ns/op	2341.68 MB/s
BenchmarkGet/16K
BenchmarkGet/16K-8          	  218928	      5299 ns/op	3091.66 MB/s
BenchmarkGet/32K
BenchmarkGet/32K-8          	  128276	      9438 ns/op	3471.79 MB/s
BenchmarkPut
BenchmarkPut/128B
BenchmarkPut/128B-8         	     312	   3741723 ns/op	   0.03 MB/s
BenchmarkPut/256B
BenchmarkPut/256B-8         	     309	   3841109 ns/op	   0.07 MB/s
BenchmarkPut/1K
BenchmarkPut/1K-8           	     309	   3979923 ns/op	   0.26 MB/s
BenchmarkPut/2K
BenchmarkPut/2K-8           	     310	   4150229 ns/op	   0.49 MB/s
BenchmarkPut/4K
BenchmarkPut/4K-8           	     277	   4385481 ns/op	   0.93 MB/s
BenchmarkPut/8K
BenchmarkPut/8K-8           	     319	   4056294 ns/op	   2.02 MB/s
BenchmarkPut/16K
BenchmarkPut/16K-8          	     273	   4242904 ns/op	   3.86 MB/s
BenchmarkPut/32K
BenchmarkPut/32K-8          	     352	   3766518 ns/op	   8.70 MB/s
PASS
```

## Getting Start

```shell
go run app/main.go
```

data will be store in data/ dir
