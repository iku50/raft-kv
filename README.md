# raft-kv

A distributed kv storage system use raft and bitcask.

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

the benchmark of bitcask(file io):

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

the benchmark of bitcask(mmap):

```plaintext
goos: darwin
goarch: arm64
pkg: raft-kv/bitcask
BenchmarkGet
BenchmarkGet/128B
BenchmarkGet/128B-8         	 8380644	       134.0 ns/op	 955.02 MB/s
BenchmarkGet/256B
BenchmarkGet/256B-8         	 7635014	       157.1 ns/op	1629.19 MB/s
BenchmarkGet/512B
BenchmarkGet/512B-8         	 5704957	       209.5 ns/op	2443.39 MB/s
BenchmarkGet/1K
BenchmarkGet/1K-8           	 3522381	       338.0 ns/op	3029.69 MB/s
BenchmarkGet/2K
BenchmarkGet/2K-8           	 2124867	       562.2 ns/op	3643.05 MB/s
BenchmarkGet/4K
BenchmarkGet/4K-8           	 1000000	      1013 ns/op	4045.29 MB/s
BenchmarkGet/8K
BenchmarkGet/8K-8           	  636344	      1909 ns/op	4291.34 MB/s
BenchmarkGet/16K
BenchmarkGet/16K-8          	  319039	      3581 ns/op	4575.01 MB/s
BenchmarkGet/32K
BenchmarkGet/32K-8          	  162922	      7392 ns/op	4432.87 MB/s
BenchmarkPut
BenchmarkPut/128B
BenchmarkPut/128B-8         	   61130	     23305 ns/op	   5.49 MB/s
BenchmarkPut/256B
BenchmarkPut/256B-8         	   60801	     31647 ns/op	   8.09 MB/s
BenchmarkPut/512B
BenchmarkPut/512B-8         	   60057	     64230 ns/op	   7.97 MB/s
BenchmarkPut/1K
BenchmarkPut/1K-8           	   53144	     82874 ns/op	  12.36 MB/s
BenchmarkPut/2K
BenchmarkPut/2K-8           	   42188	     82266 ns/op	  24.89 MB/s
BenchmarkPut/4K
BenchmarkPut/4K-8           	   26241	     96671 ns/op	  42.37 MB/s
BenchmarkPut/8K
BenchmarkPut/8K-8           	   14892	    108520 ns/op	  75.49 MB/s
BenchmarkPut/16K
BenchmarkPut/16K-8          	   10000	    112968 ns/op	 145.03 MB/s
BenchmarkPut/32K
BenchmarkPut/32K-8          	   10000	    123725 ns/op	 264.85 MB/s
PASS
```

## Getting Start

```shell
go run app/main.go
```

data will be store in data/ dir
