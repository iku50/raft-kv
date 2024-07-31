# test result of mmap and file_io

```bash
goos: darwin
goarch: arm64
pkg: raft-kv/bitcask/io
BenchmarkFileIO_Write
BenchmarkFileIO_Write-8     	  194695	      5841 ns/op
BenchmarkFileIO_Read
BenchmarkFileIO_Read-8      	 1929114	       616.7 ns/op
BenchmarkMemoryIO_Write
BenchmarkMemoryIO_Write-8   	22198482	        53.72 ns/op
BenchmarkMemoryIO_Read
BenchmarkMemoryIO_Read-8    	25873290	        44.94 ns/op
PASS
```
