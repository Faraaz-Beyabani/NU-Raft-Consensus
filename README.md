A personal implementation of the Raft Consensus Algorithm for a Northwestern University class.

Implemented code can be found in
* [src/raft/raft.go](https://github.com/Faraaz-Beyabani/NU-Raft-Consensus/blob/master/src/raft/raft.go)
* [src/main/wc.go](https://github.com/Faraaz-Beyabani/NU-Raft-Consensus/blob/master/src/main/wc.go)
* [src/mapreduce/common_map.go](https://github.com/Faraaz-Beyabani/NU-Raft-Consensus/blob/master/src/mapreduce/common_map.go)
* [src/mapreduce/common_reduce.go](https://github.com/Faraaz-Beyabani/NU-Raft-Consensus/blob/master/src/mapreduce/common_reduce.go)
* [src/mapreduce/schedule.go](https://github.com/Faraaz-Beyabani/NU-Raft-Consensus/blob/master/src/mapreduce/schedule.go)

---

# Important Dependencies
* [Go 1.13](https://golang.org/dl/)

# Build Instructions
* Clone this repo
* Set GOPATH environment variable in the root folder: `export “GOPATH=$PWD”`
* Run the following tests:
  * `go test -run Sequential` in `src/mapreduce`
  * `go test -run TestParallel` and `go test -run Failure` in `src/mapreduce`
  * `go test -run 3A` and `go test -run 3B` in `src/raft`
