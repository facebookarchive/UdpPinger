
# UpdPinger
UpdPinger is our high performance UDP packet generation, reflection and collection library. It is used internally as the basis for very wide ECMP loss and latency profiling. This release provides two simple binaries, uping and upong, which provide a trivial wrapper to the libraries.

## Requirements
UdpPinger requires
* Linux (with socket option SO_REUSEPORT)
* [Facebook Folly v0.57.0](https://github.com/facebook/folly)
* [Apache Thrift >= 0.9.3](https://thrift.apache.org/download)

## Installing

### Ubuntu 14.04 LTS
* Install Facebook Folly v0.57.0
* Install Apache Thrift
* Ensure the root FD limit is >50000 (we use loads of fd's)
`ulimit -n 500000`
* Make sure you load the new libraries!
`ldconfig`
* Build uping and uping
`git clone github.com/facebook/UdpPinger.git
cd UdpPinger/uping
make
cd ../upong
make`
* Test it out by giving adding your target hosts to target_list....

## License
UdpPinger is BSD-licensed. We also provide an additional patent grant.
