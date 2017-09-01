# ZMQ REQ/ROUTER Template

This package is a template for designing asynchronous ZMQ servers with the REQ/ROUTER pattern.

## Quickstart

Install the server/client system using `go get` as follows:

```
$ go get github.com/bbengfort/rtreq/...
```

You should then have the `rtreq` command installed on your system:

```
$ rtreq --help
```

You can run the server as follows:

```
$ rtreq serve
```

And send messages from the client as:

```
$ rtreq send "hello world"
```

Note the various arguments you can pass to both serve and send to configure the setup. Run benchmarks with the `bench` command:

```
$ rtreq bench
```

The primary comparison is `REQ/REP` vs `REQ/ROUTER` sockets. 
