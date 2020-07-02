# RawKV

[![][travis-badge]][travis] [![][goreport-badge]][goreport]

RawKV is a cloud-native distributed key-value database aiming to run on Kubernetes clusters, with focuses on:

- Arbitrary raw bytes as keys and values
- Write-optimized storage engine based on log-structured merge-tree
- Reliable data replication with Raft consensus algorithm
- Favor availability over consistency in the presence of network partitions

## Installation

### Build

Pass single-node unit tests:

```
$ go test ./pkg/...
```

Install binaries:

```
$ go install ./pkg/...
```

Build docker image:

```
$ docker build -t rawkvsvr:latest -f ./pkg/cmd/rawkvsvr/Dockerfile .
```

### Deploy

RawKV requires a running Kubernetes cluster to function. Once the cluster has been established, start deployment:

```
$ kubectl apply -f kubernetes/rawkv.yaml
```

After the deployment succeeds, get exposed port number to the load balancer of RawKV pods:

```
$ kubectl get services
NAME       TYPE       CLUSTER-IP      EXTERNAL-IP   PORT(S)                   AGE
...        ...        ...             ...           ...                       ...
rawkv-lb   NodePort   10.103.220.60   <none>        8000:<exposed port>/TCP   5s
...        ...        ...             ...           ...                       ...
```

Use the client binary `rawkvcli` to send requests to the load balancer:

```
$ $GOPATH/bin/rawkvcli put -key name -val rawkv -addr 127.0.0.1:30000
```

The command above sends a put request to add a `(name,rawkv)` key-value pair to RawKV exposed on port `30000`. Use the `-h` option to see more usages of `rawkvcli`.

Stop RawKV services:

```
$ kubectl delete -f kubernetes/rawkv.yaml
$ kubectl delete pvc -l app=rawkv
```

## License

See the [LICENSE](./LICENSE.md) file for license rights and limitations.

[travis]: https://travis-ci.org/chuyangliu/rawkv
[travis-badge]: https://travis-ci.org/chuyangliu/rawkv.svg?branch=master

[goreport]: https://goreportcard.com/report/github.com/chuyangliu/rawkv
[goreport-badge]: https://goreportcard.com/badge/github.com/chuyangliu/rawkv
