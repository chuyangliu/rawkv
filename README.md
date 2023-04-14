# RawKV

[![][goreport-badge]][goreport] [![][godoc-badge]][godoc]

RawKV is a cloud-native distributed key-value database aiming to run on Kubernetes clusters, with focuses on:

- Arbitrary raw bytes as keys and values
- Write-optimized storage engine based on log-structured merge-tree
- Reliable data replication with Raft consensus algorithm
- Favor availability over consistency in the presence of network partitions

## Installation

### Build

Pass unit tests:

```
go test ./pkg/...
```

Install RawKV client:

```
go install ./pkg/cmd/rawkvcli
```

Build docker image of RawKV server:

```
docker build -t rawkvsvr:latest .
```

### Deploy

RawKV requires a running Kubernetes cluster to function. Once the cluster has been established, start deployment:

```
kubectl apply -f rawkv.yaml
```

Get exposed port number of RawKV load balancer specified under `service/rawkv-lb`:

```
kubectl get -f rawkv.yaml
```

Use RawKV client `rawkvcli` to send requests to the load balancer:

```
$GOPATH/bin/rawkvcli put -addr 127.0.0.1:<port> -key name -val rawkv
$GOPATH/bin/rawkvcli get -addr 127.0.0.1:<port> -key name
$GOPATH/bin/rawkvcli del -addr 127.0.0.1:<port> -key name
```

Stop services and clean up:

```
kubectl delete -f rawkv.yaml
kubectl delete pvc -l app=rawkv
```

### Debug

By default, [`rawkv.yaml`](./rawkv.yaml) creates three RawKV replicas:

```
kubectl get pods -l app=rawkv
```

View logs emitted from a replica (index starts with 0):

```
kubectl logs -f rawkv-<index>
```

Delete a replica to simulate node crash:

```
kubectl delete pod rawkv-<index>
```

## License

See the [LICENSE](./LICENSE) file for license rights and limitations.

[goreport]: https://goreportcard.com/report/github.com/chuyangliu/rawkv
[goreport-badge]: https://goreportcard.com/badge/github.com/chuyangliu/rawkv

[godoc]: https://pkg.go.dev/mod/github.com/chuyangliu/rawkv
[godoc-badge]: https://img.shields.io/badge/godoc-reference-blue
