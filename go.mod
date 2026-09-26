module github.com/acneism/BitKV

go 1.26.1

require (
	github.com/acneism/raft v0.0.0-00010101000000-000000000000
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/raft v1.8.0
	github.com/hashicorp/raft-wal v0.5.0
	go.etcd.io/bbolt v1.5.0
)

require (
	github.com/benbjohnson/immutable v0.4.3 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.7.0 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.4 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/exp v0.0.0-20220827204233-334a2380cb91 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/acneism/raft => ../raft
