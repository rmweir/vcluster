package etcd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"

	"github.com/loft-sh/log/scanner"
	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/util/loghelper"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/klog/v2"
)

var (
	k3sDataDir  = path.Join(dataDir, "server")
	k3sKineSock = path.Join(k3sDataDir, "kine.sock")
)

type Value struct {
	Key      []byte
	Data     []byte
	Modified int64
}

var (
	ErrKeyNotFound = errors.New("etcd key not found")
)

type Storage interface {
	runtime.Encoder
	runtime.Decoder

	Start(ctx context.Context) error
	List(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
	Get(ctx context.Context, key string) (Value, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	Close() error
}

type kineStorage struct {
	runtime.Encoder
	runtime.Decoder

	sock       string
	etcdClient *clientv3.Client

	done   chan struct{}
	cancel context.CancelFunc
}

func newKineStorage(sock string) Storage {
	codecFactory := serializer.NewCodecFactory(scheme.Scheme)
	deserializer := codecFactory.UniversalDeserializer()
	protoSerializer := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)

	return &kineStorage{
		Decoder: deserializer,
		Encoder: protoSerializer,

		sock: sock,
	}
}

func (k *kineStorage) Start(ctx context.Context) error {
	ctx, k.cancel = context.WithCancel(ctx)
	k.done = make(chan struct{})
	go func() {
		defer k.cancel()
		defer close(k.done)

		err := runKine(ctx)
		if err != nil {
			klog.ErrorS(err, "error running kine")
		}
	}()

	// get etcd client
	var err error
	klog.Info("Wait for kine to come up")
	k.etcdClient, err = WaitForEtcdClient(ctx, nil, k.sock)
	if err != nil {
		return err
	}

	return nil
}

func (k *kineStorage) Close() error {
	k.cancel()
	<-k.done
	return k.etcdClient.Close()
}

func (k *kineStorage) List(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	getResponse, err := k.etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	return getResponse.Kvs, nil
}

func (k *kineStorage) Get(ctx context.Context, key string) (Value, error) {
	resp, err := k.etcdClient.Get(ctx, key)
	if err != nil {
		return Value{}, err
	}

	if len(resp.Kvs) == 1 {
		return Value{
			Key:      resp.Kvs[0].Key,
			Data:     resp.Kvs[0].Value,
			Modified: resp.Kvs[0].ModRevision,
		}, nil
	}

	return Value{}, ErrKeyNotFound
}

func (k *kineStorage) Put(ctx context.Context, key string, value []byte) error {
	val, err := k.Get(ctx, key)
	if err != nil {
		return err
	}
	if val.Modified == 0 {
		return k.Create(ctx, key, value)
	}
	return k.Update(ctx, key, val.Modified, value)
}

func (k *kineStorage) Create(ctx context.Context, key string, value []byte) error {
	resp, err := k.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("key exists")
	}
	return nil
}

func (k *kineStorage) Update(ctx context.Context, key string, revision int64, value []byte) error {
	resp, err := k.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("revision %d doesnt match", revision)
	}
	return nil
}

func (k *kineStorage) Delete(ctx context.Context, key string) error {
	val, err := k.Get(ctx, key)
	if err != nil {
		return err
	} else if val.Modified == 0 {
		return ErrKeyNotFound
	}

	resp, err := k.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", val.Modified)).
		Then(clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("revision %d doesnt match", val.Modified)
	}
	return nil
}

func runKine(ctx context.Context) error {
	reader, writer, err := os.Pipe()
	if err != nil {
		return err
	}
	defer writer.Close()

	// make data dir
	err = os.MkdirAll(k3sDataDir, 0777)
	if err != nil {
		return err
	}

	// start func
	done := make(chan struct{})
	go func() {
		defer close(done)

		// make sure we scan the output correctly
		scan := scanner.NewScanner(reader)
		for scan.Scan() {
			line := scan.Text()
			if len(line) == 0 {
				continue
			}

			// print to our logs
			args := []interface{}{"component", "kine"}
			loghelper.PrintKlogLine(line, args)
		}
	}()

	cmd := exec.CommandContext(ctx, "/usr/local/bin/kine", "--listen-address", "unix://kine.sock")
	cmd.Dir = k3sDataDir
	cmd.Stdout = writer
	cmd.Stderr = writer
	err = cmd.Run()
	_ = writer.Close()
	<-done
	if err != nil && err.Error() != "signal: killed" {
		return err
	}

	return nil
}
