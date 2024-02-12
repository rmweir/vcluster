package etcd

import (
	"context"

	"github.com/loft-sh/vcluster/pkg/scheme"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
)

func newEtcdStorage(certificates *Certificates, endpoints ...string) Storage {
	codecFactory := serializer.NewCodecFactory(scheme.Scheme)
	deserializer := codecFactory.UniversalDeserializer()
	protoSerializer := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)

	return &etcdStorage{
		Encoder: protoSerializer,
		Decoder: deserializer,

		certificates: certificates,
		endpoints:    endpoints,
	}
}

type etcdStorage struct {
	runtime.Encoder
	runtime.Decoder

	certificates *Certificates
	endpoints    []string

	etcdClient *clientv3.Client
}

func (r *etcdStorage) Start(ctx context.Context) error {
	var err error
	r.etcdClient, err = WaitForEtcdClient(ctx, r.certificates, r.endpoints...)
	if err != nil {
		return err
	}

	return nil
}

func (r *etcdStorage) Close() error {
	return r.Close()
}

func (r *etcdStorage) List(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	getResponse, err := r.etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	return getResponse.Kvs, nil
}

func (r *etcdStorage) Get(ctx context.Context, key string) (Value, error) {
	getResponse, err := r.etcdClient.Get(ctx, key)
	if err != nil {
		return Value{}, err
	} else if getResponse.Count != 1 {
		return Value{}, ErrKeyNotFound
	}

	return Value{
		Key:      getResponse.Kvs[0].Key,
		Data:     getResponse.Kvs[0].Value,
		Modified: getResponse.Kvs[0].ModRevision,
	}, nil
}

func (r *etcdStorage) Put(ctx context.Context, key string, value []byte) error {
	_, err := r.etcdClient.Put(ctx, key, string(value))
	if err != nil {
		return err
	}

	return nil
}

func (r *etcdStorage) Delete(ctx context.Context, key string) error {
	_, err := r.etcdClient.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}
