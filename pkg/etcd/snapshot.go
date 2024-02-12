package etcd

import (
	"archive/tar"
	"context"
	"fmt"
	"os"
	"path"
	"regexp"

	"github.com/loft-sh/vcluster/pkg/constants"
	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/util/compress"
	"github.com/loft-sh/vcluster/pkg/util/random"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

const (
	registryPrefix = "/registry/"
	dataDir        = "/data"
)

var (
	etcdBackup    = path.Join(dataDir, "snapshot.")
	kubeRootCaCrt = regexp.MustCompile(`/registry/configmaps/[^/]+/kube-root-ca.crt`)

	excludeAnnotation = "vcluster.loft.sh/exclude"
)

func Snapshot(ctx context.Context) (string, error) {
	distro := constants.GetVClusterDistro()

	// currently we only support k3s
	if distro != constants.K3SDistro {
		// TODO: support k8s & eks
		return "", fmt.Errorf("unsupported vCluster distro")
	}

	filePath := etcdBackup + random.String(6)
	f, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	etcdClient, err := GetEtcdClient(ctx, nil, "unix://"+k3sKineSock)
	if err != nil {
		return "", err
	}

	result, err := etcdClient.Get(ctx, registryPrefix, clientv3.WithPrefix())
	if err != nil {
		return "", err
	}

	tarWriter := tar.NewWriter(f)
	defer tarWriter.Close()

	codecFactory := serializer.NewCodecFactory(scheme.Scheme)
	for _, r := range result.Kvs {
		if len(r.Key) == 0 || len(r.Value) == 0 {
			continue
		}

		// filter out kube-root-ca.crt configmaps to avoid conflicts when we
		// exchange the certificates later.
		if kubeRootCaCrt.Match(r.Key) {
			continue
		}

		// write to tar archive
		err = snapshotKeyValue(tarWriter, r.Key, r.Value, codecFactory.UniversalDeserializer())
		if err != nil {
			return "", fmt.Errorf("snapshot key %s: %w", string(r.Key), err)
		}
	}

	return filePath, nil
}

func snapshotKeyValue(tarWriter *tar.Writer, key, value []byte, decoder runtime.Decoder) error {
	// try to decode object
	obj := &unstructured.Unstructured{}
	_, _, err := decoder.Decode(value, nil, obj)
	if err != nil {
		return fmt.Errorf("decode object: %w", err)
	}

	// check if we should exclude it
	annotations := obj.GetAnnotations()
	if annotations != nil && annotations[excludeAnnotation] == "true" {
		return nil
	}

	// write key value
	err = compress.WriteKeyValue(tarWriter, key, value)
	if err != nil {
		return err
	}

	return nil
}
