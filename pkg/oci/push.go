package oci

import (
	"context"
	"fmt"
	"os"

	"github.com/loft-sh/vcluster/pkg/etcd"
	"github.com/loft-sh/vcluster/pkg/registry"
	"k8s.io/client-go/kubernetes"

	"github.com/google/go-containerregistry/pkg/name"
)

func Push(
	ctx context.Context,
	hostClient kubernetes.Interface,
	vClusterNamespace string,
	target string,
	username, password string,
) error {
	// get release info
	release, err := registry.GetReleaseInfo(ctx, hostClient, vClusterNamespace)
	if err != nil {
		return fmt.Errorf("retrieve vCluster chart info: %w", err)
	} else if release == nil || release.Chart == nil {
		return fmt.Errorf("vCluster was not deployed via helm")
	}

	// get chart info
	metadataLayer, chartLayer, err := buildChart(release)
	if err != nil {
		return fmt.Errorf("build helm chart: %w", err)
	}

	// get etcd snapshot
	etcdSnapshot, err := etcd.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("retrieve etcd snapshot: %w", err)
	}
	defer os.Remove(etcdSnapshot)

	// TODO: backup pvcs

	// parse target
	registryName, repository, tag, err := ParseReference(target)
	if err != nil {
		return err
	}

	// push to registry
	err = registry.Push(
		ctx,
		metadataLayer,
		chartLayer,
		&registry.VClusterConfig{
			ChartInfo: &registry.ChartInfo{
				Name:    release.Chart.Metadata.Name,
				Version: release.Chart.Metadata.Version,
				Values:  release.Config,
			},
		},
		etcdSnapshot,
		registryName,
		repository,
		tag,
		username,
		password,
	)
	if err != nil {
		return fmt.Errorf("push %s: %w", target, err)
	}

	return nil
}

func ParseReference(target string) (string, string, string, error) {
	ref, err := name.ParseReference(target)
	if err != nil {
		return "", "", "", err
	}

	tag := "latest"
	repository := ""
	registryName := ""
	if tagRef, ok := ref.(name.Tag); ok {
		tag = tagRef.TagStr()
		repository = tagRef.RepositoryStr()
		registryName = tagRef.RegistryStr()
	} else if digestRef, ok := ref.(name.Digest); ok {
		repository = digestRef.RepositoryStr()
		registryName = digestRef.RegistryStr()
	} else {
		return "", "", "", fmt.Errorf("unrecognized image %s", target)
	}

	return registryName, repository, tag, nil
}
