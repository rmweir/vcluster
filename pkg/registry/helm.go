package registry

import (
	"context"
	"fmt"

	"github.com/loft-sh/vcluster/pkg/helm"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
)

func GetReleaseInfo(ctx context.Context, hostClient kubernetes.Interface, vClusterNamespace string) (*helm.Release, error) {
	if hostClient == nil {
		return nil, fmt.Errorf("host client is empty")
	}

	release, err := helm.NewSecrets(hostClient).Get(ctx, translate.VClusterName, vClusterNamespace)
	if err != nil {
		return nil, err
	} else if release == nil {
		return nil, nil
	} else if kerrors.IsNotFound(err) {
		return nil, nil
	}

	if release.Config == nil {
		release.Config = map[string]interface{}{}
	}

	return release, nil
}
