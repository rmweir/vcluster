package registry

const (
	// ArtifactType is the vCluster artifact type
	ArtifactType = "application/vnd.loft.vcluster"

	// ConfigMediaType is the reserved media type for the vCluster config
	ConfigMediaType = "application/vnd.loft.vcluster.config.v1+json"

	// EtcdLayerMediaType is the reserved media type for the etcd snapshot
	EtcdLayerMediaType = "application/vnd.loft.vcluster.etcd.v1.tar+gzip"

	// PersistentVolumeLayerMediaType is the reserved media type for persistent volumes
	PersistentVolumeLayerMediaType = "application/vnd.loft.vcluster.pv.v1.tar+gzip"
)

const (
	// HelmChartLayerMediaType is the reserved media type for Helm chart package content
	HelmChartLayerMediaType = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"

	// HelmConfigMediaType is the reserved media type for the Helm chart manifest config
	HelmConfigMediaType = "application/vnd.cncf.helm.config.v1+json"
)
