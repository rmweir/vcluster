package oci

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/ghodss/yaml"
	"github.com/loft-sh/vcluster/pkg/helm"
	"github.com/loft-sh/vcluster/pkg/util/compress"
)

var (
	dataDir = "/data"

	metadataBackup = path.Join(dataDir, "metadata.json")
	chartArchive   = path.Join(dataDir, "chart.tgz")
)

func buildChart(release *helm.Release) (string, string, error) {
	// marshal metadata
	metadataRaw, err := json.Marshal(release.Chart.Metadata)
	if err != nil {
		return "", "", fmt.Errorf("marshal release metadata: %w", err)
	}

	// write metadata to file
	err = os.WriteFile(metadataBackup, metadataRaw, 0666)
	if err != nil {
		return "", "", fmt.Errorf("write release metadata: %w", err)
	}

	// build tar archive
	f, err := os.Create(chartArchive)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	gzipWriter := gzip.NewWriter(f)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	// marshal values.yaml
	valuesRaw, err := yaml.Marshal(mergeMaps(release.Chart.Values, release.Config))
	if err != nil {
		return "", "", fmt.Errorf("marshal values.yaml: %w", err)
	}

	// add values.yaml
	err = compress.WriteKeyValue(tarWriter, []byte("/values.yaml"), valuesRaw)
	if err != nil {
		return "", "", fmt.Errorf("add values.yaml: %w", err)
	}

	// add Chart.yaml
	err = compress.WriteKeyValue(tarWriter, []byte("/Chart.yaml"), metadataRaw)
	if err != nil {
		return "", "", fmt.Errorf("add Chart.yaml: %w", err)
	}

	// add templates
	for _, template := range release.Chart.Templates {
		err = compress.WriteKeyValue(tarWriter, []byte("/"+template.Name), template.Data)
		if err != nil {
			return "", "", fmt.Errorf("add template %s to archive: %w", template.Name, err)
		}
	}

	// add other files
	for _, file := range release.Chart.Files {
		err = compress.WriteKeyValue(tarWriter, []byte("/"+file.Name), file.Data)
		if err != nil {
			return "", "", fmt.Errorf("add file %s to archive: %w", file.Name, err)
		}
	}

	return metadataBackup, chartArchive, nil
}

func mergeMaps(a, b map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(a))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if v, ok := v.(map[string]interface{}); ok {
			if bv, ok := out[k]; ok {
				if bv, ok := bv.(map[string]interface{}); ok {
					out[k] = mergeMaps(bv, v)
					continue
				}
			}
		}
		out[k] = v
	}
	return out
}
