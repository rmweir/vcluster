package etcd

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/loft-sh/vcluster/pkg/constants"
	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/util/compress"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	"k8s.io/klog/v2"
)

const (
	originalPrefix = "/original/"
)

func Restore(ctx context.Context, data io.ReadCloser) error {
	if data == nil {
		return nil
	}
	defer data.Close()

	// currently we only support k3s
	distro := constants.GetVClusterDistro()
	if distro != constants.K3SDistro {
		return fmt.Errorf("unsupported vCluster distro")
	}

	// run kine in background
	storage := newKineStorage("unix://" + k3sKineSock)

	// start kine storage
	err := storage.Start(ctx)
	if err != nil {
		return fmt.Errorf("start kine: %w", err)
	}
	defer func() {
		err := storage.Close()
		if err != nil {
			klog.Infof("Error closing storage: %v", err)
		}
	}()

	// init serializers
	jsonSerializer := json.NewSerializerWithOptions(
		json.DefaultMetaFactory, scheme.Scheme, scheme.Scheme,
		json.SerializerOptions{Yaml: false, Pretty: false, Strict: false},
	)

	// insert & update new keys
	klog.Info("Restoring etcd state")
	tarReader := tar.NewReader(data)
	touchedKeys := map[string]bool{}
	for {
		// read from archive
		key, value, err := compress.ReadKeyValue(tarReader)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read etcd key/value: %w", err)
		} else if errors.Is(err, io.EOF) || len(key) == 0 {
			break
		}

		// write the value to etcd
		originalKey, err := restoreKeyValue(ctx, storage, jsonSerializer, string(key), value)
		if err != nil {
			return fmt.Errorf("write key %s: %w", string(key), err)
		} else if originalKey != "" {
			touchedKeys[originalKey] = true
		}
	}

	// delete old keys
	keyValues, err := storage.List(ctx, originalPrefix)
	if err != nil {
		return err
	}
	for _, keyValue := range keyValues {
		if touchedKeys[string(keyValue.Key)] {
			continue
		}

		// delete the key if it shouldn't be there anymore
		err = storage.Delete(ctx, string(keyValue.Key))
		if err != nil {
			return fmt.Errorf("delete key %s: %w", string(keyValue.Key), err)
		}
	}

	klog.Info("Successfully restored etcd state")
	return nil
}

func restoreKeyValue(
	ctx context.Context,
	client Storage,
	jsonEncoder runtime.Encoder,
	key string,
	value []byte,
) (string, error) {
	// for non registry keys just write them and return
	if !strings.HasPrefix(key, registryPrefix) {
		return "", client.Put(ctx, key, value)
	}

	// try to merge registry keys
	originalKey := originalPrefix + strings.TrimPrefix(registryPrefix, key)

	// check if there is a original value
	originalValue, err := client.Get(ctx, originalKey)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return "", fmt.Errorf("retrieve original value: %w", err)
	}

	// get current value
	currentValue, err := client.Get(ctx, key)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return "", fmt.Errorf("retrieve current value: %w", err)
	}

	// now check if we should do a merge or put
	if originalValue.Modified != 0 && currentValue.Modified != 0 {
		// do a three-way-merge on original, current & modified
		value, err = applyThreeWayMerge(originalValue.Data, value, currentValue.Data, client, jsonEncoder)
		if err != nil {
			return "", fmt.Errorf("apply three way merge: %w", err)
		} else if value == nil {
			// seems like we don't need to update
			return originalKey, nil
		}
	}

	// write to /registry/...
	err = client.Put(ctx, key, value)
	if err != nil {
		return "", err
	}

	// write original to /original/...
	err = client.Put(ctx, originalKey, value)
	if err != nil {
		return "", err
	}

	return originalKey, nil
}

func applyThreeWayMerge(originalValue, modifiedValue, currentValue []byte, storage Storage, jsonSerializer runtime.Encoder) ([]byte, error) {
	// check if there is an update
	if string(originalValue) == string(modifiedValue) {
		return nil, nil
	}

	// convert original
	originalJson, err := convert(originalValue, storage, jsonSerializer)
	if err != nil {
		return nil, fmt.Errorf("convert original object: %w", err)
	}

	// convert modified
	modifiedJson, err := convert(modifiedValue, storage, jsonSerializer)
	if err != nil {
		return nil, fmt.Errorf("convert modified object: %w", err)
	}

	// convert current
	currentJson, err := convert(currentValue, storage, jsonSerializer)
	if err != nil {
		return nil, fmt.Errorf("convert current object: %w", err)
	}

	// create three-way json merge patch
	patch, err := jsonmergepatch.CreateThreeWayJSONMergePatch(originalJson, modifiedJson, currentJson)
	if err != nil {
		return nil, fmt.Errorf("create three-way json merge patch: %w", err)
	} else if string(patch) == "{}" {
		return nil, nil
	}

	// apply patch to current
	newValue, err := jsonpatch.MergePatch(currentJson, patch)
	if err != nil {
		return nil, fmt.Errorf("apply three-way json merge patch: %w", err)
	}

	// encode to protobuf
	return convert(newValue, storage, storage)
}

func convert(object []byte, decoder runtime.Decoder, encoder runtime.Encoder) ([]byte, error) {
	// first read all values and transform them to json
	objDecoded := &unstructured.Unstructured{}
	_, _, err := decoder.Decode(object, nil, objDecoded)
	if err != nil {
		return nil, fmt.Errorf("decode object: %w", err)
	}

	objEncodedBuffer := &bytes.Buffer{}
	err = encoder.Encode(objDecoded, objEncodedBuffer)
	if err != nil {
		return nil, fmt.Errorf("encode object: %w", err)
	}

	return objEncodedBuffer.Bytes(), nil
}
