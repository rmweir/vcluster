package compress

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
)

// Compress gzips a string and base64 encodes it
func Compress(s string) (string, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)

	_, err := gz.Write([]byte(s))
	if err != nil {
		return "", err
	}

	err = gz.Flush()
	if err != nil {
		return "", err
	}

	err = gz.Close()
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b.Bytes()), nil
}

// Uncompress uncompresses a string
func Uncompress(s string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}

	rdata := bytes.NewReader(decoded)
	r, err := gzip.NewReader(rdata)
	if err != nil {
		return "", err
	}

	decompressed, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}

	return string(decompressed), nil
}

func UncompressBytes(raw []byte) (string, error) {
	rdata := bytes.NewReader(raw)
	r, err := gzip.NewReader(rdata)
	if err != nil {
		return "", err
	}

	decompressed, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}

	return string(decompressed), nil
}

func ReadKeyValue(tarReader *tar.Reader) ([]byte, []byte, error) {
	header, err := tarReader.Next()
	if err != nil {
		return nil, nil, err
	}

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, tarReader)
	if err != nil {
		return nil, nil, err
	}

	return []byte(header.Name), buf.Bytes(), nil
}

func WriteKeyValue(tarWriter *tar.Writer, key, value []byte) error {
	err := tarWriter.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     string(key),
		Size:     int64(len(value)),
		Mode:     0666,
	})
	if err != nil {
		return err
	}

	// write value to tar archive
	_, err = tarWriter.Write(value)
	if err != nil {
		return err
	}

	return nil
}
