package checksum

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"hash/crc32"
	"testing"

	"github.com/minio/crc64nvme"
)

func TestSinglePartAllAlgos(t *testing.T) {
	data := []byte("hello world")
	w := NewWriter([]Algorithm{AlgMD5, AlgSHA1, AlgSHA256, AlgCRC32, AlgCRC32C, AlgCRC64NVME}, nil)
	w.Write(data)
	sums := w.Sums()

	cases := []struct {
		algo Algorithm
		want []byte
	}{
		{AlgMD5, md5sum(data)},
		{AlgSHA1, sha1sum(data)},
		{AlgSHA256, sha256sum(data)},
		{AlgCRC32, crc32sum(data, crc32.IEEETable)},
		{AlgCRC32C, crc32sum(data, crc32.MakeTable(crc32.Castagnoli))},
		{AlgCRC64NVME, crc64nvmesum(data)},
	}
	for _, c := range cases {
		got := sums[c.algo].FullObject
		if !bytes.Equal(got, c.want) {
			t.Errorf("%s: got %x, want %x", c.algo, got, c.want)
		}
		if sums[c.algo].Composite != nil {
			t.Errorf("%s: composite should be nil for single-part", c.algo)
		}
	}
}

func TestMultipartCompositeMatchesS3Format(t *testing.T) {
	// Three parts of 5, 5, 3 bytes.
	parts := [][]byte{
		[]byte("aaaaa"),
		[]byte("bbbbb"),
		[]byte("ccc"),
	}
	sizes := []int64{5, 5, 3}

	full := bytes.Join(parts, nil)
	w := NewWriter([]Algorithm{AlgMD5, AlgSHA256, AlgCRC64NVME}, sizes)
	// Write across part boundaries to verify splitting.
	w.Write(full[:3])
	w.Write(full[3:8])
	w.Write(full[8:])
	sums := w.Sums()

	// Each algorithm's Composite = hash(concat(per-part hashes))
	for _, c := range []struct {
		algo  Algorithm
		hash  func([]byte) []byte
	}{
		{AlgMD5, md5sum},
		{AlgSHA256, sha256sum},
		{AlgCRC64NVME, crc64nvmesum},
	} {
		var concat []byte
		for _, p := range parts {
			concat = append(concat, c.hash(p)...)
		}
		want := c.hash(concat)
		got := sums[c.algo].Composite
		if !bytes.Equal(got, want) {
			t.Errorf("%s composite: got %x, want %x", c.algo, got, want)
		}
		if sums[c.algo].NumParts != len(parts) {
			t.Errorf("%s NumParts: got %d, want %d", c.algo, sums[c.algo].NumParts, len(parts))
		}
		// Full-object output should still be the hash over all bytes,
		// independent of part boundaries.
		fullWant := c.hash(full)
		if !bytes.Equal(sums[c.algo].FullObject, fullWant) {
			t.Errorf("%s full-object: got %x, want %x", c.algo, sums[c.algo].FullObject, fullWant)
		}
	}
}

func md5sum(b []byte) []byte    { h := md5.Sum(b); return h[:] }
func sha1sum(b []byte) []byte   { h := sha1.Sum(b); return h[:] }
func sha256sum(b []byte) []byte { h := sha256.Sum256(b); return h[:] }
func crc32sum(b []byte, t *crc32.Table) []byte {
	h := crc32.New(t)
	h.Write(b)
	return h.Sum(nil)
}
func crc64nvmesum(b []byte) []byte {
	h := crc64nvme.New()
	h.Write(b)
	return h.Sum(nil)
}
