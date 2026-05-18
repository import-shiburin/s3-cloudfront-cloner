// Package checksum computes one or more digest algorithms from a byte stream
// in a single pass and emits results in both "FullObject" form (digest over
// the whole stream) and, when PartSizes is provided, "Composite" form
// (digest over the concatenation of per-part digests, S3 multipart-style).
//
// The raw digest bytes are returned; callers encode them as hex (ETag) or
// base64 (S3 x-amz-checksum-* headers and rclone's x-amz-meta-md5chksum).
package checksum

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"hash/crc32"

	"github.com/minio/crc64nvme"
)

// Algorithm identifies a digest algorithm.
type Algorithm string

const (
	AlgMD5       Algorithm = "MD5"
	AlgSHA1      Algorithm = "SHA1"
	AlgSHA256    Algorithm = "SHA256"
	AlgCRC32     Algorithm = "CRC32"
	AlgCRC32C    Algorithm = "CRC32C"
	AlgCRC64NVME Algorithm = "CRC64NVME"
)

// newHasher returns a fresh hash.Hash for the given algorithm.
func newHasher(a Algorithm) hash.Hash {
	switch a {
	case AlgMD5:
		return md5.New()
	case AlgSHA1:
		return sha1.New()
	case AlgSHA256:
		return sha256.New()
	case AlgCRC32:
		return crc32.NewIEEE()
	case AlgCRC32C:
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case AlgCRC64NVME:
		return crc64nvme.New()
	default:
		panic(fmt.Sprintf("unknown algorithm: %s", a))
	}
}

// Result holds digests for one algorithm.
type Result struct {
	// FullObject is the raw digest over the entire byte stream.
	FullObject []byte
	// Composite is the raw digest over the concatenation of per-part digests.
	// Empty when no PartSizes were supplied.
	Composite []byte
	// NumParts is the number of parts when Composite is populated; 0 otherwise.
	NumParts int
}

// Writer computes the requested digests as bytes are written. When PartSizes
// is non-empty, per-part digests are also accumulated so a composite (multipart-
// style) result can be returned alongside the full-object result.
type Writer struct {
	algos     []Algorithm
	partSizes []int64

	fullHashers map[Algorithm]hash.Hash
	partHashers map[Algorithm]hash.Hash    // current-part hashers; nil when partSizes is empty
	partDigests map[Algorithm][][]byte     // completed per-part digests

	curPartIdx     int
	curPartWritten int64
}

// NewWriter returns a Writer that computes the requested algorithms. Pass
// nil/empty partSizes for single-part sources (no composite output).
func NewWriter(algos []Algorithm, partSizes []int64) *Writer {
	w := &Writer{
		algos:       append([]Algorithm(nil), algos...),
		partSizes:   partSizes,
		fullHashers: make(map[Algorithm]hash.Hash, len(algos)),
	}
	for _, a := range algos {
		w.fullHashers[a] = newHasher(a)
	}
	if len(partSizes) > 0 {
		w.partHashers = make(map[Algorithm]hash.Hash, len(algos))
		w.partDigests = make(map[Algorithm][][]byte, len(algos))
		for _, a := range algos {
			w.partHashers[a] = newHasher(a)
		}
	}
	return w
}

// Write fans bytes out to every full-object hasher and (when configured) every
// per-part hasher, splitting writes at source-defined part boundaries.
func (w *Writer) Write(p []byte) (int, error) {
	n := len(p)
	for _, h := range w.fullHashers {
		h.Write(p)
	}

	if w.partHashers == nil {
		return n, nil
	}

	for len(p) > 0 {
		if w.curPartIdx >= len(w.partSizes) {
			// Overshoot: declared parts are exhausted. Keep accumulating into the
			// current (now-trailing) hashers — verification will fail anyway,
			// but at least Sums() produces deterministic output.
			for _, h := range w.partHashers {
				h.Write(p)
			}
			w.curPartWritten += int64(len(p))
			return n, nil
		}
		remaining := w.partSizes[w.curPartIdx] - w.curPartWritten
		if int64(len(p)) <= remaining {
			for _, h := range w.partHashers {
				h.Write(p)
			}
			w.curPartWritten += int64(len(p))
			if w.curPartWritten == w.partSizes[w.curPartIdx] {
				w.flushPart()
			}
			return n, nil
		}
		for _, h := range w.partHashers {
			h.Write(p[:remaining])
		}
		w.curPartWritten += remaining
		w.flushPart()
		p = p[remaining:]
	}
	return n, nil
}

func (w *Writer) flushPart() {
	for _, a := range w.algos {
		w.partDigests[a] = append(w.partDigests[a], w.partHashers[a].Sum(nil))
		w.partHashers[a] = newHasher(a)
	}
	w.curPartWritten = 0
	w.curPartIdx++
}

// Sums returns one Result per requested algorithm. Safe to call once after all
// writes are complete.
func (w *Writer) Sums() map[Algorithm]Result {
	out := make(map[Algorithm]Result, len(w.algos))
	for _, a := range w.algos {
		out[a] = Result{FullObject: w.fullHashers[a].Sum(nil)}
	}
	if w.partHashers == nil {
		return out
	}

	// Flush any in-progress part (short final write or overshoot).
	if w.curPartWritten > 0 {
		w.flushPart()
	}

	for _, a := range w.algos {
		digests := w.partDigests[a]
		if len(digests) == 0 {
			continue
		}
		h := newHasher(a)
		for _, d := range digests {
			h.Write(d)
		}
		r := out[a]
		r.Composite = h.Sum(nil)
		r.NumParts = len(digests)
		out[a] = r
	}
	return out
}
