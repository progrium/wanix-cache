package oci

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Media types.
const (
	MediaOCIIndex      = "application/vnd.oci.image.index.v1+json"
	MediaOCIManifest   = "application/vnd.oci.image.manifest.v1+json"
	MediaDockerList    = "application/vnd.docker.distribution.manifest.list.v2+json"
	MediaDockerV2      = "application/vnd.docker.distribution.manifest.v2+json"
	MediaOCILayer      = "application/vnd.oci.image.layer.v1.tar"
	MediaOCILayerGzip  = "application/vnd.oci.image.layer.v1.tar+gzip"
	MediaOCILayerZstd  = "application/vnd.oci.image.layer.v1.tar+zstd"
	MediaDockerLayer   = "application/vnd.docker.image.rootfs.diff.tar.gzip"
	MediaDockerForeign = "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip"
	MediaOCINondistGz  = "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip"
	MediaOCINondist    = "application/vnd.oci.image.layer.nondistributable.v1.tar"
)

var manifestAccept = []string{MediaOCIIndex, MediaOCIManifest, MediaDockerList, MediaDockerV2}

const (
	maxManifest = 4 << 20
	maxConfig   = 8 << 20
)

// Descriptor points at a blob.
type Descriptor struct {
	MediaType string    `json:"mediaType"`
	Digest    string    `json:"digest"`
	Size      int64     `json:"size"`
	Platform  *Platform `json:"platform,omitempty"`
}

// Platform is an index entry's target.
type Platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

func (p Platform) String() string {
	s := p.OS + "/" + p.Architecture
	if p.Variant != "" {
		s += "/" + p.Variant
	}
	return s
}

// ParsePlatform parses os/arch[/variant].
func ParsePlatform(s string) (Platform, error) {
	parts := strings.Split(s, "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" {
		return Platform{}, fmt.Errorf("oci: bad platform %q, want os/arch[/variant]", s)
	}
	p := Platform{OS: parts[0], Architecture: parts[1]}
	if len(parts) == 3 {
		p.Variant = parts[2]
	}
	return p, nil
}

// manifest covers both an index and an image manifest; MediaType (or the
// Content-Type) says which fields apply.
type manifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Manifests     []Descriptor `json:"manifests"`
	Config        Descriptor   `json:"config"`
	Layers        []Descriptor `json:"layers"`
}

// imageConfig is the part of an image config the cache needs.
type imageConfig struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	RootFS       struct {
		Type    string   `json:"type"`
		DiffIDs []string `json:"diff_ids"`
	} `json:"rootfs"`
}

func sha256Digest(b []byte) string {
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

func parseManifest(body []byte, contentType string) (manifest, string, error) {
	var m manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return m, "", fmt.Errorf("oci: manifest: %w", err)
	}
	mt := m.MediaType
	if mt == "" {
		mt = contentType
	}
	if mt == "" || mt == "application/json" {
		// OCI allows omitting mediaType; infer from the fields present.
		if len(m.Manifests) > 0 {
			mt = MediaOCIIndex
		} else if m.Config.Digest != "" {
			mt = MediaOCIManifest
		}
	}
	if m.SchemaVersion != 2 {
		return m, "", fmt.Errorf("oci: unsupported manifest schemaVersion %d", m.SchemaVersion)
	}
	return m, mt, nil
}

func isIndex(mt string) bool { return mt == MediaOCIIndex || mt == MediaDockerList }
func isImage(mt string) bool { return mt == MediaOCIManifest || mt == MediaDockerV2 }

// pick chooses the index entry for platform p. A missing variant matches
// any; an exact variant match wins.
func pick(ms []Descriptor, p Platform) (Descriptor, error) {
	var best *Descriptor
	for i := range ms {
		d := &ms[i]
		if d.Platform == nil || d.Platform.OS != p.OS || d.Platform.Architecture != p.Architecture {
			continue
		}
		if !isImage(d.MediaType) && d.MediaType != "" {
			continue
		}
		if p.Variant != "" && d.Platform.Variant != "" && d.Platform.Variant != p.Variant {
			continue
		}
		if best == nil || (p.Variant != "" && d.Platform.Variant == p.Variant) {
			best = d
		}
	}
	if best == nil {
		var have []string
		for _, d := range ms {
			if d.Platform != nil {
				have = append(have, d.Platform.String())
			}
		}
		return Descriptor{}, fmt.Errorf("oci: no image for %s in index (has %s)", p, strings.Join(have, ", "))
	}
	return *best, nil
}

// compression of a layer media type: "", "gzip" or "zstd".
func compression(mediaType string) (string, error) {
	switch mediaType {
	case MediaOCILayer, MediaOCINondist:
		return "", nil
	case MediaOCILayerGzip, MediaDockerLayer, MediaDockerForeign, MediaOCINondistGz:
		return "gzip", nil
	case MediaOCILayerZstd:
		return "zstd", nil
	}
	return "", fmt.Errorf("oci: unsupported layer media type %q", mediaType)
}
