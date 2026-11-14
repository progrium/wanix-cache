package nix

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// NarInfo is a binary cache's description of one store path.
type NarInfo struct {
	StorePath   StorePath
	URL         string
	Compression string // "none", "xz", "zstd", "bzip2", "gzip"; empty means bzip2
	FileHash    string
	FileSize    int64
	NarHash     string // normalized to "sha256:<base32>"
	NarSize     int64
	References  []StorePath // may include StorePath itself
	Deriver     string
	System      string
	Sigs        []string
	CA          string
}

// ParseNarInfo parses the text of a .narinfo file.
func ParseNarInfo(text string) (*NarInfo, error) {
	ni := &NarInfo{}
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, ": ")
		if !ok {
			// "References:" with no value has no space after the colon.
			if k, ok = strings.CutSuffix(line, ":"); !ok {
				return nil, fmt.Errorf("nix: narinfo: bad line %q", line)
			}
		}
		var err error
		switch k {
		case "StorePath":
			ni.StorePath, err = ParseStorePath(v)
		case "URL":
			ni.URL = v
		case "Compression":
			ni.Compression = v
		case "FileHash":
			ni.FileHash = v
		case "FileSize":
			ni.FileSize, err = strconv.ParseInt(v, 10, 64)
		case "NarHash":
			ni.NarHash, err = NormalizeHash(v)
		case "NarSize":
			ni.NarSize, err = strconv.ParseInt(v, 10, 64)
		case "References":
			for _, r := range strings.Fields(v) {
				p, err := ParseStorePath(r)
				if err != nil {
					return nil, err
				}
				ni.References = append(ni.References, p)
			}
		case "Deriver":
			if v != "unknown-deriver" {
				ni.Deriver = v
			}
		case "System":
			ni.System = v
		case "Sig":
			ni.Sigs = append(ni.Sigs, v)
		case "CA":
			ni.CA = v
		}
		if err != nil {
			return nil, fmt.Errorf("nix: narinfo %s: %w", k, err)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if ni.StorePath == "" || ni.URL == "" || ni.NarHash == "" || ni.NarSize <= 0 {
		return nil, fmt.Errorf("nix: narinfo missing StorePath, URL, NarHash or NarSize")
	}
	if ni.Compression == "" {
		ni.Compression = "bzip2"
	}
	return ni, nil
}

// String formats the narinfo in the canonical field order.
func (ni *NarInfo) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "StorePath: %s\n", ni.StorePath.Abs())
	fmt.Fprintf(&b, "URL: %s\n", ni.URL)
	fmt.Fprintf(&b, "Compression: %s\n", ni.Compression)
	if ni.FileHash != "" {
		fmt.Fprintf(&b, "FileHash: %s\n", ni.FileHash)
	}
	if ni.FileSize > 0 {
		fmt.Fprintf(&b, "FileSize: %d\n", ni.FileSize)
	}
	fmt.Fprintf(&b, "NarHash: %s\n", ni.NarHash)
	fmt.Fprintf(&b, "NarSize: %d\n", ni.NarSize)
	refs := make([]string, len(ni.References))
	for i, r := range ni.References {
		refs[i] = string(r)
	}
	fmt.Fprintf(&b, "References: %s\n", strings.Join(refs, " "))
	if ni.Deriver != "" {
		fmt.Fprintf(&b, "Deriver: %s\n", ni.Deriver)
	}
	if ni.System != "" {
		fmt.Fprintf(&b, "System: %s\n", ni.System)
	}
	for _, s := range ni.Sigs {
		fmt.Fprintf(&b, "Sig: %s\n", s)
	}
	if ni.CA != "" {
		fmt.Fprintf(&b, "CA: %s\n", ni.CA)
	}
	return b.String()
}

// Uncompressed returns a copy that describes the NAR served uncompressed
// at url. Signatures stay valid: they sign the store path, NarHash, NarSize
// and references, not how the NAR is shipped.
func (ni *NarInfo) Uncompressed(url string) *NarInfo {
	c := *ni
	c.URL = url
	c.Compression = "none"
	c.FileHash = ni.NarHash
	c.FileSize = ni.NarSize
	return &c
}

// Deps returns References without the path itself.
func (ni *NarInfo) Deps() []StorePath {
	var out []StorePath
	for _, r := range ni.References {
		if r != ni.StorePath {
			out = append(out, r)
		}
	}
	return out
}
