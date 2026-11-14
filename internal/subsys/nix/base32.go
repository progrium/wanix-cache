package nix

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

// Nix's base32 alphabet omits e, o, u and t.
const base32Chars = "0123456789abcdfghijklmnpqrsvwxyz"

// EncodeBase32 encodes b the way Nix does: least significant 5-bit group
// last, no padding.
func EncodeBase32(b []byte) string {
	n := (len(b)*8 + 4) / 5
	out := make([]byte, n)
	for i := n - 1; i >= 0; i-- {
		bit := i * 5
		j, k := bit/8, uint(bit%8)
		c := uint(b[j]) >> k
		if j+1 < len(b) {
			c |= uint(b[j+1]) << (8 - k)
		}
		out[n-1-i] = base32Chars[c&0x1f]
	}
	return string(out)
}

// DecodeBase32 decodes Nix base32 into size bytes.
func DecodeBase32(s string, size int) ([]byte, error) {
	if len(s) != (size*8+4)/5 {
		return nil, fmt.Errorf("nix: base32 %q: wrong length for %d bytes", s, size)
	}
	out := make([]byte, size)
	for i := 0; i < len(s); i++ {
		d := strings.IndexByte(base32Chars, s[len(s)-1-i])
		if d < 0 {
			return nil, fmt.Errorf("nix: base32 %q: bad character %q", s, s[len(s)-1-i])
		}
		bit := i * 5
		j, k := bit/8, uint(bit%8)
		out[j] |= byte(d << k)
		if j+1 < size {
			out[j+1] |= byte(d >> (8 - k))
		} else if d>>(8-k) != 0 {
			return nil, fmt.Errorf("nix: base32 %q: excess bits", s)
		}
	}
	return out, nil
}

// ParseHash parses a sha256 hash in any form Nix writes it: "sha256:" with
// base32, hex or base64, or SRI "sha256-<base64>".
func ParseHash(s string) ([]byte, error) {
	var body string
	switch {
	case strings.HasPrefix(s, "sha256:"):
		body = s[len("sha256:"):]
	case strings.HasPrefix(s, "sha256-"):
		b, err := base64.StdEncoding.DecodeString(s[len("sha256-"):])
		if err != nil || len(b) != sha256.Size {
			return nil, fmt.Errorf("nix: bad SRI hash %q", s)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("nix: unsupported hash %q", s)
	}
	switch len(body) {
	case 52:
		return DecodeBase32(body, sha256.Size)
	case 64:
		return hex.DecodeString(body)
	case 44:
		return base64.StdEncoding.DecodeString(body)
	}
	return nil, fmt.Errorf("nix: bad sha256 hash %q", s)
}

// FormatHash writes a sha256 digest as "sha256:<base32>", the form narinfo
// files use and the form this package uses for unit digests.
func FormatHash(sum []byte) string { return "sha256:" + EncodeBase32(sum) }

// NormalizeHash rewrites any accepted sha256 form as FormatHash does.
func NormalizeHash(s string) (string, error) {
	b, err := ParseHash(s)
	if err != nil {
		return "", err
	}
	return FormatHash(b), nil
}
