package nix

import (
	"fmt"
	"strings"
)

// StoreDir is the only store directory this cache serves.
const StoreDir = "/nix/store"

// HashLen is the length of a store path's hash part.
const HashLen = 32

// StorePath is a store path's base name, "<hash>-<name>". It is also the
// unit key: the hash names the exact output, and keeping the name lets the
// core place the unit at /nix/store/<key> without a lookup.
type StorePath string

// ParseStorePath accepts "<hash>-<name>" or "/nix/store/<hash>-<name>".
func ParseStorePath(s string) (StorePath, error) {
	s = strings.TrimPrefix(s, StoreDir+"/")
	if len(s) < HashLen+2 || s[HashLen] != '-' {
		return "", fmt.Errorf("nix: bad store path %q", s)
	}
	for i := 0; i < HashLen; i++ {
		if strings.IndexByte(base32Chars, s[i]) < 0 {
			return "", fmt.Errorf("nix: bad store path hash %q", s[:HashLen])
		}
	}
	name := s[HashLen+1:]
	if strings.HasPrefix(name, ".") {
		return "", fmt.Errorf("nix: bad store path name %q", name)
	}
	for _, c := range name {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("+-._?=", c)
		if !ok {
			return "", fmt.Errorf("nix: bad store path name %q", name)
		}
	}
	return StorePath(s), nil
}

// Hash is the 32-character hash part, which keys the narinfo upstream.
func (p StorePath) Hash() string { return string(p[:HashLen]) }

// Name is everything after the hash, such as "hello-2.12.1".
func (p StorePath) Name() string { return string(p[HashLen+1:]) }

// Abs is the full path under StoreDir.
func (p StorePath) Abs() string { return StoreDir + "/" + string(p) }

// Package splits Name the way Nix splits derivation names: the version
// starts at the first dash that is followed by something other than a
// letter. "hello-2.12.1" is ("hello", "2.12.1"); "gcc-14.3.0-lib" is
// ("gcc", "14.3.0-lib"); "env-manifest.nix" has no version.
func (p StorePath) Package() (name, version string) {
	n := p.Name()
	for i := 0; i+1 < len(n); i++ {
		if n[i] == '-' {
			c := n[i+1]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
				return n[:i], n[i+1:]
			}
		}
	}
	return n, ""
}
