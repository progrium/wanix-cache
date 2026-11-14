package oci

import (
	"fmt"
	"regexp"
	"strings"
)

// Ref is an image reference as it appears in the namespace:
// <registry>/<repo>@<tag|digest>, for example
// docker.io/library/alpine@3.20 or ghcr.io/org/img@sha256:<hex>.
type Ref struct {
	Registry string // as named in the path: docker.io, ghcr.io, localhost:5000
	Repo     string // library/alpine
	Tag      string // set when the reference is by tag
	Digest   string // set when the reference is by digest (sha256:<hex>)
}

var (
	tagRE    = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	digestRE = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	compRE   = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)
)

// String is the reference's path form.
func (r Ref) String() string { return r.Registry + "/" + r.Repo + "@" + r.Version() }

// Version is the tag or the digest.
func (r Ref) Version() string {
	if r.Digest != "" {
		return r.Digest
	}
	return r.Tag
}

// Host is the registry host the API is served from. Docker Hub's API lives
// on registry-1.docker.io.
func (r Ref) Host() string {
	if r.Registry == "docker.io" || r.Registry == "index.docker.io" {
		return "registry-1.docker.io"
	}
	return r.Registry
}

// IsRegistry reports whether a first path component names a registry
// rather than a repository component: it has a dot or a port, or is
// localhost, the same rule Docker uses.
func IsRegistry(name string) bool {
	return strings.ContainsAny(name, ".:") || name == "localhost"
}

// ParseRef parses a path form reference. The registry must be explicit,
// and a Docker Hub repo with one component gets the library/ prefix, so
// docker.io/alpine@3.20 and docker.io/library/alpine@3.20 name the same image.
func ParseRef(s string) (Ref, error) {
	at := strings.LastIndexByte(s, '@')
	if at < 0 {
		return Ref{}, fmt.Errorf("oci: %q: no @<tag|digest>", s)
	}
	name, ver := s[:at], s[at+1:]
	slash := strings.IndexByte(name, '/')
	if slash < 0 {
		return Ref{}, fmt.Errorf("oci: %q: need <registry>/<repo>", s)
	}
	r := Ref{Registry: name[:slash], Repo: name[slash+1:]}
	if !IsRegistry(r.Registry) {
		return Ref{}, fmt.Errorf("oci: %q: %q is not a registry host", s, r.Registry)
	}
	if r.Registry == "index.docker.io" {
		r.Registry = "docker.io"
	}
	for _, c := range strings.Split(r.Repo, "/") {
		if !compRE.MatchString(c) {
			return Ref{}, fmt.Errorf("oci: %q: bad repository component %q", s, c)
		}
	}
	if r.Registry == "docker.io" && !strings.Contains(r.Repo, "/") {
		r.Repo = "library/" + r.Repo
	}
	switch {
	case digestRE.MatchString(ver):
		r.Digest = ver
	case tagRE.MatchString(ver):
		r.Tag = ver
	default:
		return Ref{}, fmt.Errorf("oci: %q: bad tag or digest %q", s, ver)
	}
	return r, nil
}
