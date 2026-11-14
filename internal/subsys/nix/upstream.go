package nix

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// DefaultSubstituter is used when Options.Substituters is empty.
const DefaultSubstituter = "https://cache.nixos.org"

// substituter is one upstream binary cache.
type substituter struct {
	base   *url.URL
	client *http.Client
}

func (s *substituter) get(ctx context.Context, rel string) (*http.Response, error) {
	u, err := s.base.Parse(rel)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return resp, nil
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden:
		resp.Body.Close()
		return nil, fmt.Errorf("nix: %s: %w", u, fs.ErrNotExist)
	}
	resp.Body.Close()
	return nil, fmt.Errorf("nix: %s: %s", u, resp.Status)
}

// narInfo fetches and parses <hash>.narinfo.
func (s *substituter) narInfo(ctx context.Context, hash string) (*NarInfo, string, error) {
	resp, err := s.get(ctx, hash+".narinfo")
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, "", err
	}
	ni, err := ParseNarInfo(string(b))
	if err != nil {
		return nil, "", err
	}
	if ni.StorePath.Hash() != hash {
		return nil, "", fmt.Errorf("nix: %s.narinfo describes %s", hash, ni.StorePath)
	}
	return ni, string(b), nil
}

// nar opens the decompressed NAR stream a narinfo points at.
func (s *substituter) nar(ctx context.Context, ni *NarInfo) (io.ReadCloser, error) {
	resp, err := s.get(ctx, ni.URL)
	if err != nil {
		return nil, err
	}
	r, err := decompress(ni.Compression, resp.Body)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	return readCloser{r, func() error {
		if c, ok := r.(io.Closer); ok && c != io.Closer(resp.Body) {
			c.Close()
		}
		return resp.Body.Close()
	}}, nil
}

type readCloser struct {
	io.Reader
	close func() error
}

func (r readCloser) Close() error { return r.close() }

func decompress(method string, r io.Reader) (io.Reader, error) {
	switch strings.ToLower(method) {
	case "none", "":
		return struct{ io.Reader }{r}, nil
	case "xz":
		return xz.NewReader(r)
	case "zstd":
		d, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		return zstdReader{d}, nil
	case "bzip2":
		return bzip2.NewReader(r), nil
	case "gzip":
		return gzip.NewReader(r)
	}
	return nil, errors.New("nix: unsupported NAR compression " + method)
}

type zstdReader struct{ *zstd.Decoder }

func (z zstdReader) Close() error { z.Decoder.Close(); return nil }
