package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Client speaks the OCI distribution API (pull only), with anonymous
// bearer token auth as Docker Hub, GHCR, Quay and most others offer it.
type Client struct {
	HTTP *http.Client
	// PerRegistry caps concurrent blob downloads per registry host.
	PerRegistry int
	// PlainHTTP reports whether a host is spoken to over http rather than
	// https. Default: localhost and 127.0.0.1 only.
	PlainHTTP func(host string) bool
	// UserAgent is sent on every request.
	UserAgent string

	mu     sync.Mutex
	tokens map[string]token         // host + " " + scope -> token
	sems   map[string]chan struct{} // host -> blob download slots
}

type token struct {
	value   string
	expires time.Time
}

// ErrNotFound is a 404 from the registry.
var ErrNotFound = errors.New("oci: not found in registry")

// StatusError is a non-success reply from a registry.
type StatusError struct {
	URL    string
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("oci: %s: %d %s", e.URL, e.Status, strings.TrimSpace(e.Body))
}

func (e *StatusError) Unwrap() error {
	if e.Status == http.StatusNotFound {
		return ErrNotFound
	}
	return nil
}

func (c *Client) httpc() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) base(host string) string {
	plain := c.PlainHTTP
	if plain == nil {
		plain = func(h string) bool {
			h, _, _ = strings.Cut(h, ":")
			return h == "localhost" || h == "127.0.0.1"
		}
	}
	if plain(host) {
		return "http://" + host
	}
	return "https://" + host
}

func (c *Client) sem(host string) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sems == nil {
		c.sems = map[string]chan struct{}{}
	}
	s := c.sems[host]
	if s == nil {
		n := c.PerRegistry
		if n <= 0 {
			n = 4
		}
		s = make(chan struct{}, n)
		c.sems[host] = s
	}
	return s
}

// get issues a GET for path on ref's registry, authenticating with an
// anonymous pull token when the registry asks for one.
func (c *Client) get(ctx context.Context, ref Ref, path string, accept []string) (*http.Response, error) {
	host := ref.Host()
	u := c.base(host) + "/v2/" + ref.Repo + path
	scope := "repository:" + ref.Repo + ":pull"
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		for _, a := range accept {
			req.Header.Add("Accept", a)
		}
		if c.UserAgent != "" {
			req.Header.Set("User-Agent", c.UserAgent)
		}
		if t := c.cachedToken(host, scope); t != "" {
			req.Header.Set("Authorization", "Bearer "+t)
		}
		resp, err := c.httpc().Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			chal := resp.Header.Get("WWW-Authenticate")
			drain(resp)
			if err := c.authenticate(ctx, host, scope, chal); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			defer drain(resp)
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return nil, &StatusError{URL: u, Status: resp.StatusCode, Body: string(b)}
		}
		return resp, nil
	}
}

func drain(resp *http.Response) {
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}

func (c *Client) cachedToken(host, scope string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tokens[host+" "+scope]
	if !ok || time.Now().After(t.expires) {
		return ""
	}
	return t.value
}

// authenticate answers a Bearer challenge with an anonymous token.
func (c *Client) authenticate(ctx context.Context, host, scope, challenge string) error {
	scheme, params := parseChallenge(challenge)
	if !strings.EqualFold(scheme, "bearer") {
		return fmt.Errorf("oci: %s wants %q auth; only anonymous bearer tokens are supported", host, scheme)
	}
	realm := params["realm"]
	if realm == "" {
		return fmt.Errorf("oci: %s: bearer challenge without realm", host)
	}
	q := url.Values{}
	if s := params["service"]; s != "" {
		q.Set("service", s)
	}
	ask := scope
	if s := params["scope"]; s != "" {
		ask = s
	}
	q.Set("scope", ask)
	tu := realm
	if strings.Contains(tu, "?") {
		tu += "&" + q.Encode()
	} else {
		tu += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tu, nil)
	if err != nil {
		return err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	resp, err := c.httpc().Do(req)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &StatusError{URL: realm, Status: resp.StatusCode, Body: string(b)}
	}
	var tr struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tr); err != nil {
		return fmt.Errorf("oci: token from %s: %w", realm, err)
	}
	v := tr.Token
	if v == "" {
		v = tr.AccessToken
	}
	if v == "" {
		return fmt.Errorf("oci: token from %s: empty", realm)
	}
	ttl := time.Duration(tr.ExpiresIn) * time.Second
	if ttl < 60*time.Second {
		ttl = 60 * time.Second // the spec's default lifetime
	}
	c.mu.Lock()
	if c.tokens == nil {
		c.tokens = map[string]token{}
	}
	// Renew a little early so a long pull doesn't race the expiry.
	c.tokens[host+" "+scope] = token{value: v, expires: time.Now().Add(ttl - 10*time.Second)}
	c.mu.Unlock()
	return nil
}

// parseChallenge parses `Bearer realm="...",service="...",scope="..."`.
func parseChallenge(h string) (scheme string, params map[string]string) {
	params = map[string]string{}
	h = strings.TrimSpace(h)
	scheme, rest, _ := strings.Cut(h, " ")
	for rest = strings.TrimSpace(rest); rest != ""; {
		k, v, ok := strings.Cut(rest, "=")
		if !ok {
			break
		}
		k = strings.ToLower(strings.TrimSpace(k))
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, `"`) {
			end := strings.IndexByte(v[1:], '"')
			if end < 0 {
				params[k] = v[1:]
				break
			}
			params[k] = v[1 : end+1]
			rest = v[end+2:]
		} else {
			val, r, _ := strings.Cut(v, ",")
			params[k] = strings.TrimSpace(val)
			rest = "," + r
		}
		rest = strings.TrimLeft(rest, ", ")
	}
	return scheme, params
}

// Manifest fetches a manifest or index by tag or digest. It returns the
// raw body and its media type.
func (c *Client) Manifest(ctx context.Context, ref Ref, reference string) ([]byte, string, error) {
	resp, err := c.get(ctx, ref, "/manifests/"+reference, manifestAccept)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxManifest+1))
	if err != nil {
		return nil, "", err
	}
	if len(body) > maxManifest {
		return nil, "", fmt.Errorf("oci: %s: manifest over %d bytes", ref, maxManifest)
	}
	mt := resp.Header.Get("Content-Type")
	if i := strings.IndexByte(mt, ';'); i >= 0 {
		mt = strings.TrimSpace(mt[:i])
	}
	return body, mt, nil
}

// Blob opens a blob for reading. The download holds one of the registry's
// PerRegistry slots until the reader is closed.
func (c *Client) Blob(ctx context.Context, ref Ref, digest string) (io.ReadCloser, error) {
	sem := c.sem(ref.Host())
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	resp, err := c.get(ctx, ref, "/blobs/"+digest, nil)
	if err != nil {
		<-sem
		return nil, err
	}
	return &slotBody{ReadCloser: resp.Body, sem: sem}, nil
}

// SmallBlob reads a whole small blob (an image config) without taking a
// download slot, so config fetches never queue behind layer downloads.
func (c *Client) SmallBlob(ctx context.Context, ref Ref, digest string, limit int64) ([]byte, error) {
	resp, err := c.get(ctx, ref, "/blobs/"+digest, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("oci: blob %s over %d bytes", digest, limit)
	}
	return b, nil
}

type slotBody struct {
	io.ReadCloser
	sem  chan struct{}
	once sync.Once
}

func (b *slotBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { <-b.sem })
	return err
}
