package fill

import (
	"context"
	"sync"

	"github.com/progrium/wanix-cache/internal/store"
)

// LocalQuota implements Quota over the file store's byte count, the
// spool's byte count and its own reservations. Max 0 means unlimited.
type LocalQuota struct {
	Max   int64
	Store func() int64
	Spool func() int64

	mu        sync.Mutex
	reserved  int64
	reclaimer Reclaimer
}

var _ Quota = (*LocalQuota)(nil)

func (q *LocalQuota) setReclaimer(r Reclaimer) {
	q.mu.Lock()
	q.reclaimer = r
	q.mu.Unlock()
}

// Usage implements Quota.
func (q *LocalQuota) Usage() Usage {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Usage{Store: q.Store(), Spool: q.Spool(), Reserved: q.reserved, Max: q.Max}
}

// take reserves n bytes, reclaiming once if needed.
func (q *LocalQuota) take(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}
	for attempt := 0; ; attempt++ {
		q.mu.Lock()
		over := q.Max > 0 && q.Store()+q.Spool()+q.reserved+n > q.Max
		if !over {
			q.reserved += n
			q.mu.Unlock()
			return nil
		}
		need := q.Store() + q.Spool() + q.reserved + n - q.Max
		r := q.reclaimer
		q.mu.Unlock()
		if r == nil || attempt > 0 {
			return store.ErrFull
		}
		if _, err := r.Reclaim(ctx, need); err != nil {
			return err
		}
	}
}

// Reserve implements Quota.
func (q *LocalQuota) Reserve(ctx context.Context, n int64) (Reservation, error) {
	if err := q.take(ctx, n); err != nil {
		return nil, err
	}
	return &reservation{q: q, held: max(n, 0)}, nil
}

type reservation struct {
	q    *LocalQuota
	mu   sync.Mutex
	held int64
}

func (r *reservation) Grow(ctx context.Context, n int64) error {
	if err := r.q.take(ctx, n); err != nil {
		return err
	}
	r.mu.Lock()
	r.held += n
	r.mu.Unlock()
	return nil
}

// use moves n bytes from the reservation to the spool's own count, growing
// the reservation first if it is exhausted.
func (r *reservation) use(ctx context.Context, n int64) error {
	r.mu.Lock()
	short := n - r.held
	r.mu.Unlock()
	if short > 0 {
		if err := r.Grow(ctx, short); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.held -= n
	r.mu.Unlock()
	r.q.mu.Lock()
	r.q.reserved -= n
	r.q.mu.Unlock()
	return nil
}

func (r *reservation) Release() {
	r.mu.Lock()
	n := r.held
	r.held = 0
	r.mu.Unlock()
	r.q.mu.Lock()
	r.q.reserved -= n
	r.q.mu.Unlock()
}
