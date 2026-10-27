package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/sadewadee/serp-scraper/internal/testpg"
)

func newRefillRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })
	return mr, rc
}

// The members must be byte-identical to what pushQueryToQueue emits, otherwise
// a refilled query and an insert-time push of the same query would both queue.
func TestQueueMemberFormat(t *testing.T) {
	if got, want := queueMember(42, `yoga "studio"`), `{"id":42,"text":"yoga \"studio\""}`; got != want {
		t.Errorf("queueMember = %s, want %s", got, want)
	}

	mr, rc := newRefillRedis(t)
	r := NewRepositoryWithRedis(nil, rc)
	r.pushQueryToQueue(context.Background(), 7, "pilates")
	members, err := mr.ZMembers(QueueKey)
	if err != nil || len(members) != 1 || members[0] != queueMember(7, "pilates") {
		t.Errorf("pushQueryToQueue member = %v (err %v), want %s", members, err, queueMember(7, "pilates"))
	}
}

// Neither the low-water short-circuit nor a held lock may touch Postgres; the
// nil *sql.DB makes any query panic, so these also prove the DB stays untouched.
func TestRefillQueueSkips(t *testing.T) {
	ctx := context.Background()

	t.Run("at or above low-water", func(t *testing.T) {
		mr, rc := newRefillRedis(t)
		for i := 0; i < 3; i++ {
			mr.ZAdd(QueueKey, float64(i), fmt.Sprintf("m%d", i))
		}
		r := NewRepositoryWithRedis(nil, rc)
		for _, lowWater := range []int{3, 2, 1} { // ZCARD == lowWater is NOT below it
			n, err := r.RefillQueue(ctx, lowWater, 10)
			if n != 0 || err != nil {
				t.Errorf("lowWater=%d: got (%d, %v), want (0, nil)", lowWater, n, err)
			}
		}
		if mr.Exists(RefillLockKey) {
			t.Error("lock taken although refill was not needed")
		}
	})

	t.Run("lock held by another container", func(t *testing.T) {
		mr, rc := newRefillRedis(t)
		if err := mr.Set(RefillLockKey, "other"); err != nil {
			t.Fatal(err)
		}
		mr.SetTTL(RefillLockKey, time.Minute)
		r := NewRepositoryWithRedis(nil, rc)
		n, err := r.RefillQueue(ctx, 500, 1000)
		if n != 0 || err != nil {
			t.Errorf("got (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("no redis or empty batch", func(t *testing.T) {
		if n, err := NewRepository(nil).RefillQueue(ctx, 500, 1000); n != 0 || err != nil {
			t.Errorf("no redis: got (%d, %v)", n, err)
		}
		_, rc := newRefillRedis(t)
		if n, err := NewRepositoryWithRedis(nil, rc).RefillQueue(ctx, 500, 0); n != 0 || err != nil {
			t.Errorf("batch 0: got (%d, %v)", n, err)
		}
	})
}

func TestRefillQueueFromPostgres(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_refill_"}).DB()
	if _, err := db.Exec(`CREATE TABLE queries (
		id BIGSERIAL PRIMARY KEY, text TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending')`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	// ids 1..5 pending, 6 processing, 7 completed, 8..10 pending.
	statuses := []string{"pending", "pending", "pending", "pending", "pending", "processing", "completed", "pending", "pending", "pending"}
	for i, st := range statuses {
		if _, err := db.Exec(`INSERT INTO queries (text, status) VALUES ($1, $2)`, fmt.Sprintf("q%d", i+1), st); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	ctx := context.Background()
	mr, rc := newRefillRedis(t)
	r := NewRepositoryWithRedis(db, rc)

	// Empty queue -> pushes the first `batch` pending queries in id order, skipping non-pending.
	n, err := r.RefillQueue(ctx, 500, 4)
	if err != nil || n != 4 {
		t.Fatalf("first refill = (%d, %v), want (4, nil)", n, err)
	}
	want := []string{queueMember(1, "q1"), queueMember(2, "q2"), queueMember(3, "q3"), queueMember(4, "q4")}
	got, _ := mr.ZMembers(QueueKey) // miniredis returns members sorted by score
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("queue = %v, want %v", got, want)
	}
	// Scores are UnixMicro (same unit as pushQueryToQueue), i.e. ~1e15, never ids.
	score, _ := mr.ZScore(QueueKey, want[0])
	if score < float64(time.Now().Add(-time.Minute).UnixMicro()) || score > float64(time.Now().Add(time.Minute).UnixMicro()) {
		t.Errorf("score %v is not a current UnixMicro timestamp", score)
	}

	// The lock is still held (it is left to expire), so an immediate second
	// call from another container is a no-op even though ZCARD < lowWater.
	if n, err := r.RefillQueue(ctx, 500, 4); n != 0 || err != nil {
		t.Errorf("refill under held lock = (%d, %v), want (0, nil)", n, err)
	}

	// After the lock expires, a refill re-reads the same head of the pending
	// set: members already queued are NOT re-added (NX keeps their score) and
	// only the new ones count.
	mr.Del(RefillLockKey)
	before, _ := mr.ZScore(QueueKey, want[0])
	n, err = r.RefillQueue(ctx, 500, 6)
	if err != nil || n != 2 {
		t.Fatalf("second refill = (%d, %v), want (2, nil): q1-q4 are already queued", n, err)
	}
	after, _ := mr.ZScore(QueueKey, want[0])
	if after != before {
		t.Errorf("existing member rescored %v -> %v; ZADD NX must leave it alone", before, after)
	}
	if card := len(mustMembers(t, mr)); card != 6 {
		t.Errorf("queue size = %d, want 6 (no duplicates)", card)
	}
}

func mustMembers(t *testing.T, mr *miniredis.Miniredis) []string {
	t.Helper()
	m, err := mr.ZMembers(QueueKey)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
