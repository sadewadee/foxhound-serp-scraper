// Package testpg boots a throwaway Postgres for tests that need a real
// database. It exists because two packages (internal/db and internal/stage)
// had near-identical copies of the same boot/credential/wait code, and because
// the deadline those copies used (60s) was too short on hosts where a cold
// postgres:17 needs ~90s or more to reach "ready to accept connections" — the
// symptom was an unhelpful
//
//	postgres never became ready: read tcp ...: connection reset by peer
//
// which looks like a code failure but is really an expired deadline.
//
// Two properties are deliberate and should not be relaxed:
//
//   - The container is started under `timeout`, so it self-terminates even if
//     the test binary is killed. Combined with `--rm` that guarantees no
//     stray container can outlive the test run on a host where an interactive
//     `docker rm` is not available.
//   - The container is bound to 127.0.0.1 only, with a random non-trivial
//     role name and an openssl-generated password, per the project's
//     credentials rule.
package testpg

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// DefaultReadyTimeout is how long WaitReady waits for Postgres to accept
// connections when TEST_PG_READY_TIMEOUT is unset. A cold postgres:17 measured
// ~90s on the build host, so the deadline is generous by design.
const DefaultReadyTimeout = 180 * time.Second

// ContainerLifetime bounds how long a throwaway container may live, enforced
// inside the container by `timeout` so a killed test cannot leak one.
const ContainerLifetime = 900 * time.Second

// DefaultImage is the Postgres image used unless StartOpts.Image overrides it.
const DefaultImage = "postgres:17"

// readyTimeoutEnv overrides DefaultReadyTimeout, e.g. "90s".
const readyTimeoutEnv = "TEST_PG_READY_TIMEOUT"

// ReadyTimeout returns the connection deadline: TEST_PG_READY_TIMEOUT when it
// parses as a positive duration, DefaultReadyTimeout otherwise (unset, empty,
// malformed, or non-positive).
func ReadyTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv(readyTimeoutEnv))
	if raw == "" {
		return DefaultReadyTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return DefaultReadyTimeout
	}
	return d
}

// StartOpts customizes a throwaway Postgres.
type StartOpts struct {
	// Image overrides DefaultImage.
	Image string
	// Prefix is prepended to the generated container name, e.g.
	// "xxchk_reconcile_". Defaults to "xxchk_".
	Prefix string
	// Port is the host port to publish. Defaults to a free ephemeral port.
	Port string
}

// Instance is a running throwaway Postgres.
type Instance struct {
	// Name is the container name (useful in failure messages).
	Name string
	// DSN is a ready-to-use connection string, loopback-bound.
	DSN string

	db *sql.DB
}

// Start boots a throwaway Postgres, waits for it to accept connections, and
// registers cleanup. It skips the test when docker is unavailable.
func Start(t *testing.T, opts StartOpts) *Instance {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}

	image := opts.Image
	if image == "" {
		image = DefaultImage
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "xxchk_"
	}
	port := opts.Port
	if port == "" {
		port = FreePort(t)
	}

	user := "xxchk_" + RandHex(t, 3)
	pass := RandB64(t)
	name := prefix + RandHex(t, 3)

	// `timeout` inside the container is the safety net: even if the test
	// process is killed, --rm plus the timeout guarantees the container goes
	// away instead of lingering and getting restarted by anything watching
	// healthchecks.
	cmd := exec.Command("docker", "run", "--rm", "-d",
		"--name", name,
		"-p", "127.0.0.1:"+port+":5432",
		"-e", "POSTGRES_USER="+user,
		"-e", "POSTGRES_PASSWORD="+pass,
		"-e", "POSTGRES_DB=xxchk",
		"--entrypoint", "sh", image,
		"-c", fmt.Sprintf("timeout %d docker-entrypoint.sh postgres", int(ContainerLifetime.Seconds())),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("start postgres: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})

	// The password is base64 and may contain characters that are reserved in
	// a URL, so build the DSN through net/url rather than Sprintf.
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     "127.0.0.1:" + port,
		Path:     "/xxchk",
		RawQuery: "sslmode=disable",
	}
	dsn := u.String()

	inst := &Instance{Name: name, DSN: dsn, db: WaitReady(t, dsn)}
	t.Cleanup(func() { _ = inst.db.Close() })
	return inst
}

// DB returns the ready connection pool.
func (i *Instance) DB() *sql.DB { return i.db }

// WaitReady polls dsn until it accepts connections or the ready deadline
// expires. The failure message names the deadline, how long it waited, and the
// override knob, so an expired deadline never reads like a code bug.
func WaitReady(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	timeout := ReadyTimeout()
	start := time.Now()
	deadline := start.Add(timeout)
	var lastErr error
	for {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = db.PingContext(ctx)
			cancel()
			if err == nil {
				return db
			}
			_ = db.Close()
		}
		lastErr = err
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	// The DSN carries the random password — redact it before it lands in logs.
	redacted := dsn
	if at := strings.LastIndex(redacted, "@"); at >= 0 {
		if scheme := strings.Index(redacted, "://"); scheme >= 0 {
			redacted = redacted[:scheme+3] + "***@" + redacted[at+1:]
		}
	}
	t.Fatalf("postgres (%s) not ready after waiting %s (deadline %s, override with %s): %v",
		redacted, time.Since(start).Round(time.Second), timeout, readyTimeoutEnv, lastErr)
	return nil
}

// RandHex returns n random bytes hex-encoded.
func RandHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// RandB64 returns 24 bytes of entropy from openssl, base64-encoded.
func RandB64(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("openssl", "rand", "-base64", "24").Output()
	if err != nil {
		t.Fatalf("openssl rand: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// FreePort returns a currently-free loopback TCP port.
func FreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return fmt.Sprintf("%d", ln.Addr().(*net.TCPAddr).Port)
}
