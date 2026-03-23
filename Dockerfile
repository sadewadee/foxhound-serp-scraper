# syntax=docker/dockerfile:1
# Multi-stage production Dockerfile for serp-scraper.
#
# Build stages:
#   builder  — compiles the Go binary with -tags playwright
#   browser  — installs Camoufox + playwright-go driver (Firefox) in a throwaway image
#   runtime  — minimal Ubuntu 24.04: binary + browser caches + Xvfb + non-root user
#
# IMPORTANT — build context must be the PARENT directory ("Plugin Pro/") because
# go.mod contains:
#   replace github.com/sadewadee/foxhound => ../foxhound
# and Docker's COPY cannot reference paths outside the build context.
#
# Build from the parent directory:
#   docker build -f serp-scraper/Dockerfile -t serp-scraper:latest .
#
# Or use the docker-compose.yaml which sets `context: ..` for you.
#
# Environment variables (see config.yaml for full list):
#   POSTGRES_DSN             PostgreSQL connection string (required for pipeline)
#   REDIS_ADDR               Redis address (default: localhost:6379)
#   REDIS_PASSWORD           Redis password (default: empty)
#   PROXY_URL                Optional HTTP/SOCKS proxy
#   API_SECRET               HTTP API secret token
#   API_ADMIN_PASSWORD       Admin password for HTTP API
#   MORDIBOUNCER_API_URL     Email validation API URL
#   MORDIBOUNCER_SECRET      Email validation API secret

# ---------------------------------------------------------------------------
# Stage 1: builder
# Compiles the Go binary with the playwright build tag.
# Both serp-scraper/ and foxhound/ are copied from the parent build context
# so the local replace directive in go.mod resolves correctly:
#   replace github.com/sadewadee/foxhound => ../foxhound
# Inside the container we mirror this layout under /workspace.
# ---------------------------------------------------------------------------
FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# /workspace mirrors the parent-directory layout:
#   /workspace/foxhound/        ← satisfies the replace directive
#   /workspace/serp-scraper/    ← the module being built
WORKDIR /workspace

# Copy foxhound module (sibling directory in the parent context).
COPY foxhound/ ./foxhound/

# Copy serp-scraper module; deps first for layer-cache efficiency.
WORKDIR /workspace/serp-scraper
COPY serp-scraper/go.mod serp-scraper/go.sum ./
RUN go mod download

COPY serp-scraper/ .

# Build with playwright tag; CGO disabled for a fully static binary.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -tags playwright \
    -ldflags="-w -s -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o /serp-scraper \
    .

# ---------------------------------------------------------------------------
# Stage 2: browser
# Installs Camoufox (Python package) and the playwright-go driver with
# Firefox inside a throwaway image. Only ~/.cache directories are forwarded
# to the runtime stage, keeping the final image free of Python and pip.
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS browser

# Full dependency set required to run Firefox/Camoufox during installation.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    python3 \
    python3-pip \
    xvfb \
    fonts-liberation \
    fonts-noto-cjk \
    libasound2t64 \
    libatk1.0-0t64 \
    libcairo2 \
    libcups2t64 \
    libdbus-glib-1-2 \
    libgdk-pixbuf-2.0-0 \
    libgtk-3-0t64 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    && rm -rf /var/lib/apt/lists/*

# Fetch Camoufox browser binary via the official Python package.
# --break-system-packages is required on Ubuntu 24.04 (PEP 668).
RUN pip3 install --break-system-packages camoufox \
    && python3 -m camoufox fetch

# Install the playwright-go driver that matches the version pinned in go.mod
# (playwright-go v0.5700.1). The version is extracted automatically so this
# layer stays in sync if go.mod is ever updated.
COPY serp-scraper/go.mod /tmp/go.mod

RUN apt-get update && apt-get install -y --no-install-recommends golang \
    && PWGO_VER=$(grep -oE 'playwright-community/playwright-go v[0-9]+\.[0-9]+\.[0-9.]+' /tmp/go.mod | awk '{print $2}') \
    && go install github.com/playwright-community/playwright-go/cmd/playwright@${PWGO_VER} \
    && /root/go/bin/playwright install --with-deps firefox \
    && apt-get purge -y golang \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/go/pkg /tmp/go.mod

# ---------------------------------------------------------------------------
# Stage 3: runtime
# Minimal production image: compiled binary + browser caches + runtime libs.
# Xvfb provides the virtual display that Camoufox needs on headless servers.
# Runs as a non-root user to reduce attack surface.
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS runtime

# Security updates + runtime shared libraries only (no build tools).
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    # Virtual display for headless Camoufox / Firefox
    xvfb \
    # Firefox / Camoufox shared libraries
    libgtk-3-0t64 \
    libdbus-glib-1-2 \
    libxt6 \
    libnss3 \
    libnspr4 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxss1 \
    libxcursor1 \
    libxi6 \
    libxtst6 \
    libdrm2 \
    libgbm1 \
    libasound2t64 \
    libatk1.0-0t64 \
    libcairo2 \
    libcups2t64 \
    libgdk-pixbuf-2.0-0 \
    libpango-1.0-0 \
    libx11-xcb1 \
    # Fonts for realistic page rendering
    fonts-liberation \
    fonts-noto \
    fonts-noto-cjk \
    # TLS certificates
    ca-certificates \
    # Timezone data (IANA tz)
    tzdata \
    # curl for liveness / readiness health checks
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — running as root inside a container flags some anti-bot systems
# and is generally a security risk.
RUN groupadd --gid 1001 scraper && \
    useradd --uid 1001 --gid scraper --shell /bin/bash --create-home scraper

# Copy the statically-compiled binary.
COPY --from=builder /serp-scraper /usr/local/bin/serp-scraper

# Copy Camoufox browser binary fetched in the browser stage.
COPY --from=browser --chown=scraper:scraper \
    /root/.cache/camoufox \
    /home/scraper/.cache/camoufox

# Copy playwright-go driver and Firefox browser installed in the browser stage.
COPY --from=browser --chown=scraper:scraper \
    /root/.cache/ms-playwright \
    /home/scraper/.cache/ms-playwright

# Working directories for output and persistent data.
RUN mkdir -p /data/output /app/config && \
    chown -R scraper:scraper /data /app

# /dev/shm is critical for browser stability under load.
# Mount as tmpfs with at least 256 MB: docker run --shm-size=256m
VOLUME ["/dev/shm", "/data"]

USER scraper
WORKDIR /home/scraper

# Runtime environment.
# DISPLAY points to the Xvfb virtual display started by the entrypoint.
# PLAYWRIGHT_BROWSERS_PATH tells playwright-go where Firefox lives.
ENV DISPLAY=:99 \
    PLAYWRIGHT_BROWSERS_PATH=/home/scraper/.cache/ms-playwright

# API port (HTTP API + pipeline management).
EXPOSE 8080
# Prometheus metrics port.
EXPOSE 9090

# Health check hits the /health endpoint once the API is up.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8080/health || exit 1

# Entrypoint: start a virtual framebuffer then exec serp-scraper so that
# SIGTERM / SIGINT are delivered directly for graceful shutdown.
ENTRYPOINT ["sh", "-c", "Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp & exec serp-scraper \"$@\"", "--"]
CMD ["run"]
