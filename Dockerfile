# syntax=docker/dockerfile:1
# Multi-stage production Dockerfile for serp-scraper.
#
# Build:
#   docker build -t ghcr.io/sadewadee/foxhound-serp-scraper:latest .
#
# Run:
#   docker run --shm-size=256m \
#     -e POSTGRES_DSN="postgres://..." \
#     -e REDIS_ADDR="100.x.x.1:6379" \
#     ghcr.io/sadewadee/foxhound-serp-scraper run -stage enrich -workers 20

# ---------------------------------------------------------------------------
# Stage 1: builder
# ---------------------------------------------------------------------------
FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Deps first for layer cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -tags playwright,tls \
    -ldflags="-w -s -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o /serp-scraper \
    .

# ---------------------------------------------------------------------------
# Stage 2: browser
# Install Camoufox + playwright Firefox in a throwaway image.
# Only ~/.cache directories are forwarded to runtime stage.
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS browser

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl python3 python3-pip xvfb \
    fonts-liberation fonts-noto-cjk \
    libasound2t64 libatk1.0-0t64 libcairo2 libcups2t64 \
    libdbus-glib-1-2 libgdk-pixbuf-2.0-0 libgtk-3-0t64 \
    libnspr4 libnss3 libpango-1.0-0 libx11-xcb1 \
    libxcomposite1 libxdamage1 libxrandr2 \
    && rm -rf /var/lib/apt/lists/*

# Camoufox browser binary.
RUN pip3 install --break-system-packages camoufox \
    && python3 -m camoufox fetch

# Playwright Firefox driver (version from go.mod).
COPY go.mod /tmp/go.mod
RUN apt-get update && apt-get install -y --no-install-recommends golang unzip \
    && PWGO_VER=$(grep -oE 'playwright-community/playwright-go v[0-9]+\.[0-9]+\.[0-9.]+' /tmp/go.mod | awk '{print $2}') \
    && go install github.com/playwright-community/playwright-go/cmd/playwright@${PWGO_VER} \
    && /root/go/bin/playwright install --with-deps firefox \
    && apt-get purge -y golang \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/go/pkg /tmp/go.mod

# Pre-cache NopeCHA extension (auto-solve captcha).
RUN mkdir -p /root/.cache/foxhound/extensions/nopecha \
    && RELEASE_URL=$(curl -fsSL https://api.github.com/repos/NopeCHALLC/nopecha-extension/releases/latest \
       | grep -o '"browser_download_url": *"[^"]*firefox\.zip"' \
       | head -1 | cut -d'"' -f4) \
    && curl -fsSL "$RELEASE_URL" -o /tmp/nopecha.zip \
    && unzip -q /tmp/nopecha.zip -d /root/.cache/foxhound/extensions/nopecha \
    && rm /tmp/nopecha.zip

# ---------------------------------------------------------------------------
# Stage 3: runtime
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    xvfb \
    libgtk-3-0t64 libdbus-glib-1-2 libxt6 libnss3 libnspr4 \
    libxcomposite1 libxdamage1 libxrandr2 libxss1 libxcursor1 \
    libxi6 libxtst6 libdrm2 libgbm1 libasound2t64 \
    libatk1.0-0t64 libcairo2 libcups2t64 libgdk-pixbuf-2.0-0 \
    libpango-1.0-0 libx11-xcb1 \
    fonts-liberation fonts-noto fonts-noto-cjk \
    ca-certificates tzdata curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 scraper && \
    useradd --uid 1001 --gid scraper --shell /bin/bash --create-home scraper

COPY --from=builder /serp-scraper /usr/local/bin/serp-scraper

COPY --from=browser --chown=scraper:scraper \
    /root/.cache/camoufox /home/scraper/.cache/camoufox
COPY --from=browser --chown=scraper:scraper \
    /root/.cache/ms-playwright /home/scraper/.cache/ms-playwright
COPY --from=browser --chown=scraper:scraper \
    /root/.cache/ms-playwright-go /home/scraper/.cache/ms-playwright-go
COPY --from=browser --chown=scraper:scraper \
    /root/.cache/foxhound /home/scraper/.cache/foxhound

RUN mkdir -p /data/output /app/config && \
    chown -R scraper:scraper /data /app

VOLUME ["/dev/shm", "/data"]

USER scraper
WORKDIR /home/scraper

ENV PLAYWRIGHT_BROWSERS_PATH=/home/scraper/.cache/ms-playwright

EXPOSE 8080 9090

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8080/api/health || exit 1

# Foxhound's DisplayManager (headless=virtual) spawns Xvfb as a Go-managed
# child, monitors it, and restarts on crash — so we must NOT pre-set DISPLAY
# and must NOT start Xvfb in the entrypoint. Doing so would make foxhound
# skip its own manager, and a crashed external Xvfb would leave the browser
# wedged with no recovery.
ENTRYPOINT ["serp-scraper"]
CMD ["run"]
