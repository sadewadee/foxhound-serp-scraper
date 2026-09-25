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
#
# Pinned deliberately. Unpinned `pip3 install camoufox` grabs whatever's
# latest on PyPI (0.5.6 today), which switched its on-disk cache layout to a
# nested browsers/official/<hash>/ path — foxhound v0.0.27's
# findCamoufoxBinary() only knows the old flat ~/.cache/camoufox/camoufox
# layout, so it silently falls back to plain Firefox (no anti-fingerprint)
# and gets a reCAPTCHA on Google instead of results.
#
# camoufox==0.4.11 keeps the flat layout, but its own `fetch` always resolves
# "latest GitHub release in range" too — with no CLI flag or env var to pin a
# browser version (checked pkgman.py's CamoufoxFetcher/GitHubDownloader: the
# only inputs are the live GitHub releases API response). Newer releases
# (beta.26+) are well-formed and would now win that "latest" race, same
# problem one layer down. So we bypass fetch_latest() entirely: construct the
# fetcher without invoking GitHub API resolution and hand it the exact
# beta.24 asset URL (the version already deployed in v0.9.8-niche,
# version.json {"version":"135.0.1","release":"beta.24"}) so `install()` -
# same download/extract/version.json logic camoufox itself uses - produces a
# deterministic, byte-identical result on every build.
RUN pip3 install --break-system-packages camoufox==0.4.11 \
    && python3 -c "\
import camoufox.pkgman as pkgman; \
from camoufox.addons import DefaultAddons, maybe_download_addons; \
f = object.__new__(pkgman.CamoufoxFetcher); \
f._version_obj = pkgman.Version(release='beta.24', version='135.0.1'); \
f._url = 'https://github.com/daijro/camoufox/releases/download/v135.0.1-beta.24/camoufox-135.0.1-beta.24-lin.x86_64.zip'; \
f.install(); \
maybe_download_addons(list(DefaultAddons))"

# Playwright Firefox driver (version from go.mod).
#
# Microsoft retired the *.azureedge.net driver-zip CDN, and its replacement
# (cdn.playwright.dev/dbazure/download/playwright) never actually serves the
# driver zip either: every playwright driver version we probed (1.40 - 1.62,
# including our pinned 1.57.0) 400s at "/builds/driver/*" with a Microsoft
# gateway error (upstream tracked + closed as "won't fix": they dropped
# zip-based driver distribution entirely in playwright-go v0.6100+, which we
# can't jump to here without a go.mod/import-path migration). PLAYWRIGHT_
# DOWNLOAD_HOST alone cannot fix this — it only changes which dead mirror
# playwright-go's zip downloader hits.
#
# Workaround, built to be structurally identical to what the retired zip used
# to produce (driver dir = LICENSE + node + package/), so nothing downstream
# (foxhound's playwright.Run(), the runtime image) has to change:
#   1. Install the *same-version* `playwright` npm package (npm's registry is
#      healthy, unaffected by the CDN outage) into the driver cache dir
#      playwright-go expects, so DownloadDriver()'s up-to-date check finds a
#      working driver and skips the dead zip fetch entirely.
#   2. Download the official Node.js binary the zip used to bundle, verify it
#      against nodejs.org's published SHASUMS256.txt (build fails on
#      mismatch), and place it at .../<version>/node — the exact path/name
#      playwright-go's default getNodeExecutable() looks for, so no
#      PLAYWRIGHT_NODEJS_PATH override is needed anywhere, build or runtime.
# The Firefox *browser* binary itself downloads fine through the driver's own
# (currently healthy) cdn.playwright.dev resolution; PLAYWRIGHT_DOWNLOAD_HOST
# is pinned below for determinism, scoped to just that install step.
COPY go.mod /tmp/go.mod
RUN apt-get update && apt-get install -y --no-install-recommends golang unzip \
    && PWGO_VER=$(grep -oE 'playwright-community/playwright-go v[0-9]+\.[0-9]+\.[0-9.]+' /tmp/go.mod | awk '{print $2}') \
    && go install github.com/playwright-community/playwright-go/cmd/playwright@${PWGO_VER} \
    && PW_VER=$(grep -oE 'playwrightCliVersion = "[0-9]+\.[0-9]+\.[0-9]+"' /root/go/pkg/mod/github.com/playwright-community/playwright-go@${PWGO_VER}/run.go | grep -oE '[0-9]+\.[0-9]+\.[0-9]+') \
    && DRIVER_DIR=/root/.cache/ms-playwright-go/${PW_VER} \
    && mkdir -p "${DRIVER_DIR}/package/node_modules" \
    && NODE_VER=v24.11.1 \
    && curl -fsSL -o /tmp/node.tar.gz "https://nodejs.org/dist/${NODE_VER}/node-${NODE_VER}-linux-x64.tar.gz" \
    && curl -fsSL -o /tmp/node.SHASUMS256.txt "https://nodejs.org/dist/${NODE_VER}/SHASUMS256.txt" \
    && EXPECTED_SHA=$(grep " node-${NODE_VER}-linux-x64.tar.gz\$" /tmp/node.SHASUMS256.txt | awk '{print $1}') \
    && ACTUAL_SHA=$(sha256sum /tmp/node.tar.gz | awk '{print $1}') \
    && [ -n "$EXPECTED_SHA" ] && [ "$EXPECTED_SHA" = "$ACTUAL_SHA" ] \
    && tar -xzf /tmp/node.tar.gz -C /tmp \
    && /tmp/node-${NODE_VER}-linux-x64/bin/node /tmp/node-${NODE_VER}-linux-x64/bin/npm \
       install --no-save --prefix /tmp/pw-npm playwright@${PW_VER} \
    && cp -a /tmp/pw-npm/node_modules/playwright/. "${DRIVER_DIR}/package/" \
    && cp -a /tmp/pw-npm/node_modules/playwright-core "${DRIVER_DIR}/package/node_modules/playwright-core" \
    && cp /tmp/node-${NODE_VER}-linux-x64/bin/node "${DRIVER_DIR}/node" \
    && chmod 755 "${DRIVER_DIR}/node" \
    && cp /tmp/node-${NODE_VER}-linux-x64/LICENSE "${DRIVER_DIR}/LICENSE" \
    && rm -rf /tmp/pw-npm /tmp/node.tar.gz /tmp/node.SHASUMS256.txt /tmp/node-${NODE_VER}-linux-x64 \
    && PLAYWRIGHT_DOWNLOAD_HOST=https://cdn.playwright.dev/dbazure/download/playwright \
       /root/go/bin/playwright install --with-deps firefox \
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
