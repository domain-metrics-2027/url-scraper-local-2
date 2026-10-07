# URL scraper as a long-running pod: ungoogled-chromium + cf-autoclick behind
# Xvfb (extensions need a headed browser), driven by scraper.py over CDP.
#
# The GitHub-runner workflow this replaces installed the same stack on every
# start (6-hour runners, ~4 min boot); here it is baked once and the pods run
# 24/7 inside the cluster.
FROM python:3.12-slim-bookworm

ARG CHROMIUM_VERSION=149.0.7827.53-1
ARG CF_AUTOCLICK_REF=6879c45513f5e99655ed2c725c31cbd19968c6c9

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    CHROME_BIN=/opt/chromium/chrome \
    CF_AUTOCLICK_DIR=/opt/cf-autoclick \
    DISPLAY=:99

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git xz-utils xvfb procps \
      libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 libcups2 \
      libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
      libgbm1 libasound2 libpango-1.0-0 libcairo2 libgtk-3-0 libx11-xcb1 \
      libxcb-dri3-0 libxshmfence1 libxss1 libxtst6 libglib2.0-0 libdbus-1-3 \
      libexpat1 libu2f-udev libvulkan1 \
      fonts-liberation fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Pinned to the build the GitHub workflow already proved against Cloudflare.
RUN curl -fL -o /tmp/chromium.tar.xz \
      "https://github.com/ungoogled-software/ungoogled-chromium-portablelinux/releases/download/${CHROMIUM_VERSION}/ungoogled-chromium-${CHROMIUM_VERSION}-x86_64_linux.tar.xz" \
    && tar -xJf /tmp/chromium.tar.xz -C /opt \
    && mv /opt/ungoogled-chromium-* /opt/chromium \
    && rm /tmp/chromium.tar.xz \
    && /opt/chromium/chrome --version

RUN git clone https://github.com/tenacious6/cf-autoclick.git /opt/cf-autoclick \
    && git -C /opt/cf-autoclick checkout -q "${CF_AUTOCLICK_REF}" \
    && rm -rf /opt/cf-autoclick/.git \
    && test -f /opt/cf-autoclick/manifest.json

RUN pip install --no-cache-dir flask websocket-client requests

RUN useradd -m -u 1000 scraper
WORKDIR /app
COPY scraper.py entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh && chown -R scraper:scraper /app
USER scraper

EXPOSE 8814
ENTRYPOINT ["/app/entrypoint.sh"]
