"""
Minimal URL Scraper — Fresh Chrome per request via CDP.

Files:
  - scraper.py (this file) — Flask API + Chrome lifecycle
  - url-scraper.json — Workflow schema (reference only, JS is embedded here)

Each request:
  1. Launch fresh ungoogled-chromium with cf-autoclick extension
  2. Connect via CDP (Chrome DevTools Protocol)  
  3. Navigate to URL, wait for page load
  4. Execute JS to extract data using selectors
  5. Kill chrome, delete profile
  
API:
  POST /url-scraper-service/api/v1/scrape/
  GET  /url-scraper-service/api/v1/health/
"""

import argparse
import base64
import json
import os
import random
import select
import shutil
import threading
import subprocess
import sys
import tempfile
import time
import socket
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

import requests as http_requests
from flask import Flask, jsonify, request as flask_request

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

SCRIPT_DIR = Path(__file__).parent.resolve()

# Paths — override with env vars
CHROME_BIN = os.getenv("CHROME_BIN", str(SCRIPT_DIR / "vendor" / "ungoogled-chromium" / "chrome"))
CF_AUTOCLICK_DIR = os.getenv("CF_AUTOCLICK_DIR", str(SCRIPT_DIR / "vendor" / "cf-autoclick"))
DISPLAY = os.getenv("DISPLAY", ":0")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
SCRAPE_TIMEOUT = int(os.getenv("SCRAPE_TIMEOUT", "60"))
PORT = int(os.getenv("SCRAPER_PORT", "8814"))

# Rotating proxies. One "ip:port:user:pass" per line (the Webshare export
# format), mounted from a Secret. Empty/missing file = no proxies, direct only.
PROXY_FILE = os.getenv("PROXY_FILE", "/etc/url-scraper/proxies.txt")
PROXY_ATTEMPTS = int(os.getenv("PROXY_ATTEMPTS", "3"))      # different proxies tried per request
PROXY_COOLDOWN = int(os.getenv("PROXY_COOLDOWN", "900"))    # seconds a blocked proxy rests
PROXY_DEAD_COOLDOWN = int(os.getenv("PROXY_DEAD_COOLDOWN", "3600"))  # one that would not load a page at all
PROXY_DEADLINE = int(os.getenv("PROXY_DEADLINE", "45"))     # no new attempt starts after this

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)


# --------------------------------------------------------------------------- #
# Proxy pool
# --------------------------------------------------------------------------- #

def _load_proxies(path: str) -> list:
    proxies = []
    try:
        for line in Path(path).read_text().splitlines():
            parts = line.strip().split(":")
            if len(parts) == 4 and parts[1].isdigit():
                proxies.append({"host": parts[0], "port": int(parts[1]), "user": parts[2], "password": parts[3]})
    except FileNotFoundError:
        pass
    random.shuffle(proxies)  # so the five pods don't walk the list in lockstep
    return proxies


class ProxyPool:
    """Round-robin over the proxy list, skipping any proxy that is cooling down
    after it was blocked or failed to connect."""

    def __init__(self, proxies: list):
        self.proxies = proxies
        self._next = 0
        self._resting_until: Dict[str, float] = {}
        self._lock = threading.Lock()

    @staticmethod
    def key(p: dict) -> str:
        return f"{p['host']}:{p['port']}"

    def take(self, n: int) -> list:
        """The next n proxies that are not resting, each one different."""
        picked = []
        now = time.time()
        with self._lock:
            for _ in range(len(self.proxies)):
                if len(picked) >= n:
                    break
                p = self.proxies[self._next % len(self.proxies)]
                self._next += 1
                if self._resting_until.get(self.key(p), 0) <= now:
                    picked.append(p)
        return picked

    def rest(self, p: dict, seconds: int = PROXY_COOLDOWN) -> None:
        with self._lock:
            self._resting_until[self.key(p)] = time.time() + seconds

    def stats(self) -> dict:
        now = time.time()
        with self._lock:
            resting = sum(1 for t in self._resting_until.values() if t > now)
        return {"total": len(self.proxies), "resting": resting}


proxy_pool = ProxyPool(_load_proxies(PROXY_FILE))


class ProxyForwarder:
    """Local, credential-free proxy in front of one upstream proxy.

    Chrome's --proxy-server cannot carry a username/password, so Chrome talks
    to this listener on 127.0.0.1 and every request is passed upstream with the
    Proxy-Authorization header added. CONNECT tunnels (all https) are piped
    through untouched after that; plain http is forced to Connection: close so
    a reused connection never reaches upstream without the header.
    """

    def __init__(self, proxy: dict):
        self.proxy = proxy
        token = base64.b64encode(f"{proxy['user']}:{proxy['password']}".encode()).decode()
        self._auth = f"Proxy-Authorization: Basic {token}\r\n".encode()
        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(64)
        self.url = f"http://127.0.0.1:{self._sock.getsockname()[1]}"
        threading.Thread(target=self._accept, daemon=True).start()

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._sock.accept()
            except OSError:
                return  # closed
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: socket.socket) -> None:
        upstream = None
        try:
            client.settimeout(30)
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = client.recv(65536)
                if not chunk:
                    return
                head += chunk
                if len(head) > 1 << 20:
                    return
            head, rest = head.split(b"\r\n\r\n", 1)
            lines = head.split(b"\r\n")
            kept = [l for l in lines[1:] if not l.lower().startswith((b"proxy-authorization:", b"proxy-connection:", b"connection:"))]
            is_connect = lines[0].upper().startswith(b"CONNECT ")
            if not is_connect:
                kept.append(b"Connection: close")
            new_head = b"\r\n".join([lines[0]] + kept) + b"\r\n" + self._auth + b"\r\n"
            upstream = socket.create_connection((self.proxy["host"], self.proxy["port"]), timeout=15)
            upstream.sendall(new_head + rest)
            self._pipe(client, upstream)
        except Exception:
            pass
        finally:
            for s in (client, upstream):
                if s:
                    try:
                        s.close()
                    except Exception:
                        pass

    @staticmethod
    def _pipe(a: socket.socket, b: socket.socket) -> None:
        a.settimeout(None)
        b.settimeout(None)
        socks = [a, b]
        while True:
            readable, _, broken = select.select(socks, [], socks, 120)
            if broken or not readable:
                return
            for s in readable:
                data = s.recv(65536)
                if not data:
                    return
                (b if s is a else a).sendall(data)


# What a blocked page looks like once loaded. The navigation status comes from
# the Navigation Timing entry; the text checks catch challenge pages that are
# served with 200.
BLOCK_CHECK_JS = r"""
JSON.stringify((() => {
  const nav = performance.getEntriesByType('navigation')[0];
  const status = nav && nav.responseStatus ? nav.responseStatus : 0;
  const title = (document.title || '').toLowerCase();
  const text = ((document.body && document.body.innerText) || '').slice(0, 3000).toLowerCase();
  const url = window.location.href;
  let reason = '';
  if (url.startsWith('chrome-error://')) reason = 'page failed to load';
  else if (status === 403 || status === 429 || status === 407 || status === 503) reason = 'HTTP ' + status;
  else if (title.includes('just a moment') || title.includes('attention required')) reason = 'cloudflare challenge';
  else if (title.includes('access denied') || text.startsWith('access denied')) reason = 'access denied';
  else if (text.includes('press & hold') || text.includes('are you a robot') || text.includes('verify you are human')) reason = 'bot challenge';
  else if (text.length < 400 && (text.includes('captcha') || text.includes('blocked'))) reason = 'blocked';
  return { status, reason };
})())
"""


def _blocked_reason(send_cdp) -> str:
    """'' when the loaded page looks like real content, else why it doesn't."""
    try:
        res = send_cdp("Runtime.evaluate", {"expression": BLOCK_CHECK_JS, "returnByValue": True})
        info = json.loads(res.get("result", {}).get("result", {}).get("value", "{}") or "{}")
        return info.get("reason", "") or ""
    except Exception:
        return ""


def with_proxy_rotation(fn, *args, **kwargs) -> dict:
    """Run a Chrome job through rotating proxies.

    Each request starts on the next proxy in the rotation. A blocked page, an
    error or a dead proxy rests that proxy and retries on a different one, up to
    PROXY_ATTEMPTS proxies, then once directly from the pod's own IP. No new
    attempt starts after PROXY_DEADLINE seconds, so callers' timeouts hold.
    """
    started = time.time()
    plan = proxy_pool.take(PROXY_ATTEMPTS) + [None]
    last = {"success": False, "error": "no attempt made"}
    tried = []
    for proxy in plan:
        if tried and time.time() - started > PROXY_DEADLINE:
            break
        fwd = ProxyForwarder(proxy) if proxy else None
        try:
            result = fn(*args, proxy_server=fwd.url if fwd else None, **kwargs)
        finally:
            if fwd:
                fwd.close()
        label = ProxyPool.key(proxy) if proxy else "direct"
        reason = result.get("blocked") or ("" if result.get("success") else result.get("error", "failed"))
        tried.append({"via": label, "result": reason or "ok"})
        if not reason:
            result["proxy"] = label
            result["attempts"] = tried
            return result
        print(f"[proxy] {label} -> {reason}; rotating", flush=True)
        if proxy:
            # Chrome's own error page: usually a dead proxy, so rest it longer.
            proxy_pool.rest(proxy, PROXY_DEAD_COOLDOWN if reason == "page failed to load" else PROXY_COOLDOWN)
        # A blocked page still carries content; keep the last one so callers
        # get what the old code returned if every route is blocked.
        if result.get("success") or not last.get("success"):
            last = result
    last["attempts"] = tried
    return last

_stats = {"processed": 0, "errors": 0, "active": 0, "started_at": time.time()}


# --------------------------------------------------------------------------- #
# Helper: find free port
# --------------------------------------------------------------------------- #

def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


# --------------------------------------------------------------------------- #
# Default selectors (when none provided)
# --------------------------------------------------------------------------- #

DEFAULT_SELECTORS = [
    {"name": "source_title", "selector": "title", "js_query": "document.title", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_content", "selector": "article", "js_query": "(() => { let el = document.querySelector('article') || document.querySelector('.article-body, .post-content, .entry-content, [role=main], main'); return el ? el.innerText : document.body.innerText.substring(0,50000); })()", "is_multiple_value": False, "remove_selector": ["script","style","nav","header","footer","aside","ins","iframe"]},
    {"name": "source_author", "selector": "meta[name='author']", "js_query": "document.querySelector('meta[name=\"author\"]')?.content || ''", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_published_date", "selector": "meta[property='article:published_time']", "js_query": "document.querySelector('meta[property=\"article:published_time\"]')?.content || document.querySelector('time[datetime]')?.getAttribute('datetime') || ''", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_featured_image", "selector": "meta[property='og:image']", "js_query": "document.querySelector('meta[property=\"og:image\"]')?.content || ''", "is_multiple_value": False, "remove_selector": []},
    {"name": "source_excerpt", "selector": "meta[name='description']", "js_query": "document.querySelector('meta[name=\"description\"]')?.content || document.querySelector('meta[property=\"og:description\"]')?.content || ''", "is_multiple_value": False, "remove_selector": []},
]


# --------------------------------------------------------------------------- #
# Build extraction JS from selectors
# --------------------------------------------------------------------------- #

def build_extraction_js(selectors: list, remove_tags: list = None) -> str:
    """Build JavaScript that extracts data using the provided selectors."""
    selectors_json = json.dumps(selectors)
    # ponytail: remove_tags strips entire HTML elements globally before extraction
    remove_tags_js = ""
    if remove_tags:
        tag_selector = ", ".join(remove_tags)
        remove_tags_js = f"document.querySelectorAll('{tag_selector}').forEach(el => el.remove());"
    return f"""
    (() => {{
        {remove_tags_js}
        const selectors = {selectors_json};
        const results = [];
        for (const sel of selectors) {{
            const result = {{ name: sel.name, selector: sel.selector, value: null }};
            try {{
                if (sel.remove_selector && sel.remove_selector.length > 0) {{
                    sel.remove_selector.forEach(rs => {{
                        document.querySelectorAll(rs).forEach(el => el.remove());
                    }});
                }}
                if (sel.js_query) {{
                    try {{ result.value = eval(sel.js_query); }} catch(e) {{}}
                }}
                if (!result.value) {{
                    if (sel.is_multiple_value) {{
                        const els = document.querySelectorAll(sel.selector);
                        result.value = Array.from(els).map(el => 
                            el.getAttribute('content') || el.getAttribute('href') || el.getAttribute('src') || el.innerText.trim()
                        );
                    }} else {{
                        const el = document.querySelector(sel.selector);
                        if (el) result.value = el.getAttribute('content') || el.getAttribute('href') || el.getAttribute('src') || el.innerText.trim();
                    }}
                }}
            }} catch(e) {{ result.error = e.message; }}
            results.push(result);
        }}
        return JSON.stringify(results);
    }})()
    """


# --------------------------------------------------------------------------- #
# Core: scrape with fresh chrome via CDP
# --------------------------------------------------------------------------- #

def scrape_url(target_url: str, selectors: list, remove_tags: list = None, proxy_server: str = None) -> dict:
    """Launch chrome, navigate, extract, kill. Returns parsed result."""
    
    if not selectors:
        selectors = DEFAULT_SELECTORS
    
    profile_dir = tempfile.mkdtemp(prefix="scrape_")
    cdp_port = _free_port()
    chrome_proc = None
    
    try:
        # 1. Launch chrome
        chrome_args = [
            CHROME_BIN,
            f"--user-data-dir={profile_dir}",
            f"--remote-debugging-port={cdp_port}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--headless=new",
        ]
        
        # Add cf-autoclick extension if exists
        if os.path.isdir(CF_AUTOCLICK_DIR):
            chrome_args.append(f"--load-extension={CF_AUTOCLICK_DIR}")
            # Can't use headless with extensions, switch to headed
            chrome_args = [a for a in chrome_args if a != "--headless=new"]
        
        if proxy_server:
            chrome_args.append(f"--proxy-server={proxy_server}")

        env = os.environ.copy()
        env["DISPLAY"] = DISPLAY
        
        chrome_proc = subprocess.Popen(
            chrome_args, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        
        # 2. Wait for CDP to be ready
        cdp_base = f"http://127.0.0.1:{cdp_port}"
        ready = False
        for _ in range(15):
            time.sleep(1)
            try:
                r = http_requests.get(f"{cdp_base}/json/version", timeout=2)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
        
        if not ready:
            return {"success": False, "error": "Chrome CDP not ready after 15s"}
        
        # 3. Get a page target (or create new tab)
        tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
        page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            # Create a new tab
            r = http_requests.put(f"{cdp_base}/json/new?about:blank", timeout=5)
            tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
            page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            return {"success": False, "error": "No page target available"}
        
        ws_url = page_tabs[0]["webSocketDebuggerUrl"]
        
        # 4. Connect via WebSocket and navigate
        import websocket
        ws = websocket.create_connection(ws_url, timeout=SCRAPE_TIMEOUT)
        msg_id = 1
        
        def send_cdp(method, params=None):
            nonlocal msg_id
            msg = {"id": msg_id, "method": method, "params": params or {}}
            ws.send(json.dumps(msg))
            msg_id += 1
            # Wait for response with matching id
            while True:
                resp = json.loads(ws.recv())
                if resp.get("id") == msg_id - 1:
                    return resp
                # Also handle events (just skip)
        
        # Enable Page events
        send_cdp("Page.enable")
        
        # Navigate
        send_cdp("Page.navigate", {"url": target_url})
        
        # Wait for load (simple approach: just wait)
        time.sleep(8)
        blocked = _blocked_reason(send_cdp)
        
        # 5. Extract data using JS
        extraction_js = build_extraction_js(selectors, remove_tags)
        result = send_cdp("Runtime.evaluate", {
            "expression": extraction_js,
            "returnByValue": True,
        })
        
        scraped_raw = result.get("result", {}).get("result", {}).get("value", "[]")
        
        # 6. Get page HTML
        html_result = send_cdp("Runtime.evaluate", {
            "expression": "document.documentElement.outerHTML",
            "returnByValue": True,
        })
        page_html = html_result.get("result", {}).get("result", {}).get("value", "")
        
        # 7. Get page info
        info_result = send_cdp("Runtime.evaluate", {
            "expression": "JSON.stringify({title: document.title, url: window.location.href, domain: window.location.hostname})",
            "returnByValue": True,
        })
        page_info_raw = info_result.get("result", {}).get("result", {}).get("value", "{}")
        
        ws.close()
        
        # 8. Parse results
        try:
            scraped_data = json.loads(scraped_raw) if isinstance(scraped_raw, str) else scraped_raw
        except Exception:
            scraped_data = []
        
        try:
            page_info = json.loads(page_info_raw) if isinstance(page_info_raw, str) else page_info_raw
        except Exception:
            page_info = {}
        
        # Build variables dict
        variables = {}
        for item in (scraped_data if isinstance(scraped_data, list) else []):
            if isinstance(item, dict) and item.get("name"):
                variables[item["name"]] = item.get("value")
        
        variables["page_html"] = page_html
        if page_info:
            variables["__page_title"] = page_info.get("title", "")
            variables["__page_url"] = page_info.get("url", target_url)
        
        return {
            "success": True,
            "blocked": blocked,
            "data": {
                "variables": variables,
                "scraped_data": scraped_data,
                "page_info": page_info,
            }
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}
    
    finally:
        # ALWAYS cleanup
        if chrome_proc and chrome_proc.poll() is None:
            try:
                chrome_proc.terminate()
                chrome_proc.wait(timeout=5)
            except Exception:
                chrome_proc.kill()
        shutil.rmtree(profile_dir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Core: screenshot with fresh chrome via CDP
# --------------------------------------------------------------------------- #

def fetch_binary_url(target_url: str, wait: int = 4, proxy_server: str = None) -> dict:
    """Fetch a URL's raw bytes through Chrome and return them base64 encoded.

    Publishers that drop plain HTTP clients from datacentre IPs still serve a
    real browser carrying the cf-autoclick extension, so featured images have
    to come down the same path as the article HTML. Chrome navigates to the
    asset, then an in-page fetch() of the same origin returns the bytes.
    """
    profile_dir = tempfile.mkdtemp(prefix="fetchbin_")
    cdp_port = _free_port()
    chrome_proc = None

    try:
        chrome_args = [
            CHROME_BIN,
            f"--user-data-dir={profile_dir}",
            f"--remote-debugging-port={cdp_port}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1280,900",
        ]
        if os.path.isdir(CF_AUTOCLICK_DIR):
            chrome_args.append(f"--load-extension={CF_AUTOCLICK_DIR}")
        else:
            chrome_args.append("--headless=new")

        if proxy_server:
            chrome_args.append(f"--proxy-server={proxy_server}")

        env = os.environ.copy()
        env["DISPLAY"] = DISPLAY
        chrome_proc = subprocess.Popen(
            chrome_args, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        cdp_base = f"http://127.0.0.1:{cdp_port}"
        ready = False
        for _ in range(15):
            time.sleep(1)
            try:
                if http_requests.get(f"{cdp_base}/json/version", timeout=2).status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
        if not ready:
            return {"success": False, "error": "Chrome CDP not ready after 15s"}

        tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
        page_tabs = [t for t in tabs if t.get("type") == "page"]
        if not page_tabs:
            http_requests.put(f"{cdp_base}/json/new?about:blank", timeout=5)
            tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
            page_tabs = [t for t in tabs if t.get("type") == "page"]
        if not page_tabs:
            return {"success": False, "error": "No page target available"}

        import websocket
        ws = websocket.create_connection(page_tabs[0]["webSocketDebuggerUrl"], timeout=60)
        msg_id = 1

        def send_cdp(method, params=None):
            nonlocal msg_id
            msg = {"id": msg_id, "method": method, "params": params or {}}
            ws.send(json.dumps(msg))
            msg_id += 1
            while True:
                resp = json.loads(ws.recv())
                if resp.get("id") == msg_id - 1:
                    return resp

        send_cdp("Page.enable")
        send_cdp("Page.navigate", {"url": target_url})
        time.sleep(wait)

        # Same-origin fetch from inside the loaded page: cookies and any
        # Cloudflare clearance the navigation earned come along with it.
        js = """
        (async () => {
          try {
            const r = await fetch(window.location.href, { credentials: 'include' });
            if (!r.ok) return JSON.stringify({ error: 'HTTP ' + r.status });
            const buf = new Uint8Array(await r.arrayBuffer());
            let bin = '';
            const CHUNK = 0x8000;
            for (let i = 0; i < buf.length; i += CHUNK) {
              bin += String.fromCharCode.apply(null, buf.subarray(i, i + CHUNK));
            }
            return JSON.stringify({
              content_type: r.headers.get('content-type') || '',
              bytes: buf.length,
              b64: btoa(bin)
            });
          } catch (e) { return JSON.stringify({ error: String(e) }); }
        })()
        """
        res = send_cdp("Runtime.evaluate", {
            "expression": js, "awaitPromise": True, "returnByValue": True,
        })
        raw = res.get("result", {}).get("result", {}).get("value", "")
        try:
            ws.close()
        except Exception:
            pass

        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {}
        if not payload or payload.get("error"):
            return {"success": False,
                    "error": payload.get("error", "no data returned from page")}

        return {
            "success": True,
            "data": {
                "content_type": payload.get("content_type", ""),
                "bytes": payload.get("bytes", 0),
                "b64": payload.get("b64", ""),
                "url": target_url,
            },
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if chrome_proc:
            try:
                chrome_proc.terminate()
                chrome_proc.wait(timeout=5)
            except Exception:
                try:
                    chrome_proc.kill()
                except Exception:
                    pass
        shutil.rmtree(profile_dir, ignore_errors=True)


def screenshot_url(target_url: str, full_page: bool = True, width: int = 1920, height: int = 1080, wait: int = 5, proxy_server: str = None) -> dict:
    """Launch chrome, navigate, take screenshot, kill. Returns base64 PNG."""
    
    profile_dir = tempfile.mkdtemp(prefix="screenshot_")
    cdp_port = _free_port()
    chrome_proc = None
    
    try:
        # 1. Launch chrome
        chrome_args = [
            CHROME_BIN,
            f"--user-data-dir={profile_dir}",
            f"--remote-debugging-port={cdp_port}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            f"--window-size={width},{height}",
        ]
        
        # Add cf-autoclick extension if exists
        if os.path.isdir(CF_AUTOCLICK_DIR):
            chrome_args.append(f"--load-extension={CF_AUTOCLICK_DIR}")
        else:
            chrome_args.append("--headless=new")
        
        if proxy_server:
            chrome_args.append(f"--proxy-server={proxy_server}")

        env = os.environ.copy()
        env["DISPLAY"] = DISPLAY
        
        chrome_proc = subprocess.Popen(
            chrome_args, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        
        # 2. Wait for CDP
        cdp_base = f"http://127.0.0.1:{cdp_port}"
        ready = False
        for _ in range(15):
            time.sleep(1)
            try:
                r = http_requests.get(f"{cdp_base}/json/version", timeout=2)
                if r.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
        
        if not ready:
            return {"success": False, "error": "Chrome CDP not ready after 15s"}
        
        # 3. Get page target
        tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
        page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            http_requests.put(f"{cdp_base}/json/new?about:blank", timeout=5)
            tabs = http_requests.get(f"{cdp_base}/json", timeout=5).json()
            page_tabs = [t for t in tabs if t.get("type") == "page"]
        
        if not page_tabs:
            return {"success": False, "error": "No page target available"}
        
        ws_url = page_tabs[0]["webSocketDebuggerUrl"]
        
        # 4. Connect via WebSocket and navigate
        import websocket
        ws = websocket.create_connection(ws_url, timeout=60)
        msg_id = 1
        
        def send_cdp(method, params=None):
            nonlocal msg_id
            msg = {"id": msg_id, "method": method, "params": params or {}}
            ws.send(json.dumps(msg))
            msg_id += 1
            while True:
                resp = json.loads(ws.recv())
                if resp.get("id") == msg_id - 1:
                    return resp
        
        # Set viewport
        send_cdp("Emulation.setDeviceMetricsOverride", {
            "width": width, "height": height,
            "deviceScaleFactor": 1, "mobile": False,
        })
        
        send_cdp("Page.enable")
        send_cdp("Page.navigate", {"url": target_url})
        
        # Wait for page to load
        time.sleep(wait)
        blocked = _blocked_reason(send_cdp)
        
        # 5. Take screenshot
        if full_page:
            # Get full page dimensions
            metrics = send_cdp("Page.getLayoutMetrics")
            content_size = metrics.get("result", {}).get("contentSize", {})
            page_width = content_size.get("width", width)
            page_height = content_size.get("height", height)
            
            # Set viewport to full page
            send_cdp("Emulation.setDeviceMetricsOverride", {
                "width": int(page_width), "height": int(page_height),
                "deviceScaleFactor": 1, "mobile": False,
            })
            time.sleep(1)
        
        screenshot_result = send_cdp("Page.captureScreenshot", {
            "format": "png",
            "quality": 90,
        })
        
        screenshot_data = screenshot_result.get("result", {}).get("data", "")
        
        # Get page info + text content
        info_result = send_cdp("Runtime.evaluate", {
            "expression": """JSON.stringify({
                title: document.title,
                url: window.location.href,
                text_content: document.body.innerText,
                meta_description: (document.querySelector('meta[name="description"]') || {}).content || '',
                meta_keywords: (document.querySelector('meta[name="keywords"]') || {}).content || '',
                og_title: (document.querySelector('meta[property="og:title"]') || {}).content || '',
                og_description: (document.querySelector('meta[property="og:description"]') || {}).content || '',
                og_image: (document.querySelector('meta[property="og:image"]') || {}).content || '',
                h1: Array.from(document.querySelectorAll('h1')).map(e => e.innerText).join(' | '),
                h2s: Array.from(document.querySelectorAll('h2')).map(e => e.innerText),
                links_count: document.querySelectorAll('a[href]').length,
                images_count: document.querySelectorAll('img').length,
                word_count: document.body.innerText.split(/\\s+/).filter(w => w.length > 0).length
            })""",
            "returnByValue": True,
        })
        page_info_raw = info_result.get("result", {}).get("result", {}).get("value", "{}")
        
        ws.close()
        
        try:
            page_info = json.loads(page_info_raw)
        except Exception:
            page_info = {}
        
        return {
            "success": True,
            "blocked": blocked,
            "data": {
                "screenshot_base64": screenshot_data,
                "page_title": page_info.get("title", ""),
                "page_url": page_info.get("url", target_url),
                "text_content": page_info.get("text_content", ""),
                "meta": {
                    "description": page_info.get("meta_description", ""),
                    "keywords": page_info.get("meta_keywords", ""),
                    "og_title": page_info.get("og_title", ""),
                    "og_description": page_info.get("og_description", ""),
                    "og_image": page_info.get("og_image", ""),
                },
                "structure": {
                    "h1": page_info.get("h1", ""),
                    "h2s": page_info.get("h2s", []),
                    "links_count": page_info.get("links_count", 0),
                    "images_count": page_info.get("images_count", 0),
                    "word_count": page_info.get("word_count", 0),
                },
                "width": width,
                "height": page_height if full_page else height,
                "full_page": full_page,
            }
        }
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    
    finally:
        if chrome_proc and chrome_proc.poll() is None:
            try:
                chrome_proc.terminate()
                chrome_proc.wait(timeout=5)
            except Exception:
                chrome_proc.kill()
        shutil.rmtree(profile_dir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Flask API
# --------------------------------------------------------------------------- #

@app.route("/url-scraper-service/api/v1/scrape/", methods=["POST"])
def scrape():
    _stats["active"] += 1
    try:
        body = flask_request.get_json(force=True)
        target_url = body.get("target_url", "")
        selectors = body.get("selectors", [])
        remove_tags = body.get("remove_tags", ["figcaption"])
        
        if not target_url:
            return jsonify({"success": False, "error": "target_url required"}), 400
        
        future = executor.submit(with_proxy_rotation, scrape_url, target_url, selectors, remove_tags)
        result = future.result(timeout=SCRAPE_TIMEOUT + 30)
        
        if result.get("success"):
            _stats["processed"] += 1
        else:
            _stats["errors"] += 1
        
        return jsonify(result)
    except Exception as e:
        _stats["errors"] += 1
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        _stats["active"] -= 1


@app.route("/url-scraper-service/api/v1/screenshot/", methods=["POST"])
def screenshot():
    """Take a screenshot of a URL. Returns base64 PNG or binary PNG file."""
    _stats["active"] += 1
    try:
        body = flask_request.get_json(force=True)
        target_url = body.get("target_url", "")
        full_page = body.get("full_page", True)
        width = body.get("width", 1920)
        height = body.get("height", 1080)
        wait = body.get("wait", 5)
        output = body.get("output", "base64")  # "base64" or "binary"

        if not target_url:
            return jsonify({"success": False, "error": "target_url required"}), 400

        future = executor.submit(with_proxy_rotation, screenshot_url, target_url, full_page, width, height, wait)
        result = future.result(timeout=SCRAPE_TIMEOUT + 30)

        if not result.get("success"):
            _stats["errors"] += 1
            return jsonify(result), 500

        _stats["processed"] += 1

        # Return as binary PNG file
        if output == "binary":
            import base64
            from flask import Response
            png_data = base64.b64decode(result["data"]["screenshot_base64"])
            return Response(
                png_data,
                mimetype="image/png",
                headers={
                    "Content-Disposition": f"inline; filename=screenshot.png",
                    "X-Page-Title": result["data"].get("page_title", ""),
                    "X-Page-URL": result["data"].get("page_url", ""),
                }
            )

        # Return as JSON with base64
        return jsonify(result)
    except Exception as e:
        _stats["errors"] += 1
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        _stats["active"] -= 1


@app.route("/url-scraper-service/api/v1/fetch-binary/", methods=["POST"])
def fetch_binary():
    """Fetch a binary asset (image, etc.) through Chrome. Returns base64."""
    _stats["active"] += 1
    try:
        body = flask_request.get_json(force=True)
        target_url = body.get("target_url", "")
        wait = int(body.get("wait", 4))
        if not target_url:
            return jsonify({"success": False, "error": "target_url required"}), 400

        future = executor.submit(with_proxy_rotation, fetch_binary_url, target_url, wait)
        result = future.result(timeout=SCRAPE_TIMEOUT + 30)

        if result.get("success"):
            _stats["processed"] += 1
        else:
            _stats["errors"] += 1
        return jsonify(result)
    except Exception as e:
        _stats["errors"] += 1
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        _stats["active"] -= 1


@app.route("/url-scraper-service/api/v1/health/", methods=["GET"])
def health():
    return jsonify({
        "service": "url-scraper",
        "status": "healthy",
        "architecture": "fresh-chrome-cdp-per-request",
        "stats": {
            "processed": _stats["processed"],
            "errors": _stats["errors"],
            "active": _stats["active"],
            "max_concurrent": MAX_CONCURRENT,
            "uptime": int(time.time() - _stats["started_at"]),
        },
        "chrome": CHROME_BIN,
        "extension": CF_AUTOCLICK_DIR,
        "proxies": proxy_pool.stats(),
    })


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT)
    parser.add_argument("--timeout", type=int, default=SCRAPE_TIMEOUT)
    parser.add_argument("--chrome", type=str, default=CHROME_BIN)
    parser.add_argument("--extension", type=str, default=CF_AUTOCLICK_DIR)
    args = parser.parse_args()
    
    CHROME_BIN = args.chrome
    CF_AUTOCLICK_DIR = args.extension
    MAX_CONCURRENT = args.max_concurrent
    SCRAPE_TIMEOUT = args.timeout
    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)
    
    print(f"🚀 URL Scraper starting on port {args.port}")
    print(f"   Chrome: {CHROME_BIN}")
    print(f"   Extension: {CF_AUTOCLICK_DIR}")
    print(f"   Concurrent: {MAX_CONCURRENT}")
    print(f"   Timeout: {SCRAPE_TIMEOUT}s")
    print(f"   Proxies: {len(proxy_pool.proxies)} from {PROXY_FILE}")
    
    app.run(host="0.0.0.0", port=args.port, threaded=True)
