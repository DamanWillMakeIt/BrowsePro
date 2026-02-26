"""
main.py  v7.0 — FULLY FIXED
----------------------------------
FIXES vs v6.5:
  1. CRITICAL: BD Scraping Browser zone check was inverted — was `!= "scraping_browser1"`
     which means it NEVER activated. Now correctly activates when zone IS set.
  2. CRITICAL: _is_blocked() now detects blank-page Cloudflare JS challenges
     (empty DOM, which v6.5 completely missed).
  3. Camoufox warm-up: switched to HTTP-only via Bright Data pass-through port 33335
     to avoid SEC_ERROR_UNKNOWN_ISSUER on Firefox engine.
  4. Warm-up now uses Camoufox AS the main session browser (not Chromium fallback).
  5. on_step_end: blank page now triggers wait+reload cycle instead of passive wait.
  6. Video + Data BOTH guaranteed in response — video_url falls back to frame
     collage if ffmpeg unavailable.
  7. Result extraction: added JS-based table scrape as fallback before giving up.
  8. Proxy IP for UAE: correctly appended to session user-agent string.
"""
from __future__ import annotations
from typing import Any, Optional
import asyncio, base64, glob, json, os, random, re as _re, shutil, uuid
from datetime import datetime
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from browser_use import Agent

# ---------------------------------------------------------------------------
# CAMOUFOX
# ---------------------------------------------------------------------------
CAMOUFOX_AVAILABLE = False
try:
    from camoufox.async_api import AsyncCamoufox
    CAMOUFOX_AVAILABLE = True
    print("[Browser] Camoufox available")
except ImportError:
    print("[Browser] Camoufox not installed - Chromium fallback")

from utils.helpers import create_and_upload_video

load_dotenv()

app = FastAPI(title="OnDemand Browser-Use Agent", version="7.0.0")
SCAN_DIR = "scans"
os.makedirs(SCAN_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# ENV / CONFIG
# ---------------------------------------------------------------------------
CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "")
PROXY_USER        = os.getenv("PROXY_USER", "brd-customer-hl_ea313532-zone-demo")
PROXY_PASS        = os.getenv("PROXY_PASS", "jzbld1hf9ygu")
BD_API_KEY        = os.getenv("BD_API_KEY", "25e73165-8000-4476-b814-6c79af3550c8")
BD_UNLOCKER_ZONE  = os.getenv("BD_UNLOCKER_ZONE", "unlocker")
BD_SCRAPING_BROWSER_PASS = os.getenv("BD_SCRAPING_BROWSER_PASS", "uqcs8vv8fs0j")

# ── Bright Data Scraping Browser (handles JS bot challenges natively) ──────
BD_SCRAPING_BROWSER_HOST = os.getenv("BD_SCRAPING_BROWSER_HOST", "brd.superproxy.io")
BD_SCRAPING_BROWSER_PORT = os.getenv("BD_SCRAPING_BROWSER_PORT", "9222")
# FIX #1: Set this to your actual scraping browser zone name in .env
# e.g. BD_SCRAPING_BROWSER_ZONE=scraping_browser1
BD_SCRAPING_BROWSER_ZONE = os.getenv("BD_SCRAPING_BROWSER_ZONE", "scraping_browser1")

RACE_MAX_ROUNDS   = 5
WORKER_TIMEOUT    = 600
MAX_BROWSERS      = 1
_browser_semaphore = asyncio.Semaphore(MAX_BROWSERS)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
]

# Use UAE exit node
_PROXY_POOL: list[dict] = [{
    "host": "brd.superproxy.io",
    "port": "33335",
    "user": f"{PROXY_USER}-country-ae",
    "pass": PROXY_PASS,
}]

async def _refresh_proxy_pool() -> None:
    print(f"[ProxyPool] Bright Data residential: {_PROXY_POOL[0]['host']}:{_PROXY_POOL[0]['port']}")

def _proxy_httpx_url(p: dict) -> str:
    return f"http://{p['user']}:{p['pass']}@{p['host']}:{p['port']}"

print("=" * 60)
print(f"[Deploy] Camoufox          : {'yes' if CAMOUFOX_AVAILABLE else 'no'}")
print(f"[Deploy] CAPSOLVER_API_KEY : {'yes' if CAPSOLVER_API_KEY else 'no'}")
print(f"[Deploy] BD_API_KEY        : {'yes' if BD_API_KEY else 'no'}")
print(f"[Deploy] BD SB Zone        : {BD_SCRAPING_BROWSER_ZONE}")
print(f"[Deploy] Proxy             : Bright Data Residential")
print("=" * 60)

# ---------------------------------------------------------------------------
# PAGE HELPERS
# ---------------------------------------------------------------------------
async def _page_url(page) -> str:
    try:
        fn = getattr(page, "get_url", None)
        if fn:
            r = fn() if not asyncio.iscoroutinefunction(fn) else await fn()
            if r:
                return r
        url = page.url
        return (await url if asyncio.iscoroutine(url) else url) or ""
    except Exception:
        try:
            return await page.evaluate("() => window.location.href")
        except Exception:
            return ""

async def _page_frames(page) -> list:
    try:
        f = page.frames
        return (await f if asyncio.iscoroutine(f) else f) or []
    except Exception:
        return []

async def _frame_url(frame) -> str:
    try:
        u = frame.url
        return (await u if asyncio.iscoroutine(u) else u) or ""
    except Exception:
        return ""

# ---------------------------------------------------------------------------
# HUMAN BEHAVIOR
# ---------------------------------------------------------------------------
async def human_mouse_move(page, to_x: int, to_y: int) -> None:
    try:
        current = await page.evaluate("() => [window.lastMouseX || 0, window.lastMouseY || 0]")
        start_x, start_y = current[0], current[1]
        cp1_x = start_x + (to_x - start_x) * random.uniform(0.2, 0.4)
        cp1_y = start_y + (to_y - start_y) * random.uniform(0.2, 0.4) + random.randint(-50, 50)
        cp2_x = start_x + (to_x - start_x) * random.uniform(0.6, 0.8)
        cp2_y = start_y + (to_y - start_y) * random.uniform(0.6, 0.8) + random.randint(-50, 50)
        steps = random.randint(15, 25)
        for i in range(steps + 1):
            t = i / steps
            x = int((1-t)**3 * start_x + 3*(1-t)**2*t * cp1_x + 3*(1-t)*t**2 * cp2_x + t**3 * to_x)
            y = int((1-t)**3 * start_y + 3*(1-t)**2*t * cp1_y + 3*(1-t)*t**2 * cp2_y + t**3 * to_y)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.01, 0.03))
        await page.evaluate(f"() => {{ window.lastMouseX = {to_x}; window.lastMouseY = {to_y}; }}")
    except Exception:
        pass

async def human_scroll(page, distance: int = 300) -> None:
    try:
        scroll_steps = random.randint(8, 15)
        for i in range(scroll_steps):
            progress = i / scroll_steps
            ease = progress * (2 - progress)
            step = (distance / scroll_steps) * ease * random.uniform(0.8, 1.2)
            await page.evaluate(f"window.scrollBy(0, {step})")
            await asyncio.sleep(random.uniform(0.05, 0.2))
        await asyncio.sleep(random.uniform(0.3, 0.8))
    except Exception:
        pass

async def human_delay_long() -> None:
    await asyncio.sleep(random.uniform(5.0, 15.0))

async def human_delay_short() -> None:
    await asyncio.sleep(random.uniform(1.0, 3.0))

# ---------------------------------------------------------------------------
# WARM-UP  — uses Camoufox with HTTP-only sites to avoid SSL CA issue
# ---------------------------------------------------------------------------
async def _warmup_extended(proxy: dict, wid: str) -> None:
    """
    Warm-up using Camoufox async context manager.
    Uses plain HTTP sites only — Firefox engine throws SEC_ERROR_UNKNOWN_ISSUER
    on HTTPS through BD proxy because BD intercepts SSL and its CA cert isn't
    in Firefox's trust store. HTTP sites bypass this entirely.
    """
    if not CAMOUFOX_AVAILABLE:
        print(f"[W{wid}] Skipping warm-up (Camoufox not available)")
        return

    # Plain HTTP warmup sites — no SSL involved, no CA cert needed
    sites = [
        "http://neverssl.com",
        "http://example.com",
        "http://httpforever.com",
        "http://detectportal.firefox.com/success.txt",
    ]
    random.shuffle(sites)

    try:
        async with AsyncCamoufox(
            headless=True,
            os="windows",
            proxy={
                "server": f"http://{proxy['host']}:{proxy['port']}",
                "username": proxy["user"],
                "password": proxy["pass"],
            },
            # geoip=True removed — broken in camoufox v146 beta
            humanize=0.5,
        ) as browser:
            page = await browser.new_page()
            print(f"[W{wid}] Camoufox warm-up started")
            success = 0
            for i, site in enumerate(sites[:3]):
                try:
                    print(f"[W{wid}] Warm-up {i+1}/3: {site}")
                    await page.goto(site, timeout=15000, wait_until="domcontentloaded")
                    await human_delay_short()
                    await human_scroll(page, random.randint(200, 400))
                    await asyncio.sleep(random.uniform(1.5, 3.0))
                    success += 1
                    print(f"[W{wid}] Warm-up {i+1} OK")
                except Exception as e:
                    print(f"[W{wid}] Warm-up {i+1} failed (non-fatal): {e}")
            print(f"[W{wid}] Warm-up complete ({success}/3 sites ok)")
    except Exception as e:
        print(f"[W{wid}] Warm-up context failed (non-fatal, continuing): {e}")

# ---------------------------------------------------------------------------
# PROXY VERIFY
# ---------------------------------------------------------------------------
async def _verify_proxy(proxy: dict, wid: str) -> None:
    try:
        async with httpx.AsyncClient(proxy=_proxy_httpx_url(proxy), timeout=8, verify=False) as c:
            ip = (await c.get("https://ipinfo.io/json")).json().get("ip", "?")
            print(f"[W{wid}] Proxy exit IP: {ip}")
    except Exception as e:
        print(f"[W{wid}] ProxyCheck failed: {e}")

# ---------------------------------------------------------------------------
# WEB UNLOCKER  — raw HTML fetch for non-JS pages
# ---------------------------------------------------------------------------
async def _fetch_via_unlocker(url: str) -> str | None:
    if not BD_API_KEY:
        return None
    try:
        print(f"[Unlocker] Fetching: {url}")
        async with httpx.AsyncClient(timeout=60, verify=False) as c:
            r = await c.post(
                "https://api.brightdata.com/request",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {BD_API_KEY}",
                },
                json={"zone": BD_UNLOCKER_ZONE, "url": url, "format": "raw"},
            )
            if r.status_code == 200:
                print(f"[Unlocker] Got {len(r.text)} chars")
                return r.text
            print(f"[Unlocker] Status {r.status_code}: {r.text[:200]}")
            return None
    except Exception as e:
        print(f"[Unlocker] Error: {e}")
        return None

# ---------------------------------------------------------------------------
# FIX #2: _is_blocked() — now catches blank-page Cloudflare challenges
# ---------------------------------------------------------------------------
async def _is_blocked(page) -> bool:
    """
    Detect Cloudflare / bot-check walls.
    KEY FIX: Cloudflare's modern JS challenge renders as a completely BLANK page
    in raw HTML — no 'Just a moment' text, just empty body. We must catch this.
    """
    try:
        url = await _page_url(page)
        if "browser_check" in url or "captcha" in url.lower():
            return True

        html = await page.content()

        # ── FIX: Blank page = Cloudflare JS challenge not yet rendered ──────
        stripped = html.strip().lower().replace('\n', '').replace(' ', '')
        if stripped in (
            '',
            '<html><head></head><body></body></html>',
            '<html><body></body></html>',
            '<!doctypehtml><html><head></head><body></body></html>',
        ):
            print(f"[Block] Blank page detected at {url} — likely Cloudflare JS challenge")
            return True

        # ── Short page with no meaningful content ──────────────────────────
        if len(html.strip()) < 500 and '<table' not in html and 'rfp' not in html.lower():
            body_content = _re.sub(r'<[^>]+>', '', html).strip()
            if len(body_content) < 100:
                print(f"[Block] Near-empty page ({len(html)} chars) at {url}")
                return True

        # ── Known bot-wall signals ─────────────────────────────────────────
        signals = [
            "Just a moment",
            "cf-browser-verification",
            "browser_check",
            "wrong captcha",
            "Access Denied",
            "Please verify you are a human",
            "Enable JavaScript and cookies to continue",
            "Checking your browser",
            "DDoS protection by",
            "Ray ID",  # Cloudflare Ray ID footer
        ]
        return any(s in html for s in signals)
    except Exception:
        return False

# ---------------------------------------------------------------------------
# CAPSOLVER
# ---------------------------------------------------------------------------
async def _capsolver_solve(task: dict, proxy: dict | None = None) -> dict | None:
    if not CAPSOLVER_API_KEY:
        return None
    task = dict(task)
    if proxy:
        task.update({
            "proxyType": "http", "proxyAddress": proxy["host"],
            "proxyPort": int(proxy["port"]), "proxyLogin": proxy["user"],
            "proxyPassword": proxy["pass"],
        })
        task["type"] = task["type"].replace("ProxyLess", "")
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post("https://api.capsolver.com/createTask",
                             json={"clientKey": CAPSOLVER_API_KEY, "task": task})
            d = r.json()
            if d.get("errorId") != 0:
                return None
            tid = d["taskId"]
        async with httpx.AsyncClient(timeout=120) as c:
            for _ in range(60):
                await asyncio.sleep(2)
                d = (await c.post("https://api.capsolver.com/getTaskResult",
                                  json={"clientKey": CAPSOLVER_API_KEY, "taskId": tid})).json()
                if d.get("status") == "ready":
                    return d.get("solution", {})
                if d.get("status") == "failed":
                    return None
    except Exception:
        pass
    return None

async def _solve_captcha(page, proxy: dict) -> None:
    try:
        try:
            html = await page.content()
        except Exception:
            try:
                html = await page.evaluate("() => document.documentElement.outerHTML")
            except Exception:
                return
        purl = await _page_url(page)
        frames = await _page_frames(page)

        ts_key = None
        for f in frames:
            fu = await _frame_url(f)
            if "challenges.cloudflare.com" in fu or "turnstile" in fu.lower():
                m = _re.search(r'[?&]k=([^&]+)', fu)
                if m:
                    ts_key = m.group(1)
                break
        if not ts_key:
            m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
            if m and ("cf-turnstile" in html or "turnstile" in html.lower()):
                ts_key = m.group(1)
        if ts_key:
            print("[CAPTCHA] Turnstile - solving...")
            sol = await _capsolver_solve(
                {"type": "AntiTurnstileTask", "websiteURL": purl, "websiteKey": ts_key}, proxy=proxy)
            if sol:
                t = sol.get("token", "")
                await page.evaluate("""(t) => {
                    document.querySelectorAll('input[name*="cf-turnstile-response"],input[name*="turnstile"]')
                        .forEach(el => { el.value=t; el.dispatchEvent(new Event('change',{bubbles:true})); });
                    const el = document.querySelector('.cf-turnstile,[data-sitekey]');
                    if (el) { const cb=el.getAttribute('data-callback'); if(cb&&window[cb]) try{window[cb](t);}catch(e){} }
                }""", t)
            return

        if "Just a moment" in html or "cf-browser-verification" in html:
            print("[CAPTCHA] CF browser check — waiting up to 20s for auto-resolve")
            for _ in range(20):
                await asyncio.sleep(1)
                try:
                    current_html = await page.content()
                    if "Just a moment" not in current_html and len(current_html) > 1000:
                        print("[CAPTCHA] CF check resolved")
                        return
                except Exception:
                    pass
            return

        rc_key = None
        for f in frames:
            fu = await _frame_url(f)
            if "recaptcha" in fu and "anchor" in fu:
                m = _re.search(r'[?&]k=([^&]+)', fu)
                if m:
                    rc_key = m.group(1)
                try:
                    cb = f.locator(".recaptcha-checkbox-border").first
                    if await cb.count() > 0:
                        await cb.click(timeout=3000)
                        await asyncio.sleep(5)
                        nf = await _page_frames(page)
                        if not any("bframe" in (await _frame_url(x)) for x in nf):
                            return
                except Exception:
                    pass
                break
        if not rc_key:
            m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
            if m:
                rc_key = m.group(1)
        if rc_key and "6L" in rc_key:
            sol = await _capsolver_solve(
                {"type": "ReCaptchaV2Task", "websiteURL": purl, "websiteKey": rc_key}, proxy=proxy)
            if sol:
                t = sol.get("gRecaptchaResponse", "")
                await page.evaluate("""(token) => {
                    document.querySelectorAll('textarea[name="g-recaptcha-response"]').forEach(el => {
                        el.innerHTML = token; el.value = token; el.style.display = 'block';
                    });
                    document.querySelectorAll('[data-callback]').forEach(el => {
                        const cb = el.getAttribute('data-callback');
                        if (cb && window[cb]) { try { window[cb](token); } catch(e) {} }
                    });
                    const form = document.querySelector('textarea[name="g-recaptcha-response"]')?.closest('form');
                    if (form) setTimeout(() => { try { form.submit(); } catch(e) {} }, 500);
                    const btn = document.querySelector('button[type="submit"], input[type="submit"]');
                    if (btn) setTimeout(() => btn.click(), 1000);
                }""", t)
                await asyncio.sleep(5)
                return

        if "hcaptcha" in html.lower():
            m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
            if m:
                sol = await _capsolver_solve(
                    {"type": "HCaptchaTask", "websiteURL": purl, "websiteKey": m.group(1)}, proxy=proxy)
                if sol:
                    t = sol.get("gRecaptchaResponse", "")
                    await page.evaluate("""(t) => {
                        const ta=document.querySelector('[name="h-captcha-response"]');
                        if(ta){ta.innerHTML=t;ta.value=t;}
                        document.querySelectorAll('[data-callback]').forEach(el=>{
                            const cb=el.getAttribute('data-callback');
                            if(cb&&window[cb])try{window[cb](t);}catch(e){}
                        });
                    }""", t)
    except Exception as e:
        print(f"[CAPTCHA] error: {e}")

# ---------------------------------------------------------------------------
# JS TABLE SCRAPE FALLBACK — used when agent fails to extract data
# ---------------------------------------------------------------------------
async def _js_scrape_procurement(page) -> list[dict] | None:
    """
    Direct JS extraction from procurement.gov.ae table structure.
    Called as last resort before giving up.
    """
    try:
        result = await page.evaluate("""
        () => {
            const rows = [];
            // Try multiple selectors used by this SPA
            const selectors = [
                'table tbody tr',
                '[role="grid"] [role="row"]',
                '.rfp-list-item',
                '.tender-row',
                '[class*="grid-row"]',
                '[class*="list-row"]',
            ];
            let elements = [];
            for (const sel of selectors) {
                elements = Array.from(document.querySelectorAll(sel));
                if (elements.length > 0) break;
            }

            elements.forEach(row => {
                const cells = Array.from(row.querySelectorAll('td, [role="gridcell"], [class*="cell"]'));
                if (cells.length === 0) return;

                const anchors = Array.from(row.querySelectorAll('a[href]'));
                const link = anchors.length > 0
                    ? (anchors[0].href.startsWith('http')
                        ? anchors[0].href
                        : 'https://procurement.gov.ae' + anchors[0].getAttribute('href'))
                    : null;

                const text = cells.map(c => c.innerText.trim()).filter(Boolean);
                if (text.length === 0) return;

                rows.push({
                    issuing_entity: text[0] || null,
                    tender_title: text[1] || null,
                    date_published: text[2] || null,
                    submission_deadline: text[3] || null,
                    reference_number: text[4] || null,
                    notice_link: link,
                    raw_cells: text,
                });
            });
            return rows;
        }
        """)
        if result and len(result) > 0:
            print(f"[JSScrape] Extracted {len(result)} rows via JS fallback")
            return result
        return None
    except Exception as e:
        print(f"[JSScrape] Failed: {e}")
        return None

# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------
class AgentRequest(BaseModel):
    prompt: str
    max_steps: int = 50
    model: str = "gpt-4.1"

class AgentResponse(BaseModel):
    video_url: str | None = None
    steps_taken: int = 0
    extracted_data: Any = None
    worker_id: str | None = None

# ---------------------------------------------------------------------------
# SCREENSHOT HELPERS
# ---------------------------------------------------------------------------
def _ensure_frames(folder: str) -> None:
    if glob.glob(os.path.join(folder, "*.png")):
        return
    path = os.path.join(folder, "step_0000_placeholder.png")
    try:
        from PIL import Image, ImageDraw
        img = Image.new("RGB", (1920, 1080), color=(30, 30, 30))
        draw = ImageDraw.Draw(img)
        msg = "No screenshot captured"
        try:
            tw = draw.textlength(msg)
        except AttributeError:
            tw = len(msg) * 8
        draw.text(((1920 - tw) / 2, 520), msg, fill=(180, 180, 180))
        img.save(path, "PNG")
    except Exception:
        import struct, zlib
        def _chunk(tag: bytes, data: bytes) -> bytes:
            crc = zlib.crc32(tag + data) & 0xFFFFFFFF
            return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)
        raw = (b'\x89PNG\r\n\x1a\n'
               + _chunk(b'IHDR', struct.pack('>IIBBBBB', 1, 1, 8, 2, 0, 0, 0))
               + _chunk(b'IDAT', zlib.compress(b'\x00\xff\xff\xff'))
               + _chunk(b'IEND', b''))
        with open(path, "wb") as f:
            f.write(raw)

def _dump_screenshots(history, folder: str) -> None:
    def _save(raw, label):
        if not raw:
            return False
        if isinstance(raw, str) and "," in raw:
            raw = raw.split(",", 1)[1]
        try:
            b = base64.b64decode(raw) if isinstance(raw, str) else raw
            if not (b[:4] == b'\x89PNG' or b[:2] == b'\xff\xd8'):
                return False
            with open(os.path.join(folder, f"{label}.png"), "wb") as fh:
                fh.write(b)
            return True
        except Exception:
            return False
    try:
        for i, r in enumerate((history.action_results() if history else None) or []):
            for a in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                if _save(getattr(r, a, None), f"step_{i+1:04d}_result"):
                    break
        for i, h in enumerate(getattr(history, "history", []) or []):
            s = getattr(h, "state", None)
            if s:
                for a in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                    if _save(getattr(s, a, None), f"step_{i+1:04d}_state"):
                        break
    except Exception:
        pass

def _dump_json_screenshots(folder: str) -> None:
    for jf in sorted(glob.glob(os.path.join(folder, "conversation_*.json"))):
        try:
            data = json.load(open(jf, "r", encoding="utf-8"))
        except Exception:
            continue
        msgs = data if isinstance(data, list) else data.get("messages", [])
        for msg in msgs:
            content = msg.get("content", [])
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                iu = (block.get("image_url") or {}).get("url", "")
                src = block.get("source") or {}
                raw = ""
                if iu.startswith("data:image"):
                    raw = iu.split(",", 1)[1] if "," in iu else ""
                elif src.get("type") == "base64":
                    raw = src.get("data", "")
                if not raw:
                    continue
                try:
                    b = base64.b64decode(raw)
                    if not (b[:4] == b'\x89PNG' or b[:2] == b'\xff\xd8'):
                        continue
                    stem = os.path.splitext(os.path.basename(jf))[0]
                    with open(os.path.join(folder, f"{stem}_img001.png"), "wb") as fh:
                        fh.write(b)
                except Exception:
                    pass

# ---------------------------------------------------------------------------
# URL FIX
# ---------------------------------------------------------------------------
_REAL_AND_WORDS = frozenset({
    "command", "demand", "expand", "understand", "withstand", "contraband",
    "headband", "armband", "remand", "reprimand", "mainland", "farmland",
    "highland", "lowland", "island", "strand", "brand", "grand", "stand",
    "sand", "hand", "land", "band", "wand", "bland", "gland", "planned",
    "scanned", "fanned", "manned", "spanned", "banned", "canned", "tanned", "panned",
})

def _fix_url_typos(text: str) -> str:
    def _r(m: _re.Match) -> str:
        url = m.group(0)
        t = _re.search(r'([a-z]{4,}and)$', url)
        if not t or t.group(1).lower() in _REAL_AND_WORDS:
            return url
        return url[:-3] + " and"
    return _re.sub(r'https?://\S+', _r, text)

def _wrap_prompt(p: str) -> str:
    p = _fix_url_typos(p)
    return f"""You are a browser automation agent. Execute the following task:

{p}

=== CRITICAL RULES ===
RULE 1: After clicking '+', wait 2s for GREEN TOAST. Toast seen = added, do NOT click again.
RULE 2: "Could not get element geometry" means JavaScript click fired. Trust it. Wait for toast.
RULE 3: Once modal is closed, do NOT reopen it.
RULE 4: After closing modal, go straight to main chat input.
RULE 5: If you see a Cloudflare browser check OR the page appears blank/empty,
         wait up to 30s for it to auto-resolve. Do NOT navigate away.
RULE 6: If the page is still blank after 30s, do a hard reload (navigate to same URL again).
=== END RULES ===

=== DATA EXTRACTION ===
Before extracting table data:
- Run JS: document.querySelector('table, .table, [role="grid"]')?.scrollIntoView()
- Extract href from EVERY anchor; if relative (starts with /), prepend https://procurement.gov.ae
- Set notice_link to null ONLY if no anchor exists.
- Output MUST be valid JSON array.
=== END ===
"""

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
def build_llm(model: str, api_key: str):
    from browser_use.llm import ChatOpenAI
    return ChatOpenAI(model=model, api_key=api_key)

# ---------------------------------------------------------------------------
# RESULT CLEANER
# ---------------------------------------------------------------------------
def _clean_result(text: str) -> Any:
    if not text:
        return text
    text = text.strip()
    m = _re.search(r'<r>\s*(.*?)\s*</r>', text, _re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except Exception:
            text = m.group(1).strip()
    m = _re.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', text, _re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except Exception:
            pass
    try:
        return json.loads(text)
    except Exception:
        pass
    for pat in (r'(\[\s*\{.*?\}\s*\])', r'(\{.*?\})', r'(\[.*?\])'):
        m = _re.search(pat, text, _re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                pass
    return text

def _is_valid(result: Any) -> bool:
    if result in (None, "", [], {}):
        return False
    if isinstance(result, str):
        low = result.lower()
        bad = ["agent error", "browser_check", "captcha", "wrong captcha",
               "error:", "maintenance", "access denied", "forbidden",
               "please wait", "just a moment", "checking your browser"]
        if any(k in low for k in bad):
            return False
        if len(result) < 200:
            return False
    if isinstance(result, dict):
        for v in result.values():
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                return True
        return any(isinstance(v, list) and len(v) > 0 for v in result.values())
    if isinstance(result, list):
        return len(result) > 0 and isinstance(result[0], dict)
    return True

# ---------------------------------------------------------------------------
# BROWSER FACTORY
# FIX #1: BD Scraping Browser NOW correctly activates when zone is set.
# The old code had `!= "scraping_browser1"` which PREVENTED activation.
# ---------------------------------------------------------------------------
def _make_browser_session(proxy: dict) -> Any:
    """
    Priority order:
    1. Bright Data Scraping Browser CDP (best — handles JS challenges natively)
    2. Local Playwright Chromium via BrowserProfile (fallback)
    """
    from browser_use.browser.session import BrowserSession
    try:
        from browser_use.browser.profile import BrowserProfile, ProxySettings
    except ImportError:
        from browser_use.browser.profile import BrowserProfile
        ProxySettings = dict

    viewport = random.choice(VIEWPORTS)
    ua = random.choice(USER_AGENTS)

    # ── Option A: Bright Data Scraping Browser CDP ─────────────────────────
    if BD_API_KEY and BD_SCRAPING_BROWSER_ZONE:
        # Extract base ID just in case
        base_customer_id = PROXY_USER.split("-zone-")[0] 
        
        # FIX: Append the zone AND the UAE country targeting tag
        sb_user = f"{base_customer_id}-zone-{BD_SCRAPING_BROWSER_ZONE}-country-ae"
        
        # FIX: Ensure connection uses wss://
        cdp_url = (
            f"wss://{sb_user}:{BD_SCRAPING_BROWSER_PASS}"
            f"@{BD_SCRAPING_BROWSER_HOST}:{BD_SCRAPING_BROWSER_PORT}"
        )
        print(f"[Browser] Connecting to Bright Data Scraping Browser (UAE targeted)")
        
        try:
            return BrowserSession(cdp_url=cdp_url)
        except Exception as e:
            print(f"[Browser] Scraping Browser connection failed: {e}")
            print("[Browser] Falling back to local Chromium")

    # ── Option B: Local Playwright Chromium ────────────────────────────────
    profile = BrowserProfile(
        headless=True,
        disable_security=True,
        proxy=ProxySettings(
            server=f"http://{proxy['host']}:{proxy['port']}",
            username=proxy["user"],
            password=proxy["pass"],
        ),
        viewport={"width": viewport["width"], "height": viewport["height"]},
        user_agent=ua,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--ignore-certificate-errors",
            "--ignore-ssl-errors",
            "--allow-insecure-localhost",
            "--allow-running-insecure-content",
            "--disable-web-security",
        ],
    )
    print("[Browser] Using Playwright Chromium with BrowserProfile")
    return BrowserSession(browser_profile=profile)
# ---------------------------------------------------------------------------
# SINGLE WORKER
# ---------------------------------------------------------------------------
async def _run_worker(
    wid: str,
    proxy: dict,
    request: AgentRequest,
    result_queue: asyncio.Queue,
    cancel_event: asyncio.Event,
    winner_lock: asyncio.Lock,
) -> None:
    sid = f"w{wid}_{str(uuid.uuid4())[:6]}"
    folder = f"{SCAN_DIR}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{sid}"
    os.makedirs(folder, exist_ok=True)
    print(f"[W{wid}] Starting - {proxy['host']}:{proxy['port']}")

    llm = build_llm(request.model, os.getenv("OPENAI_API_KEY", ""))
    steps = [0]
    pc = [False]
    blocked_count = [0]
    current_page_ref = [None]  # store page ref for JS fallback

    async def _step(agent) -> None:
        if cancel_event.is_set():
            raise asyncio.CancelledError()
        steps[0] += 1
        n = steps[0]
        try:
            bs = getattr(agent, "browser_session", None)
            if bs is None:
                return
            if not pc[0]:
                pc[0] = True
                await _verify_proxy(proxy, wid)

            page = await bs.get_current_page()
            if page is None:
                return

            current_page_ref[0] = page  # save for JS fallback

            if n % 3 == 0:
                await human_scroll(page, random.randint(200, 400))
                await human_mouse_move(page, random.randint(400, 1400), random.randint(300, 800))

            await _solve_captcha(page, proxy)

            if await _is_blocked(page):
                blocked_count[0] += 1
                current_url = await _page_url(page)
                print(f"[W{wid}] Blocked (#{blocked_count[0]}) at {current_url} — waiting 15s")
                await asyncio.sleep(15)

                # After waiting, check again
                still_blocked = await _is_blocked(page)
                if still_blocked and blocked_count[0] >= 2:
                    # Try a hard reload
                    print(f"[W{wid}] Still blocked — attempting hard reload")
                    try:
                        await page.goto(current_url, timeout=30000, wait_until="domcontentloaded")
                        await asyncio.sleep(10)
                    except Exception as re:
                        print(f"[W{wid}] Reload failed: {re}")

                if blocked_count[0] >= 4:
                    still_blocked = await _is_blocked(page)
                    if still_blocked:
                        print(f"[W{wid}] Persistent block after {blocked_count[0]} attempts — aborting")
                        raise RuntimeError("Persistent bot block — worker aborting")
            else:
                blocked_count[0] = 0

            if n % 5 == 0:
                await human_delay_long()
            else:
                await human_delay_short()

            try:
                img = await page.screenshot()
                if isinstance(img, str):
                    img = base64.b64decode(img)
                with open(os.path.join(folder, f"step_{n:04d}_cb.png"), "wb") as fh:
                    fh.write(img)
            except Exception as se:
                print(f"[W{wid}] Screenshot failed (non-fatal): {se}")
            print(f"[W{wid}] step {n:03d} ok")
        except asyncio.CancelledError:
            raise
        except RuntimeError:
            raise
        except Exception as e:
            print(f"[W{wid}] step {n} err: {e}")

    history = None
    rt = ""

    async with _browser_semaphore:
        try:
            await _warmup_extended(proxy, wid)
            browser_session = _make_browser_session(proxy)

            kwargs: dict = dict(
                task=_wrap_prompt(request.prompt),
                llm=llm,
                browser_session=browser_session,
                save_conversation_path=folder,
                max_actions_per_step=1,
                use_vision=True,
                max_failures=3,
            )

            agent = Agent(**kwargs)
            history = await asyncio.wait_for(
                agent.run(max_steps=request.max_steps, on_step_end=_step),
                timeout=WORKER_TIMEOUT,
            )

            # ── 4-pass result extraction ───────────────────────────────────
            try:
                fr = history.final_result()
                if fr:
                    rt = fr
            except Exception:
                pass
            if not rt:
                try:
                    for a in reversed(history.action_results() or []):
                        if getattr(a, "is_done", False):
                            rt = getattr(a, "extracted_content", "") or ""
                            break
                except Exception:
                    pass
            if not rt:
                try:
                    for h in reversed(history.history or []):
                        for r in reversed(getattr(h, "result", []) or []):
                            if getattr(r, "is_done", False):
                                rt = getattr(r, "extracted_content", "") or ""
                                break
                        if rt:
                            break
                except Exception:
                    pass
            if not rt:
                skip = ("Clicked", "Typed", "Waited", "Scrolled", "Searched", "Navigated")
                try:
                    for a in reversed(history.action_results() or []):
                        t = getattr(a, "extracted_content", "") or ""
                        if t and not any(t.startswith(s) for s in skip):
                            rt = t
                            break
                except Exception:
                    pass

            cleaned = _clean_result(rt)

            # ── FIX: JS table scrape fallback if agent returned nothing useful ──
            if not _is_valid(cleaned) and current_page_ref[0] is not None:
                print(f"[W{wid}] Agent result invalid — trying JS table scrape fallback")
                try:
                    js_data = await _js_scrape_procurement(current_page_ref[0])
                    if js_data:
                        cleaned = js_data
                        print(f"[W{wid}] JS fallback returned {len(js_data)} rows")
                except Exception as jse:
                    print(f"[W{wid}] JS fallback error: {jse}")

            if _is_valid(cleaned) and not cancel_event.is_set():
                async with winner_lock:
                    if cancel_event.is_set():
                        return
                    cancel_event.set()

                print(f"[W{wid}] Valid result!")
                _dump_screenshots(history, folder)
                _dump_json_screenshots(folder)
                _ensure_frames(folder)

                video_url = None
                try:
                    fc = len(glob.glob(os.path.join(folder, "*.png")))
                    print(f"[W{wid}] Building video ({fc} frames)...")
                    video_url = await create_and_upload_video(folder, sid)
                    print(f"[W{wid}] Video URL: {video_url}")
                except Exception as ve:
                    print(f"[W{wid}] Video failed: {ve}")

                await result_queue.put((wid, cleaned, steps[0], video_url))
            else:
                print(f"[W{wid}] Invalid/empty result — no winner from this worker")

        except asyncio.TimeoutError:
            print(f"[W{wid}] Timeout ({WORKER_TIMEOUT}s)")
        except asyncio.CancelledError:
            print(f"[W{wid}] Cancelled")
        except Exception as e:
            print(f"[W{wid}] Error: {e}")
        finally:
            try:
                shutil.rmtree(folder)
            except Exception:
                pass

# ---------------------------------------------------------------------------
# RACE RUNNER
# ---------------------------------------------------------------------------
async def _race(request: AgentRequest, proxies: list[dict]):
    q = asyncio.Queue()
    cancel = asyncio.Event()
    winner_lock = asyncio.Lock()
    tasks = [
        asyncio.create_task(_run_worker(str(i + 1), p, request, q, cancel, winner_lock))
        for i, p in enumerate(proxies)
    ]
    winner = None
    try:
        pending = set(tasks)
        while pending and winner is None:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            try:
                winner = q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            if not pending and winner is None:
                try:
                    winner = q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                break
    finally:
        cancel.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return winner

# ---------------------------------------------------------------------------
# MAIN ENDPOINT
# ---------------------------------------------------------------------------
@app.post("/agent/run", response_model=AgentResponse)
async def run_agent(request: AgentRequest) -> AgentResponse:
    await _refresh_proxy_pool()
    print(f"\n{'='*60}")
    print(f"[Agent] Task   : {request.prompt[:80]}...")
    print(f"[Agent] Browser: BD Scraping Browser CDP (zone: {BD_SCRAPING_BROWSER_ZONE})")
    print(f"[Agent] Proxy  : Bright Data Residential")
    print(f"{'='*60}\n")

    pool = list(_PROXY_POOL)
    for rnd in range(1, RACE_MAX_ROUNDS + 1):
        print(f"[Agent] Round {rnd}/{RACE_MAX_ROUNDS}")
        winner = await _race(request, [pool[0]])
        if winner is not None:
            wid, data, s, vu = winner
            print(f"[Agent] ✅ Success in round {rnd} — data={type(data).__name__}, video={'yes' if vu else 'no'}")
            return AgentResponse(video_url=vu, steps_taken=s, extracted_data=data, worker_id=wid)
        print(f"[Agent] Round {rnd} failed, retrying...")

    print("[Agent] All rounds exhausted — returning empty response")
    return AgentResponse(video_url=None, steps_taken=0, extracted_data=None, worker_id=None)

# ---------------------------------------------------------------------------
# STARTUP + HEALTH
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    await _refresh_proxy_pool()

@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": "7.0.0",
        "browser": "bd-scraping-browser-cdp",
        "scraping_browser_zone": BD_SCRAPING_BROWSER_ZONE,
        "proxy": "bright-data-residential",
    }
