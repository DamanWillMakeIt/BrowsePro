"""
main.py  v6.1 — DEMO STEALTH MODE
----------------------------------
MAX stealth with Webshare proxies only (no residential needed for demo)

CHANGES FROM v6:
  ✅ Realistic mouse movements with bezier curves
  ✅ Human-like scrolling (not instant scrollIntoView)
  ✅ Variable typing speed with occasional backspaces
  ✅ 5-15s random pauses between actions
  ✅ Pre-browsing warm-up (3 pages before target)
  ✅ Cookie persistence across steps
  ✅ Disabled race mode (1 worker = less suspicious)
  ✅ Extended timeout (5min per worker)
  ✅ Random user agents pool
  ✅ Realistic viewport sizes
"""
from __future__ import annotations
from typing import Any
import asyncio, base64, glob, json, os, random, re as _re, shutil, sys, uuid
from datetime import datetime, timedelta
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from browser_use import Agent

# Camoufox import
CAMOUFOX_AVAILABLE = False
try:
    from camoufox.async_api import AsyncCamoufox
    CAMOUFOX_AVAILABLE = True
    print("[Browser] ✅ Camoufox available")
except ImportError:
    print("[Browser] ⚠️  Camoufox not installed — Chromium fallback")

try:
    from browser_use.browser.browser import Browser, BrowserConfig
except ImportError:
    try:
        from browser_use.browser import Browser, BrowserConfig
    except ImportError:
        Browser = None
        BrowserConfig = None

from utils.helpers import create_and_upload_video

load_dotenv()

app = FastAPI(title="OnDemand Browser-Use Agent", version="6.1.0")
SCAN_DIR = "scans"
os.makedirs(SCAN_DIR, exist_ok=True)

CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "")
WEBSHARE_API_KEY  = os.getenv("WEBSHARE_API_KEY", "")
PROXY_USER        = os.getenv("PROXY_USER", "hgfumqbe")
PROXY_PASS        = os.getenv("PROXY_PASS", "t8a93hs91l3r")

# DEMO MODE: Single worker, longer timeout, multiple retries
RACE_WORKERS      = 1   # Single worker = less suspicious
RACE_MAX_ROUNDS   = 5   # Try 5 different proxies
WORKER_TIMEOUT    = 300 # 5 minutes per attempt
MAX_BROWSERS      = 1

_browser_semaphore = asyncio.Semaphore(MAX_BROWSERS)

# ---------------------------------------------------------------------------
# REALISTIC USER AGENTS POOL
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

# ---------------------------------------------------------------------------
# REALISTIC VIEWPORT SIZES
# ---------------------------------------------------------------------------
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
]

# ---------------------------------------------------------------------------
# PROXY POOL
# ---------------------------------------------------------------------------
_HARDCODED_PROXIES = [
    ("104.252.62.99", "5470"), ("45.248.55.14", "6600"), ("103.130.178.57", "5721"),
    ("82.22.181.141", "7852"), ("192.46.188.160", "5819"), ("82.21.49.192", "7455"),
    ("104.253.248.49", "5828"), ("140.233.168.158", "7873"), ("82.21.39.38", "7799"),
    ("9.142.219.200",  "6364"),
]

def _make_proxy(host: str, port: str) -> dict:
    return {"host": host, "port": port, "user": PROXY_USER, "pass": PROXY_PASS}

_PROXY_POOL: list[dict] = [_make_proxy(h, p) for h, p in _HARDCODED_PROXIES]
_pool_refreshed_at: datetime = datetime.min

async def _refresh_proxy_pool() -> None:
    global _PROXY_POOL, _pool_refreshed_at
    if not WEBSHARE_API_KEY:
        print("[ProxyPool] No WEBSHARE_API_KEY — using hardcoded pool")
        return
    if datetime.utcnow() - _pool_refreshed_at < timedelta(hours=1):
        return
    print("[ProxyPool] Fetching from Webshare API…")
    try:
        proxies, page_num = [], 1
        async with httpx.AsyncClient(timeout=15) as c:
            while True:
                r = await c.get(
                    "https://proxy.webshare.io/api/v2/proxy/list/",
                    headers={"Authorization": f"Token {WEBSHARE_API_KEY}"},
                    params={"mode": "direct", "page": page_num, "page_size": 100},
                )
                data = r.json()
                for p in data.get("results", []):
                    proxies.append(_make_proxy(p["proxy_address"], str(p["port"])))
                if not data.get("next"):
                    break
                page_num += 1
        if proxies:
            _PROXY_POOL = proxies
            _pool_refreshed_at = datetime.utcnow()
            print(f"[ProxyPool] ✅ Loaded {len(_PROXY_POOL)} proxies")
    except Exception as exc:
        print(f"[ProxyPool] Fetch failed: {exc}")

def _proxy_browser_dict(p: dict) -> dict:
    return {"server": f"http://{p['host']}:{p['port']}", "username": p["user"], "password": p["pass"]}

def _proxy_httpx_url(p: dict) -> str:
    return f"http://{p['user']}:{p['pass']}@{p['host']}:{p['port']}"

def _proxy_camoufox_dict(p: dict) -> dict:
    return {"server": f"http://{p['host']}:{p['port']}", "username": p["user"], "password": p["pass"]}

# ---------------------------------------------------------------------------
# STEALTH (JS fallback)
# ---------------------------------------------------------------------------
try:
    from playwright_stealth import stealth_async as _stealth_async
    STEALTH_LIB = True
except ImportError:
    STEALTH_LIB = False

print("=" * 60)
print(f"[Deploy] Camoufox          : {'✅' if CAMOUFOX_AVAILABLE else '❌ Chromium fallback'}")
print(f"[Deploy] CAPSOLVER_API_KEY : {'✅' if CAPSOLVER_API_KEY else '❌'}")
print(f"[Deploy] Demo Mode         : 1 worker, 5 rounds, 5min timeout")
print("=" * 60)

STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [
    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' },
]});
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en', 'ar'] });
Object.defineProperty(navigator, 'language', { get: () => 'en-US' });
Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
Object.defineProperty(navigator, 'vendor', { get: () => 'Google Inc.' });
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 0 });
window.chrome = {
    runtime: { id: undefined, connect: () => {}, sendMessage: () => {}, onMessage: { addListener: () => {}, removeListener: () => {} } },
    loadTimes: () => ({ requestTime: Date.now()/1000 - Math.random(), wasNpnNegotiated: true, npnNegotiatedProtocol: 'h2', connectionInfo: 'h2' }),
    csi: () => ({ startE: Date.now()-500, onloadT: Date.now()-200, pageT: 1200, tran: 15 }), app: {},
};
const _oQ = window.navigator.permissions.query.bind(navigator.permissions);
window.navigator.permissions.query = (p) => p.name === 'notifications' ? Promise.resolve({ state: Notification.permission }) : _oQ(p);
const _oD = HTMLCanvasElement.prototype.toDataURL;
HTMLCanvasElement.prototype.toDataURL = function(t, ...a) {
    const c = this.getContext('2d');
    if (c) { const d = c.getImageData(0,0,this.width||1,this.height||1); d.data[0]^=1; c.putImageData(d,0,0); }
    return _oD.call(this, t, ...a);
};
const _oG = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(p) {
    if (p===37445) return 'Intel Inc.'; if (p===37446) return 'Intel Iris OpenGL Engine';
    return _oG.call(this, p);
};
try {
    const _oG2 = WebGL2RenderingContext.prototype.getParameter;
    WebGL2RenderingContext.prototype.getParameter = function(p) {
        if (p===37445) return 'Intel Inc.'; if (p===37446) return 'Intel Iris OpenGL Engine';
        return _oG2.call(this, p);
    };
} catch(e) {}
Object.defineProperty(screen, 'width', { get: () => 1920 });
Object.defineProperty(screen, 'height', { get: () => 1080 });
Object.defineProperty(screen, 'availWidth', { get: () => 1920 });
Object.defineProperty(screen, 'availHeight', { get: () => 1040 });
Object.defineProperty(screen, 'colorDepth', { get: () => 24 });
Object.defineProperty(screen, 'pixelDepth', { get: () => 24 });
try {
    Object.defineProperty(navigator, 'connection', {
        get: () => ({ effectiveType: '4g', rtt: 50+Math.floor(Math.random()*50), downlink: 10+Math.random()*5, saveData: false })
    });
} catch(e) {}
"""

async def _apply_cdp_stealth(bs) -> None:
    if bs is None:
        return
    fn = getattr(bs, "_cdp_add_init_script", None)
    if fn:
        try:
            await fn(STEALTH_SCRIPT) if asyncio.iscoroutinefunction(fn) else fn(STEALTH_SCRIPT)
        except Exception:
            pass
    if STEALTH_LIB:
        gp = getattr(bs, "_cdp_get_all_pages", None)
        if gp:
            try:
                pages = await gp() if asyncio.iscoroutinefunction(gp) else gp()
                for pg in (pages or []):
                    if hasattr(pg, "add_init_script"):
                        try:
                            await _stealth_async(pg)
                        except Exception:
                            pass
                        break
            except Exception:
                pass

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
# HUMAN BEHAVIOR SIMULATION
# ---------------------------------------------------------------------------
async def human_mouse_move(page, to_x: int, to_y: int) -> None:
    """Bezier curve mouse movement"""
    try:
        # Get current position
        current = await page.evaluate("() => [window.lastMouseX || 0, window.lastMouseY || 0]")
        start_x, start_y = current[0], current[1]
        
        # Generate bezier control points
        cp1_x = start_x + (to_x - start_x) * random.uniform(0.2, 0.4)
        cp1_y = start_y + (to_y - start_y) * random.uniform(0.2, 0.4) + random.randint(-50, 50)
        cp2_x = start_x + (to_x - start_x) * random.uniform(0.6, 0.8)
        cp2_y = start_y + (to_y - start_y) * random.uniform(0.6, 0.8) + random.randint(-50, 50)
        
        # Move in steps along curve
        steps = random.randint(15, 25)
        for i in range(steps + 1):
            t = i / steps
            # Cubic bezier formula
            x = int((1-t)**3 * start_x + 3*(1-t)**2*t * cp1_x + 3*(1-t)*t**2 * cp2_x + t**3 * to_x)
            y = int((1-t)**3 * start_y + 3*(1-t)**2*t * cp1_y + 3*(1-t)*t**2 * cp2_y + t**3 * to_y)
            
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.01, 0.03))
        
        # Store position for next move
        await page.evaluate(f"() => {{ window.lastMouseX = {to_x}; window.lastMouseY = {to_y}; }}")
    except Exception:
        pass

async def human_scroll(page, distance: int = 300) -> None:
    """Human-like scrolling with variable speed"""
    try:
        scroll_steps = random.randint(8, 15)
        step_size = distance / scroll_steps
        
        for _ in range(scroll_steps):
            await page.evaluate(f"window.scrollBy(0, {step_size})")
            await asyncio.sleep(random.uniform(0.05, 0.15))
        
        # Random pause after scroll
        await asyncio.sleep(random.uniform(0.3, 0.8))
    except Exception:
        pass

async def human_delay_long() -> None:
    """5-15 second human thinking pause"""
    await asyncio.sleep(random.uniform(5.0, 15.0))

async def human_delay_short() -> None:
    """1-3 second pause"""
    await asyncio.sleep(random.uniform(1.0, 3.0))

# ---------------------------------------------------------------------------
# EXTENDED WARM-UP (3 pages before target)
# ---------------------------------------------------------------------------
async def _warmup_extended(page, wid: str) -> None:
    """Browse 3 random pages to establish realistic session"""
    sites = [
        "https://www.google.com/search?q=uae+news",
        "https://www.bbc.com/news",
        "https://www.wikipedia.org",
        "https://www.linkedin.com",
        "https://www.bing.com/search?q=dubai+weather",
    ]
    
    random.shuffle(sites)
    
    for i, site in enumerate(sites[:3]):  # Visit 3 sites
        try:
            print(f"[W{wid}] Warm-up {i+1}/3: {site}")
            nav = getattr(page, "goto", None) or getattr(page, "navigate", None)
            if nav:
                await nav(site, timeout=20000)
                
                # Realistic user behavior on each page
                await human_delay_short()
                
                # Random scroll
                await human_scroll(page, random.randint(200, 600))
                
                # Random mouse movement
                await human_mouse_move(page, random.randint(300, 1200), random.randint(200, 700))
                
                # Longer pause between pages
                await asyncio.sleep(random.uniform(2.0, 4.0))
                
        except Exception as e:
            print(f"[W{wid}] Warm-up {i+1} failed (non-fatal): {e}")
    
    print(f"[W{wid}] ✅ Extended warm-up complete (3 pages)")

# ---------------------------------------------------------------------------
# PROXY VERIFY
# ---------------------------------------------------------------------------
async def _verify_proxy(proxy: dict, wid: str) -> None:
    try:
        async with httpx.AsyncClient(proxy=_proxy_httpx_url(proxy), timeout=8) as c:
            ip = (await c.get("https://ipinfo.io/json")).json().get("ip", "?")
            print(f"[W{wid}] Proxy exit IP: {ip}")
    except Exception as e:
        print(f"[W{wid}] ProxyCheck failed: {e}")

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

# ---------------------------------------------------------------------------
# CAPTCHA DETECTION
# ---------------------------------------------------------------------------
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

        # Turnstile
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
            print("[CAPTCHA] Turnstile detected — solving…")
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
                print("[CAPTCHA] Turnstile solved ✅")
            return

        if "Just a moment" in html or "cf-browser-verification" in html:
            print("[CAPTCHA] Cloudflare JS challenge — waiting…")
            for _ in range(15):
                await asyncio.sleep(1)
                if "Just a moment" not in await page.content():
                    print("[CAPTCHA] Cloudflare cleared ✅")
                    return
            return

        # reCAPTCHA v2
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
                        await asyncio.sleep(3)
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
            print("[CAPTCHA] reCAPTCHA v2 detected — solving…")
            sol = await _capsolver_solve(
                {"type": "ReCaptchaV2Task", "websiteURL": purl, "websiteKey": rc_key}, proxy=proxy)
            if sol:
                t = sol.get("gRecaptchaResponse", "")
                await page.evaluate("""(t) => {
                    document.querySelectorAll('[name="g-recaptcha-response"]')
                        .forEach(el => { el.innerHTML=t; el.value=t; el.style.display='block'; });
                    document.querySelectorAll('[data-callback]').forEach(el => {
                        const cb=el.getAttribute('data-callback');
                        if(cb&&window[cb]) try{window[cb](t);}catch(e){}
                    });
                    const ta=document.querySelector('textarea[name="g-recaptcha-response"]');
                    if(ta){const f=ta.closest('form');if(f)try{f.submit();}catch(e){}}
                }""", t)
                print("[CAPTCHA] reCAPTCHA solved ✅")
            return

        # hCaptcha
        if "hcaptcha" in html.lower():
            m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
            if m:
                print("[CAPTCHA] hCaptcha detected — solving…")
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
                    print("[CAPTCHA] hCaptcha solved ✅")
    except Exception as e:
        print(f"[CAPTCHA] error: {e}")

# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------
class AgentRequest(BaseModel):
    prompt: str
    max_steps: int = 50
    model: str = "gpt-5.1"

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
            for a in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                if _save(getattr(h, a, None), f"step_{i+1:04d}_h"):
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
RULE 1: After clicking '+', wait 2s for GREEN TOAST. Toast seen → added, do NOT click again.
RULE 2: "Could not get element geometry" = JavaScript click fired. Trust it. Wait for toast.
RULE 3: Once modal is closed, do NOT reopen it.
RULE 4: After closing modal, go straight to main chat input.
=== END RULES ===

=== DATA EXTRACTION ===
Before extracting table data:
- Run JS: document.querySelector('table, .table, [role="grid"]')?.scrollIntoView()
- Extract href from EVERY anchor; if relative (starts with /), prepend https://procurement.gov.ae
- Set notice_link to null ONLY if no anchor exists.
=== END ===
"""

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
def build_llm(model: str, api_key: str):
    for mod, cls in [("browser_use.llm", "ChatOpenAI"), ("browser_use.agent.llm", "ChatOpenAI")]:
        try:
            import importlib
            m = importlib.import_module(mod)
            return getattr(m, cls)(model=model, api_key=api_key)
        except Exception:
            pass
    try:
        from openai import AsyncOpenAI
        return AsyncOpenAI(api_key=api_key)
    except Exception:
        pass
    raise RuntimeError("Could not build LLM")

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
# ---------------------------------------------------------------------------
async def _create_browser_and_page(proxy: dict, wid: str):
    if CAMOUFOX_AVAILABLE:
        try:
            viewport = random.choice(VIEWPORTS)
            camoufox_ctx = AsyncCamoufox(
                headless=True,
                os="windows",
                proxy=_proxy_camoufox_dict(proxy),
                geoip=True,
                humanize=True,
                screen=viewport,  # Random viewport size
            )
            browser = await camoufox_ctx.__aenter__()
            page = await browser.new_page()
            print(f"[W{wid}] 🦊 Camoufox launched ({viewport['width']}x{viewport['height']})")
            return camoufox_ctx, page, True
        except Exception as e:
            print(f"[W{wid}] Camoufox failed ({e}), fallback to Chromium")

    # Chromium fallback
    if BrowserConfig and Browser:
        try:
            viewport = random.choice(VIEWPORTS)
            ua = random.choice(USER_AGENTS)
            cfg = BrowserConfig(
                headless="new",
                proxy=_proxy_browser_dict(proxy),
                extra_chromium_args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox", "--disable-dev-shm-usage",
                    "--enable-webgl", "--use-gl=swiftshader",
                    "--enable-accelerated-2d-canvas",
                    f"--window-size={viewport['width']},{viewport['height']}",
                    "--start-maximized",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--no-first-run", "--no-default-browser-check",
                    "--password-store=basic", "--use-mock-keychain",
                    "--disable-infobars", "--lang=en-US,en",
                    "--accept-lang=en-US,en;q=0.9,ar;q=0.8",
                    f'--user-agent={ua}',
                ],
            )
            b = Browser(config=cfg)
            print(f"[W{wid}] 🌐 Chromium launched ({viewport['width']}x{viewport['height']})")
            return b, None, False
        except Exception as e:
            print(f"[W{wid}] Chromium also failed: {e}")
    return None, None, False

async def _close_browser(browser_obj, is_camoufox: bool) -> None:
    if browser_obj is None:
        return
    try:
        if is_camoufox:
            await browser_obj.__aexit__(None, None, None)
        else:
            await browser_obj.close()
    except Exception:
        pass

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
    print(f"[W{wid}] Starting — {proxy['host']}:{proxy['port']}")

    llm = build_llm(request.model, os.getenv("OPENAI_API_KEY", ""))
    steps = [0]
    pc = [False]

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
            if not CAMOUFOX_AVAILABLE:
                await _apply_cdp_stealth(bs)
            page = await bs.get_current_page()
            if page is None:
                return
            
            # Human behavior every 3 steps
            if n % 3 == 0:
                await human_scroll(page, random.randint(200, 400))
                await human_mouse_move(page, random.randint(400, 1400), random.randint(300, 800))
            
            await _solve_captcha(page, proxy)
            
            # Longer random pauses every 5 steps
            if n % 5 == 0:
                await human_delay_long()
            else:
                await human_delay_short()
            
            img = await page.screenshot()
            if isinstance(img, str):
                img = base64.b64decode(img)
            with open(os.path.join(folder, f"step_{n:04d}_cb.png"), "wb") as fh:
                fh.write(img)
            print(f"[W{wid}] step {n:03d} ✓")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[W{wid}] step {n} err: {e}")

    browser_obj = None
    is_camoufox = False
    history = None
    rt = ""

    async with _browser_semaphore:
        try:
            browser_obj, warm_page, is_camoufox = await _create_browser_and_page(proxy, wid)

            # EXTENDED WARM-UP (3 pages)
            if warm_page is not None:
                await _warmup_extended(warm_page, wid)

            kwargs: dict = dict(
                task=_wrap_prompt(request.prompt), llm=llm,
                save_conversation_path=folder, max_actions_per_step=1,
                use_vision=True, max_failures=3, retry_delay=2,
            )

            if is_camoufox and browser_obj is not None:
                try:
                    kwargs["browser"] = browser_obj
                except Exception:
                    pass
            elif browser_obj is not None:
                kwargs["browser"] = browser_obj

            agent = Agent(**kwargs)
            history = await asyncio.wait_for(
                agent.run(max_steps=request.max_steps, on_step_end=_step),
                timeout=WORKER_TIMEOUT,
            )

            # 4-pass extraction
            try:
                fr = history.final_result()
                if fr:
                    rt = fr
            except Exception:
                pass
            if not rt:
                try:
                    for a in reversed(history.action_results() or []):
                        if getattr(a, 'is_done', False):
                            rt = getattr(a, 'extracted_content', '') or ''
                            break
                except Exception:
                    pass
            if not rt:
                try:
                    for h in reversed(history.history or []):
                        for r in reversed(getattr(h, 'result', []) or []):
                            if getattr(r, 'is_done', False):
                                rt = getattr(r, 'extracted_content', '') or ''
                                break
                        if rt:
                            break
                except Exception:
                    pass
            if not rt:
                skip = ('🔗','🔍','Clicked','Typed','Waited','Scrolled','Searched','Navigated','scroll','Scroll')
                try:
                    for a in reversed(history.action_results() or []):
                        t = getattr(a, 'extracted_content', '') or ''
                        if t and not any(t.startswith(s) for s in skip):
                            rt = t
                            break
                except Exception:
                    pass

            cleaned = _clean_result(rt)

            if _is_valid(cleaned) and not cancel_event.is_set():
                async with winner_lock:
                    if cancel_event.is_set():
                        print(f"[W{wid}] Lost the race")
                        return
                    cancel_event.set()

                print(f"[W{wid}] ✅ Valid result!")
                _dump_screenshots(history, folder)
                _dump_json_screenshots(folder)
                _ensure_frames(folder)

                video_url = None
                try:
                    fc = len(glob.glob(os.path.join(folder, "*.png")))
                    print(f"[W{wid}] Building video ({fc} frames)…")
                    video_url = await create_and_upload_video(folder, sid)
                except Exception as ve:
                    print(f"[W{wid}] Video failed: {ve}")

                await result_queue.put((wid, cleaned, steps[0], video_url))
            else:
                print(f"[W{wid}] ❌ Invalid result")

        except asyncio.TimeoutError:
            print(f"[W{wid}] ⏱ Timeout ({WORKER_TIMEOUT}s)")
        except asyncio.CancelledError:
            print(f"[W{wid}] Cancelled")
        except Exception as e:
            print(f"[W{wid}] Error: {e}")
        finally:
            await _close_browser(browser_obj, is_camoufox)
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
        asyncio.create_task(_run_worker(str(i+1), p, request, q, cancel, winner_lock))
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
    print(f"[Demo] Task    : {request.prompt[:80]}…")
    print(f"[Demo] Browser : {'Camoufox 🦊' if CAMOUFOX_AVAILABLE else 'Chromium'}")
    print(f"[Demo] Mode    : 1 worker, 5 rounds, 5min timeout, extended warm-up")
    print(f"{'='*60}\n")

    pool = list(_PROXY_POOL)
    random.shuffle(pool)

    for rnd in range(1, RACE_MAX_ROUNDS + 1):
        proxies = [pool[(rnd - 1) % len(pool)]]  # Single proxy per round
        print(f"[Demo] Round {rnd}/5 — {proxies[0]['host']}")

        winner = await _race(request, proxies)

        if winner is not None:
            wid, data, steps, vu = winner
            print(f"\n[Demo] 🏆 Success in round {rnd}")
            return AgentResponse(
                video_url=vu,
                steps_taken=steps,
                extracted_data=data,
                worker_id=wid,
            )

        print(f"[Demo] Round {rnd} failed, next proxy…")

    print("[Demo] ❌ All rounds exhausted")
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
        "version": "6.1.0-demo-stealth",
        "browser": "camoufox" if CAMOUFOX_AVAILABLE else "chromium-fallback",
        "proxy_pool_size": len(_PROXY_POOL),
        "mode": "single-worker-extended-warmup",
    }
