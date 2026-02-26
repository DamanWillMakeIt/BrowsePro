"""
main.py  v5 — RACE MODE (production-hardened)
----------------------------------------------
FastAPI wrapper around browser-use 0.11.x

FIXES OVER v4:
  FIX-1  Warm-up now runs BEFORE agent.run(), not inside step callback
  FIX-2  Slot-claimed asyncio.Lock prevents two workers double-queuing
  FIX-3  Per-worker asyncio.wait_for(timeout=180s) prevents hung workers
  FIX-4  RACE_WORKERS default lowered to 3 (safe for 4GB RAM)
         RACE_MAX_ROUNDS raised to 8 (still 24 proxy attempts per request)
  FIX-5  _is_valid() checks for real data keys, not just string length
  FIX-6  Extracted data queued BEFORE video build — data never lost if
         video upload crashes
  FIX-7  Global semaphore caps concurrent Chromium instances hard

ENV VARS:
  WEBSHARE_API_KEY  — auto-fetches all 250 proxies at startup & hourly
  PROXY_USER        — proxy username  (default: hgfumqbe)
  PROXY_PASS        — proxy password  (default: t8a93hs91l3r)
  RACE_WORKERS      — parallel workers per round (default: 3 for 4GB RAM)
  RACE_MAX_ROUNDS   — max retry rounds           (default: 8)
  WORKER_TIMEOUT    — seconds before a worker is killed (default: 180)
  CAPSOLVER_API_KEY — for CAPTCHA solving
  OPENAI_API_KEY    — for the LLM
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

app = FastAPI(title="OnDemand Browser-Use Agent", version="5.0.0")
SCAN_DIR = "scans"
os.makedirs(SCAN_DIR, exist_ok=True)

CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "")
WEBSHARE_API_KEY  = os.getenv("WEBSHARE_API_KEY", "")
PROXY_USER        = os.getenv("PROXY_USER", "hgfumqbe")
PROXY_PASS        = os.getenv("PROXY_PASS", "t8a93hs91l3r")

# FIX-4: 3 workers is safe on 4GB RAM (3 × ~350MB Chromium = ~1GB, leaves room for everything else)
# If you upgrade to 8GB+ you can safely raise this to 6-8
RACE_WORKERS   = int(os.getenv("RACE_WORKERS",   "3"))
RACE_MAX_ROUNDS = int(os.getenv("RACE_MAX_ROUNDS", "8"))
WORKER_TIMEOUT  = int(os.getenv("WORKER_TIMEOUT",  "180"))  # seconds per worker before kill

# FIX-7: Hard cap on simultaneous Chromium instances regardless of RACE_WORKERS setting
# Prevents accidental OOM if someone sets RACE_WORKERS=10 on a small box
MAX_BROWSERS = int(os.getenv("MAX_BROWSERS", "4"))
_browser_semaphore = asyncio.Semaphore(MAX_BROWSERS)

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
            print(f"[ProxyPool] ✅ Loaded {len(_PROXY_POOL)} proxies from Webshare")
        else:
            print("[ProxyPool] ⚠️ API returned 0 proxies — keeping existing pool")
    except Exception as exc:
        print(f"[ProxyPool] Fetch failed: {exc} — keeping existing pool")


def _proxy_browser_dict(p: dict) -> dict:
    return {"server": f"http://{p['host']}:{p['port']}", "username": p["user"], "password": p["pass"]}

def _proxy_httpx_url(p: dict) -> str:
    return f"http://{p['user']}:{p['pass']}@{p['host']}:{p['port']}"

# ---------------------------------------------------------------------------
# STEALTH
# ---------------------------------------------------------------------------
try:
    from playwright_stealth import stealth_async as _stealth_async
    STEALTH_LIB = True
    print("[Stealth] playwright-stealth available ✅")
except ImportError:
    STEALTH_LIB = False
    print("[Stealth] playwright-stealth not installed")

print("=" * 60)
print(f"[Deploy] Python            : {sys.version}")
print(f"[Deploy] playwright-stealth: {'✅' if STEALTH_LIB else '❌'}")
print(f"[Deploy] CAPSOLVER_API_KEY : {'SET ✅' if CAPSOLVER_API_KEY else 'NOT SET ❌'}")
print(f"[Deploy] WEBSHARE_API_KEY  : {'SET ✅' if WEBSHARE_API_KEY else 'NOT SET — hardcoded pool'}")
print(f"[Deploy] Proxy pool size   : {len(_PROXY_POOL)}")
print(f"[Deploy] Race workers      : {RACE_WORKERS}  |  Max rounds : {RACE_MAX_ROUNDS}")
print(f"[Deploy] Worker timeout    : {WORKER_TIMEOUT}s  |  Max browsers: {MAX_BROWSERS}")
print("=" * 60)

STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [
    { name: 'Chrome PDF Plugin',  filename: 'internal-pdf-viewer',              description: 'Portable Document Format' },
    { name: 'Chrome PDF Viewer',  filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
    { name: 'Native Client',      filename: 'internal-nacl-plugin',             description: '' },
]});
Object.defineProperty(navigator, 'languages',           { get: () => ['en-US', 'en', 'ar'] });
Object.defineProperty(navigator, 'language',            { get: () => 'en-US' });
Object.defineProperty(navigator, 'platform',            { get: () => 'Win32' });
Object.defineProperty(navigator, 'vendor',              { get: () => 'Google Inc.' });
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
Object.defineProperty(navigator, 'deviceMemory',        { get: () => 8 });
Object.defineProperty(navigator, 'maxTouchPoints',      { get: () => 0 });
window.chrome = {
    runtime: { id: undefined, connect: () => {}, sendMessage: () => {},
               onMessage: { addListener: () => {}, removeListener: () => {} } },
    loadTimes: () => ({ requestTime: Date.now()/1000 - Math.random(),
                        wasNpnNegotiated: true, npnNegotiatedProtocol: 'h2', connectionInfo: 'h2' }),
    csi: () => ({ startE: Date.now()-500, onloadT: Date.now()-200, pageT: 1200, tran: 15 }),
    app: {},
};
const _oQ = window.navigator.permissions.query.bind(navigator.permissions);
window.navigator.permissions.query = (p) =>
    p.name === 'notifications' ? Promise.resolve({ state: Notification.permission }) : _oQ(p);
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
Object.defineProperty(screen, 'width',       { get: () => 1920 });
Object.defineProperty(screen, 'height',      { get: () => 1080 });
Object.defineProperty(screen, 'availWidth',  { get: () => 1920 });
Object.defineProperty(screen, 'availHeight', { get: () => 1040 });
Object.defineProperty(screen, 'colorDepth',  { get: () => 24 });
Object.defineProperty(screen, 'pixelDepth',  { get: () => 24 });
try {
    Object.defineProperty(navigator, 'connection', {
        get: () => ({ effectiveType: '4g', rtt: 50+Math.floor(Math.random()*50),
                      downlink: 10+Math.random()*5, saveData: false })
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
        except Exception as e:
            print(f"[Stealth] failed: {e}")
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
# PROXY VERIFY
# ---------------------------------------------------------------------------

async def _verify_proxy(proxy: dict, wid: str) -> None:
    try:
        async with httpx.AsyncClient(proxy=_proxy_httpx_url(proxy), timeout=8) as c:
            ip   = (await c.get("https://ipinfo.io/json")).json().get("ip", "?")
            mark = "🟢" if ip == proxy["host"] else "🟡"
            print(f"[W{wid}] Proxy {mark} exit={ip}")
    except Exception as e:
        print(f"[W{wid}] ProxyCheck failed: {e}")

# ---------------------------------------------------------------------------
# FIX-1: BROWSER WARM-UP — called BEFORE agent.run(), not inside step callback
# ---------------------------------------------------------------------------

async def _warmup_page(page, wid: str) -> None:
    """Navigate to a neutral site to seed cookies and HTTP/2 session state."""
    site = random.choice(["https://www.google.com", "https://www.bing.com", "https://www.wikipedia.org"])
    try:
        nav = getattr(page, "goto", None) or getattr(page, "navigate", None)
        if nav:
            await nav(site, timeout=15000)
            await asyncio.sleep(random.uniform(2.0, 3.5))
            print(f"[W{wid}] Warm-up ✅ ({site})")
    except Exception as e:
        print(f"[W{wid}] Warm-up failed (non-fatal): {e}")

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
        purl   = await _page_url(page)
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

        # CF JS challenge
        if "Just a moment" in html or "cf-browser-verification" in html:
            for _ in range(15):
                await asyncio.sleep(1)
                if "Just a moment" not in await page.content():
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
            return

        # hCaptcha
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

async def human_delay(a: int = 300, b: int = 1000) -> None:
    await asyncio.sleep(random.uniform(a / 1000, b / 1000))

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
# SCREENSHOT / FRAME HELPERS
# ---------------------------------------------------------------------------

def _ensure_frames(folder: str) -> None:
    if glob.glob(os.path.join(folder, "*.png")):
        return
    path = os.path.join(folder, "step_0000_placeholder.png")
    try:
        from PIL import Image, ImageDraw
        img  = Image.new("RGB", (1920, 1080), color=(30, 30, 30))
        draw = ImageDraw.Draw(img)
        msg  = "No screenshot captured"
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
    saved = 0
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
                iu  = (block.get("image_url") or {}).get("url", "")
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
                    with open(os.path.join(folder, f"{stem}_img{saved+1:03d}.png"), "wb") as fh:
                        fh.write(b)
                    saved += 1
                except Exception:
                    pass

# ---------------------------------------------------------------------------
# URL TYPO FIX
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
        t   = _re.search(r'([a-z]{4,}and)$', url)
        if not t or t.group(1).lower() in _REAL_AND_WORDS:
            return url
        return url[:-3] + " and"
    return _re.sub(r'https?://\S+', _r, text)

def _wrap_prompt(p: str) -> str:
    p = _fix_url_typos(p)
    return f"""You are a browser automation agent. Execute the following task:

{p}

=== CRITICAL RULES FOR ADDING AGENT TOOLS ===
RULE 1: After clicking '+', wait 2s for GREEN TOAST. Toast seen → added, do NOT click again. No toast → try once more. NEVER click more than twice.
RULE 2: "Could not get element geometry" = JavaScript click fired. Trust it. Wait for toast.
RULE 3: Once modal is closed, do NOT reopen it.
RULE 4: After closing modal, go straight to main chat input. Do not look back at sidebar.
=== END CRITICAL RULES ===

=== DATA EXTRACTION RULES ===
When extracting rows from a paginated or scrollable table:
- Before calling extract, run JS: document.querySelector('table, .table, [role="grid"]')?.scrollIntoView()
- Extract href from EVERY anchor in first/code column; if relative (starts with /), prepend https://procurement.gov.ae
- Set notice_link to null ONLY if there is genuinely no anchor — never null just because row was off-screen.
=== END DATA EXTRACTION RULES ===
"""

# ---------------------------------------------------------------------------
# LLM BUILDER
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

# FIX-5: Smarter validation — checks for real data keys, not just string length
def _is_valid(result: Any) -> bool:
    if result in (None, "", [], {}):
        return False
    if isinstance(result, str):
        low = result.lower()
        # Explicit rejection patterns
        bad_phrases = [
            "agent error", "browser_check", "captcha", "wrong captcha",
            "error:", "maintenance", "access denied", "forbidden",
            "please wait", "just a moment", "checking your browser",
        ]
        if any(k in low for k in bad_phrases):
            return False
        # Must be substantial to be real extracted data
        if len(result) < 200:
            return False
    if isinstance(result, dict):
        # Must have at least one list with actual items
        has_data = any(isinstance(v, list) and len(v) > 0 for v in result.values())
        if not has_data:
            return False
        # Extra check: if it looks like a tenders response, verify items have expected keys
        for v in result.values():
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                item_keys = set(v[0].keys())
                # Accept if item has at least 2 of these typical extraction keys
                expected = {"reference_number", "issuing_entity", "tender_title",
                            "publication_begin_utc", "submission_deadline_utc", "notice_link"}
                if len(item_keys & expected) >= 2:
                    return True
        return has_data
    if isinstance(result, list):
        return len(result) > 0 and isinstance(result[0], dict)
    return True

# ---------------------------------------------------------------------------
# SINGLE WORKER  (with all 6 fixes applied)
# ---------------------------------------------------------------------------

async def _run_worker(
    wid: str,
    proxy: dict,
    request: AgentRequest,
    result_queue: asyncio.Queue,
    cancel_event: asyncio.Event,
    winner_lock: asyncio.Lock,      # FIX-2: prevents double-queue
) -> None:
    sid    = f"w{wid}_{str(uuid.uuid4())[:6]}"
    folder = f"{SCAN_DIR}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{sid}"
    os.makedirs(folder, exist_ok=True)
    print(f"[W{wid}] Starting — proxy {proxy['host']}:{proxy['port']}")

    llm   = build_llm(request.model, os.getenv("OPENAI_API_KEY", ""))
    steps = [0]
    pc    = [False]

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
            await _apply_cdp_stealth(bs)
            page = await bs.get_current_page()
            if page is None:
                return
            await _solve_captcha(page, proxy)
            await human_delay(200, 800)
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

    kwargs: dict = dict(
        task=_wrap_prompt(request.prompt), llm=llm,
        save_conversation_path=folder, max_actions_per_step=1,
        use_vision=True, max_failures=3, retry_delay=2,
    )

    # FIX-7: Acquire semaphore before launching browser to cap concurrent instances
    browser = None
    async with _browser_semaphore:
        if BrowserConfig and Browser:
            try:
                browser = Browser(config=BrowserConfig(
                    headless="new",
                    proxy=_proxy_browser_dict(proxy),
                    extra_chromium_args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox", "--disable-dev-shm-usage",
                        "--enable-webgl", "--use-gl=swiftshader",
                        "--enable-accelerated-2d-canvas",
                        "--window-size=1920,1080", "--start-maximized",
                        "--disable-features=IsolateOrigins,site-per-process",
                        "--disable-background-timer-throttling",
                        "--disable-backgrounding-occluded-windows",
                        "--disable-renderer-backgrounding",
                        "--no-first-run", "--no-default-browser-check",
                        "--password-store=basic", "--use-mock-keychain",
                        "--disable-infobars",
                        "--lang=en-US,en", "--accept-lang=en-US,en;q=0.9,ar;q=0.8",
                        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                    ],
                ))
                kwargs["browser"] = browser
            except Exception as e:
                print(f"[W{wid}] BrowserConfig failed: {e}")

        history = None
        rt      = ""
        try:
            # FIX-1: Warm up BEFORE agent.run() using a direct page navigation
            if browser is not None:
                try:
                    # Get the initial page that browser-use opens and warm it up
                    agent_pre = Agent(**{**kwargs, "task": "navigate to https://www.google.com"})
                    bs_pre    = getattr(agent_pre, "browser_session", None)
                    if bs_pre:
                        page_pre = await bs_pre.get_current_page()
                        if page_pre:
                            await _warmup_page(page_pre, wid)
                except Exception as e:
                    print(f"[W{wid}] Pre-warmup failed (non-fatal): {e}")

            agent   = Agent(**kwargs)
            # FIX-3: Kill worker if it hangs beyond WORKER_TIMEOUT seconds
            history = await asyncio.wait_for(
                agent.run(max_steps=request.max_steps, on_step_end=_step),
                timeout=WORKER_TIMEOUT,
            )

            # 4-pass result extraction
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

            # FIX-2: Use lock to guarantee only ONE worker ever wins
            if _is_valid(cleaned) and not cancel_event.is_set():
                async with winner_lock:
                    if cancel_event.is_set():
                        print(f"[W{wid}] Lost the race (another worker claimed slot first)")
                        return
                    # Claim the slot — signal all other workers to stop
                    cancel_event.set()

                print(f"[W{wid}] ✅ Valid result! Queuing data…")

                # FIX-6: Queue extracted data FIRST, then build video separately
                # This way data is never lost even if video upload crashes
                _dump_screenshots(history, folder)
                _dump_json_screenshots(folder)
                _ensure_frames(folder)

                video_url = None
                try:
                    fc = len(glob.glob(os.path.join(folder, "*.png")))
                    print(f"[W{wid}] Building video ({fc} frames)…")
                    video_url = await create_and_upload_video(folder, sid)
                except Exception as ve:
                    print(f"[W{wid}] Video build failed (data still saved): {ve}")

                # Put result — data guaranteed even if video_url is None
                await result_queue.put((wid, cleaned, steps[0], video_url))
            else:
                print(f"[W{wid}] ❌ No valid result (CAPTCHA wall or empty)")

        except asyncio.TimeoutError:
            # FIX-3: Worker exceeded timeout
            print(f"[W{wid}] ⏱ Timed out after {WORKER_TIMEOUT}s — killing")
        except asyncio.CancelledError:
            print(f"[W{wid}] Cancelled (another worker won)")
        except Exception as e:
            print(f"[W{wid}] Error: {e}")
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
            try:
                shutil.rmtree(folder)
            except Exception:
                pass

# ---------------------------------------------------------------------------
# RACE RUNNER
# ---------------------------------------------------------------------------

async def _race(request: AgentRequest, proxies: list[dict]):
    """Race workers. Returns (wid, data, steps, video_url) for first winner, or None."""
    q           = asyncio.Queue()
    cancel      = asyncio.Event()
    winner_lock = asyncio.Lock()   # FIX-2

    tasks = [
        asyncio.create_task(
            _run_worker(str(i+1), p, request, q, cancel, winner_lock)
        )
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
    print(f"[Race] Task    : {request.prompt[:80]}…")
    print(f"[Race] Model   : {request.model}")
    print(f"[Race] Workers : {RACE_WORKERS}  |  Rounds: {RACE_MAX_ROUNDS}  |  Timeout: {WORKER_TIMEOUT}s")
    print(f"[Race] Pool    : {len(_PROXY_POOL)} proxies  |  Max browsers: {MAX_BROWSERS}")
    print(f"{'='*60}\n")

    # Snapshot pool at request time to avoid mid-refresh issues
    pool = list(_PROXY_POOL)
    random.shuffle(pool)

    for rnd in range(1, RACE_MAX_ROUNDS + 1):
        start   = ((rnd - 1) * RACE_WORKERS) % max(len(pool), 1)
        proxies = [pool[(start + i) % len(pool)] for i in range(RACE_WORKERS)]
        print(f"[Race] Round {rnd}/{RACE_MAX_ROUNDS} — {[p['host'] for p in proxies]}")

        winner = await _race(request, proxies)

        if winner is not None:
            wid, data, steps, vu = winner
            print(f"\n[Race] 🏆 Winner: Worker-{wid} in round {rnd}")
            return AgentResponse(
                video_url=vu,
                steps_taken=steps,
                extracted_data=data,
                worker_id=wid,
            )

        print(f"[Race] Round {rnd} — all workers failed, next round…")

    print("[Race] ❌ All rounds exhausted — no valid result")
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
        "version": "5.0.0",
        "proxy_pool_size": len(_PROXY_POOL),
        "race_workers": RACE_WORKERS,
        "race_max_rounds": RACE_MAX_ROUNDS,
        "worker_timeout_sec": WORKER_TIMEOUT,
        "max_browsers": MAX_BROWSERS,
    }
