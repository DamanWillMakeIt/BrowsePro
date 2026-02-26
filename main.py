"""
main.py  v6.3 — CAMOUFOX COMPATIBILITY FIX
----------------------------------
CHANGES FROM v6.2:
  ✅ Fixed Camoufox crash: 'dict' object has no attribute 'is_set'
     → browser-use 0.11.13 expects a Playwright browser object, not AsyncCamoufox context
     → Now correctly extracts the underlying playwright browser from Camoufox
  ✅ Warm-up now uses the Camoufox page directly before handing off to agent
  ✅ SSL verify disabled for proxy health check (Bright Data uses self-signed cert)
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

app = FastAPI(title="OnDemand Browser-Use Agent", version="6.3.0")
SCAN_DIR = "scans"
os.makedirs(SCAN_DIR, exist_ok=True)

CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "")
WEBSHARE_API_KEY  = os.getenv("WEBSHARE_API_KEY", "")

# Bright Data credentials — set these in your .env
PROXY_USER        = os.getenv("PROXY_USER", "brd-customer-hl_ea313532-zone-demo")
PROXY_PASS        = os.getenv("PROXY_PASS", "jzbld1hf9ygu")
BD_API_KEY        = os.getenv("BD_API_KEY", "25e73165-8000-4476-b814-6c79af3550c8")
BD_UNLOCKER_ZONE  = os.getenv("BD_UNLOCKER_ZONE", "unlocker")

# Single worker, multiple rounds
RACE_WORKERS      = 1
RACE_MAX_ROUNDS   = 5
WORKER_TIMEOUT    = 300
MAX_BROWSERS      = 1

_browser_semaphore = asyncio.Semaphore(MAX_BROWSERS)

# ---------------------------------------------------------------------------
# USER AGENTS POOL
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

# ---------------------------------------------------------------------------
# VIEWPORT SIZES
# ---------------------------------------------------------------------------
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
]

# ---------------------------------------------------------------------------
# PROXY POOL — Single Bright Data rotating residential endpoint
# ---------------------------------------------------------------------------
_HARDCODED_PROXIES = [
    ("brd.superproxy.io", "33335"),
]

def _make_proxy(host: str, port: str) -> dict:
    return {"host": host, "port": port, "user": PROXY_USER, "pass": PROXY_PASS}

_PROXY_POOL: list[dict] = [_make_proxy(h, p) for h, p in _HARDCODED_PROXIES]
_pool_refreshed_at: datetime = datetime.min

async def _refresh_proxy_pool() -> None:
    # Bright Data handles rotation automatically — no need to fetch pool
    print(f"[ProxyPool] Using Bright Data residential proxy: {_PROXY_POOL[0]['host']}:{_PROXY_POOL[0]['port']}")

def _proxy_browser_dict(p: dict) -> dict:
    return {"server": f"http://{p['host']}:{p['port']}", "username": p["user"], "password": p["pass"]}

def _proxy_httpx_url(p: dict) -> str:
    return f"http://{p['user']}:{p['pass']}@{p['host']}:{p['port']}"

def _proxy_camoufox_dict(p: dict) -> dict:
    return {"server": f"http://{p['host']}:{p['port']}", "username": p["user"], "password": p["pass"]}

print("=" * 60)
print(f"[Deploy] Camoufox          : {'✅' if CAMOUFOX_AVAILABLE else '❌ Chromium fallback'}")
print(f"[Deploy] CAPSOLVER_API_KEY : {'✅' if CAPSOLVER_API_KEY else '❌'}")
print(f"[Deploy] Proxy             : Bright Data Residential ✅")
print(f"[Deploy] Mode              : 1 worker, 5 rounds, 5min timeout")
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
# HUMAN BEHAVIOR SIMULATION
# ---------------------------------------------------------------------------
async def human_mouse_move(page, to_x: int, to_y: int) -> None:
    """Bezier curve mouse movement"""
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
    """Human-like scrolling with acceleration curve (not uniform)"""
    try:
        scroll_steps = random.randint(8, 15)
        for i in range(scroll_steps):
            # Acceleration curve — speeds up then slows down like a real human
            progress = i / scroll_steps
            ease = progress * (2 - progress)  # ease-in-out
            step = (distance / scroll_steps) * ease * random.uniform(0.8, 1.2)
            await page.evaluate(f"window.scrollBy(0, {step})")
            await asyncio.sleep(random.uniform(0.05, 0.2))
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

    for i, site in enumerate(sites[:3]):
        try:
            print(f"[W{wid}] Warm-up {i+1}/3: {site}")
            nav = getattr(page, "goto", None) or getattr(page, "navigate", None)
            if nav:
                await nav(site, timeout=20000)
                await human_delay_short()
                await human_scroll(page, random.randint(200, 600))
                await human_mouse_move(page, random.randint(300, 1200), random.randint(200, 700))
                await asyncio.sleep(random.uniform(2.0, 4.0))
        except Exception as e:
            print(f"[W{wid}] Warm-up {i+1} failed (non-fatal): {e}")

    print(f"[W{wid}] ✅ Extended warm-up complete (3 pages)")

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
# BRIGHT DATA WEB UNLOCKER — fallback for heavily protected sites
# ---------------------------------------------------------------------------
async def _fetch_via_unlocker(url: str) -> str | None:
    """Fetch a URL via Bright Data Web Unlocker API — bypasses any bot protection."""
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
                print(f"[Unlocker] ✅ Got {len(r.text)} chars")
                return r.text
            else:
                print(f"[Unlocker] ❌ Status {r.status_code}: {r.text[:200]}")
                return None
    except Exception as e:
        print(f"[Unlocker] Error: {e}")
        return None

async def _is_blocked(page) -> bool:
    """Check if current page is blocked/captcha'd."""
    try:
        url = await _page_url(page)
        if "browser_check" in url or "captcha" in url.lower():
            return True
        html = await page.content()
        blocked_signals = [
            "Just a moment", "cf-browser-verification", "browser_check",
            "wrong captcha", "Access Denied", "403 Forbidden",
            "Please verify you are a human",
        ]
        return any(s in html for s in blocked_signals)
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
                        await asyncio.sleep(5)
                        nf = await _page_frames(page)
                        if not any("bframe" in (await _frame_url(x)) for x in nf):
                            print("[CAPTCHA] reCAPTCHA checkbox passed ✅")
                            return
                except Exception:
                    pass
                break

        if not rc_key:
            m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
            if m:
                rc_key = m.group(1)

        if rc_key and "6L" in rc_key:
            print(f"[CAPTCHA] reCAPTCHA v2 detected (key: {rc_key[:20]}...) — solving…")
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
                    const textarea = document.querySelector('textarea[name="g-recaptcha-response"]');
                    if (textarea) {
                        const form = textarea.closest('form');
                        if (form) { setTimeout(() => { try { form.submit(); } catch(e) {} }, 500); }
                    }
                    const submitBtn = document.querySelector('button[type="submit"], input[type="submit"]');
                    if (submitBtn) { setTimeout(() => submitBtn.click(), 1000); }
                }""", t)
                print("[CAPTCHA] reCAPTCHA token injected ✅")
                await asyncio.sleep(5)
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
            camoufox_proxy = {
                "server": f"http://{proxy['host']}:{proxy['port']}",
                "username": proxy['user'],
                "password": proxy['pass'],
            }
            # Launch Camoufox and get the underlying playwright browser
            # browser-use 0.11.13 needs a raw playwright Browser object, not AsyncCamoufox context
            camoufox_ctx = AsyncCamoufox(
                headless=True,
                os="windows",
                proxy=camoufox_proxy,
                geoip=True,
                humanize=0.5,
                screen={"width": viewport['width'], "height": viewport['height']},
            )
            # __aenter__ returns the playwright browser directly
            playwright_browser = await camoufox_ctx.__aenter__()
            
            # Open a warm-up page
            warm_page = await playwright_browser.new_page()
            
            print(f"[W{wid}] 🦊 Camoufox launched ({viewport['width']}x{viewport['height']})")
            # Return ctx for cleanup, warm_page for warm-up, and the raw browser for agent
            return camoufox_ctx, warm_page, playwright_browser
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
            return b, None, None  # no warm_page, no raw browser for Chromium path
        except Exception as e:
            print(f"[W{wid}] Chromium also failed: {e}")
    return None, None, None

async def _close_browser(browser_obj, is_camoufox: bool) -> None:
    if browser_obj is None:
        return
    try:
        if is_camoufox:
            await browser_obj.__aexit__(None, None, None)
        else:
            try:
                await browser_obj.close()
            except Exception:
                pass
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
            page = await bs.get_current_page()
            if page is None:
                return

            # Human behavior every 3 steps
            if n % 3 == 0:
                await human_scroll(page, random.randint(200, 400))
                await human_mouse_move(page, random.randint(400, 1400), random.randint(300, 800))

            await _solve_captcha(page, proxy)

            # Web Unlocker fallback — if still blocked after captcha solve attempt
            if await _is_blocked(page):
                current_url = await _page_url(page)
                print(f"[W{wid}] 🔓 Blocked detected — trying Web Unlocker for {current_url}")
                html = await _fetch_via_unlocker(current_url)
                if html:
                    # Inject the unblocked HTML directly into the page
                    escaped = html.replace('`', '\\`').replace('$', '\\$')
                    await page.evaluate(f"""() => {{
                        document.open();
                        document.write(`{escaped[:500000]}`);
                        document.close();
                    }}""")
                    print(f"[W{wid}] ✅ Unlocker HTML injected")

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
            browser_obj, warm_page, raw_browser = await _create_browser_and_page(proxy, wid)
            is_camoufox = raw_browser is not None  # raw_browser only set for Camoufox path

            # Run warm-up to establish realistic session
            if warm_page:
                await _warmup_extended(warm_page, wid)
                await warm_page.close()

            kwargs: dict = dict(
                task=_wrap_prompt(request.prompt), llm=llm,
                save_conversation_path=folder, max_actions_per_step=1,
                use_vision=True, max_failures=3, retry_delay=2,
            )

            # Pass the correct browser object to the agent
            if is_camoufox and raw_browser is not None:
                # For Camoufox: pass the raw playwright browser
                # browser-use expects a Browser wrapper, so wrap it
                if BrowserConfig and Browser:
                    try:
                        brd_proxy = _proxy_browser_dict(proxy)
                        cfg = BrowserConfig(proxy=brd_proxy)
                        bu_browser = Browser(config=cfg)
                        bu_browser._playwright_browser = raw_browser
                        kwargs["browser"] = bu_browser
                    except Exception:
                        kwargs["browser"] = browser_obj
                else:
                    kwargs["browser"] = browser_obj
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
    print(f"[Agent] Task   : {request.prompt[:80]}…")
    print(f"[Agent] Browser: {'Camoufox 🦊' if CAMOUFOX_AVAILABLE else 'Chromium'}")
    print(f"[Agent] Proxy  : Bright Data Residential ✅")
    print(f"{'='*60}\n")

    pool = list(_PROXY_POOL)

    for rnd in range(1, RACE_MAX_ROUNDS + 1):
        proxies = [pool[0]]  # Single Bright Data endpoint — it rotates IPs automatically
        print(f"[Agent] Round {rnd}/{RACE_MAX_ROUNDS}")

        winner = await _race(request, proxies)

        if winner is not None:
            wid, data, steps, vu = winner
            print(f"\n[Agent] 🏆 Success in round {rnd}")
            return AgentResponse(
                video_url=vu,
                steps_taken=steps,
                extracted_data=data,
                worker_id=wid,
            )

        print(f"[Agent] Round {rnd} failed, retrying with fresh IP…")

    print("[Agent] ❌ All rounds exhausted")
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
        "version": "6.3.0",
        "browser": "camoufox" if CAMOUFOX_AVAILABLE else "chromium-fallback",
        "proxy": "bright-data-residential",
        "mode": "single-worker-warmup-enabled",
    }
