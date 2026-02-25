"""
main.py
-------
FastAPI wrapper around browser-use 0.11.x.

Run:
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
"""

from __future__ import annotations
from typing import Any

import asyncio
import base64
import glob
import json
import os
import random
import re as _re
import shutil
import uuid
from datetime import datetime
from pathlib import Path

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

app = FastAPI(title="OnDemand Browser-Use Agent", version="1.0.0")

SCAN_DIR = "scans"
os.makedirs(SCAN_DIR, exist_ok=True)

CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY", "")

# ---------------------------------------------------------------------------
# RESIDENTIAL PROXY POOL — single source of truth
# ---------------------------------------------------------------------------
PROXY_POOL = [
    {"host": os.getenv("PROXY_1_HOST", "104.252.62.99"),  "port": os.getenv("PROXY_1_PORT", "5470"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_2_HOST", "45.248.55.14"),   "port": os.getenv("PROXY_2_PORT", "6600"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_3_HOST", "103.130.178.57"), "port": os.getenv("PROXY_3_PORT", "5721"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_4_HOST", "82.22.181.141"),  "port": os.getenv("PROXY_4_PORT", "7852"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_5_HOST", "192.46.188.160"), "port": os.getenv("PROXY_5_PORT", "5819"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
]

# Single active proxy per session — set once in run_agent, shared by browser + CapSolver
_ACTIVE_PROXY: dict = PROXY_POOL[0]


def _pick_proxy() -> dict:
    """Pick a random proxy from the pool."""
    return random.choice(PROXY_POOL)


def _proxy_server_url(proxy: dict) -> str:
    """Returns http://user:pass@host:port for Playwright BrowserConfig."""
    return f"http://{proxy['user']}:{proxy['pass']}@{proxy['host']}:{proxy['port']}"


# Persistent browser profile directory
PROFILE_DIR = os.path.join(os.getcwd(), "browser_profile")
os.makedirs(PROFILE_DIR, exist_ok=True)

# playwright-stealth
try:
    from playwright_stealth import stealth_async as _stealth_async
    STEALTH_LIB_AVAILABLE = True
    print("[Stealth] playwright-stealth available ✅")
except ImportError:
    STEALTH_LIB_AVAILABLE = False
    print("[Stealth] playwright-stealth not installed — using JS-only stealth")
    print("[Stealth] Run: pip install playwright-stealth")


# ---------------------------------------------------------------------------
# STEALTH SCRIPT
# ---------------------------------------------------------------------------
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [
    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
    { name: 'Native Client',     filename: 'internal-nacl-plugin',                description: '' },
]});
Object.defineProperty(navigator, 'languages',           { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'platform',            { get: () => 'Win32' });
Object.defineProperty(navigator, 'vendor',              { get: () => 'Google Inc.' });
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
Object.defineProperty(navigator, 'deviceMemory',        { get: () => 8 });
window.chrome = { runtime: {}, loadTimes: () => ({}), csi: () => ({}), app: {} };
const _origQuery = window.navigator.permissions.query.bind(navigator.permissions);
window.navigator.permissions.query = (p) =>
    p.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : _origQuery(p);
const _origToDataURL = HTMLCanvasElement.prototype.toDataURL;
HTMLCanvasElement.prototype.toDataURL = function(type, ...a) {
    const ctx = this.getContext('2d');
    if (ctx) { const d = ctx.getImageData(0,0,this.width||1,this.height||1); d.data[0]^=1; ctx.putImageData(d,0,0); }
    return _origToDataURL.call(this, type, ...a);
};
const _origGP = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(p) {
    if (p===37445) return 'Intel Inc.'; if (p===37446) return 'Intel Iris OpenGL Engine';
    return _origGP.call(this,p);
};
"""


# ---------------------------------------------------------------------------
# SAFE PAGE HELPERS — handle both property and coroutine Playwright versions
# ---------------------------------------------------------------------------

async def _page_url(page) -> str:
    """Safely get page URL regardless of Playwright version."""
    try:
        url = page.url
        if asyncio.iscoroutine(url):
            url = await url
        return url or ""
    except Exception:
        try:
            return await page.evaluate("() => window.location.href")
        except Exception:
            return ""


async def _page_frames(page) -> list:
    """Safely get page frames regardless of Playwright version."""
    try:
        frames = page.frames
        if asyncio.iscoroutine(frames):
            frames = await frames
        return frames or []
    except Exception:
        return []


async def _frame_url(frame) -> str:
    """Safely get frame URL regardless of Playwright version."""
    try:
        url = frame.url
        if asyncio.iscoroutine(url):
            url = await url
        return url or ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# CAPTCHA SOLVER
# ---------------------------------------------------------------------------

async def _capsolver_solve(task: dict, proxy: dict | None = None) -> dict | None:
    """Solve a CAPTCHA via CapSolver using the active session proxy."""
    if not CAPSOLVER_API_KEY:
        print("[CapSolver] No API key configured — skipping")
        return None

    if proxy:
        task["proxyType"]     = "http"
        task["proxyAddress"]  = proxy["host"]
        task["proxyPort"]     = int(proxy["port"])
        task["proxyLogin"]    = proxy["user"]
        task["proxyPassword"] = proxy["pass"]
        task["type"]          = task["type"].replace("ProxyLess", "")

    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r    = await c.post("https://api.capsolver.com/createTask",
                                json={"clientKey": CAPSOLVER_API_KEY, "task": task})
            data = r.json()
            if data.get("errorId") != 0:
                print(f"[CapSolver] Error: {data.get('errorDescription')}")
                return None
            task_id = data["taskId"]
        async with httpx.AsyncClient(timeout=120) as c:
            for _ in range(60):
                await asyncio.sleep(2)
                r = await c.post("https://api.capsolver.com/getTaskResult",
                                 json={"clientKey": CAPSOLVER_API_KEY, "taskId": task_id})
                d = r.json()
                if d.get("status") == "ready":
                    return d.get("solution", {})
                if d.get("status") == "failed":
                    print("[CapSolver] Task failed")
                    return None
    except Exception as exc:
        print(f"[CapSolver] Exception: {exc}")
    return None


async def detect_and_solve_captcha(page) -> None:
    """Detect and solve Turnstile / reCAPTCHA v2 / hCaptcha on the current page."""
    try:
        try:
            html = await page.content()
        except Exception:
            try:
                html = await page.evaluate("() => document.documentElement.outerHTML")
            except Exception:
                return

        # Safe URL and frames access — fixes 'Page has no attribute url/frames'
        page_url = await _page_url(page)
        frames   = await _page_frames(page)
        proxy    = _ACTIVE_PROXY

        # ── Cloudflare Turnstile ──────────────────────────────────────────
        ts_key = None
        for frame in frames:
            frame_url = await _frame_url(frame)
            if "challenges.cloudflare.com" in frame_url or "turnstile" in frame_url.lower():
                m = _re.search(r'[?&]k=([^&]+)', frame_url)
                if m:
                    ts_key = m.group(1)
                break
        if not ts_key:
            m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
            if m and ("cf-turnstile" in html or "turnstile" in html.lower()):
                ts_key = m.group(1)
        if ts_key:
            print(f"[CAPTCHA] Turnstile detected — solving…")
            sol = await _capsolver_solve({
                "type":       "AntiTurnstileTask",
                "websiteURL": page_url,
                "websiteKey": ts_key,
            }, proxy=proxy)
            if sol:
                token = sol.get("token", "")
                await page.evaluate("""(t) => {
                    document.querySelectorAll('input[name*="cf-turnstile-response"],input[name*="turnstile"]')
                        .forEach(el => { el.value=t; el.dispatchEvent(new Event('change',{bubbles:true})); });
                    const el = document.querySelector('.cf-turnstile,[data-sitekey]');
                    if (el) { const cb=el.getAttribute('data-callback'); if(cb&&window[cb]) try{window[cb](t);}catch(e){} }
                }""", token)
                print("[CAPTCHA] Turnstile injected ✅")
            return

        # ── Cloudflare JS challenge ───────────────────────────────────────
        if "Just a moment" in html or "cf-browser-verification" in html:
            print("[CAPTCHA] Cloudflare JS challenge — waiting up to 15s…")
            for _ in range(15):
                await asyncio.sleep(1)
                new_html = await page.content()
                if "Just a moment" not in new_html:
                    print("[CAPTCHA] Cloudflare cleared ✅")
                    return
            print("[CAPTCHA] Cloudflare challenge persists")
            return

        # ── reCAPTCHA v2 ─────────────────────────────────────────────────
        rc_key = None
        for frame in frames:
            frame_url = await _frame_url(frame)
            if "recaptcha" in frame_url and "anchor" in frame_url:
                m = _re.search(r'[?&]k=([^&]+)', frame_url)
                if m:
                    rc_key = m.group(1)
                try:
                    cb = frame.locator(".recaptcha-checkbox-border").first
                    if await cb.count() > 0:
                        await cb.click(timeout=3000)
                        await asyncio.sleep(3)
                        # Re-fetch frames after click
                        new_frames = await _page_frames(page)
                        if not any("bframe" in (await _frame_url(f)) for f in new_frames):
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
            print("[CAPTCHA] reCAPTCHA v2 detected — solving…")
            sol = await _capsolver_solve({
                "type":       "ReCaptchaV2Task",
                "websiteURL": page_url,
                "websiteKey": rc_key,
            }, proxy=proxy)
            if sol:
                token = sol.get("gRecaptchaResponse", "")
                await page.evaluate("""(t) => {
                    document.querySelectorAll('[name="g-recaptcha-response"]')
                        .forEach(el => { el.innerHTML=t; el.value=t; el.style.display='block'; });
                    document.querySelectorAll('[data-callback]').forEach(el => {
                        const cb=el.getAttribute('data-callback');
                        if(cb&&window[cb]) try{window[cb](t);}catch(e){}
                    });
                    const ta=document.querySelector('textarea[name="g-recaptcha-response"]');
                    if(ta){const f=ta.closest('form');if(f)try{f.submit();}catch(e){}}
                }""", token)
                print("[CAPTCHA] reCAPTCHA v2 injected ✅")
            return

        # ── hCaptcha ──────────────────────────────────────────────────────
        if "hcaptcha" in html.lower():
            m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html)
            if m:
                print("[CAPTCHA] hCaptcha detected — solving…")
                sol = await _capsolver_solve({
                    "type":       "HCaptchaTask",
                    "websiteURL": page_url,
                    "websiteKey": m.group(1),
                }, proxy=proxy)
                if sol:
                    token = sol.get("gRecaptchaResponse", "")
                    await page.evaluate("""(t) => {
                        const ta=document.querySelector('[name="h-captcha-response"]');
                        if(ta){ta.innerHTML=t;ta.value=t;}
                        document.querySelectorAll('[data-callback]').forEach(el=>{
                            const cb=el.getAttribute('data-callback');
                            if(cb&&window[cb])try{window[cb](t);}catch(e){}
                        });
                    }""", token)
                    print("[CAPTCHA] hCaptcha injected ✅")

    except Exception as exc:
        print(f"[CAPTCHA] detect_and_solve error: {exc}")


# ---------------------------------------------------------------------------
# HUMAN-LIKE BEHAVIOUR
# ---------------------------------------------------------------------------

async def human_delay(min_ms: int = 500, max_ms: int = 2000) -> None:
    await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


async def apply_stealth_to_page(page) -> None:
    if STEALTH_LIB_AVAILABLE:
        try:
            await _stealth_async(page)
        except Exception as exc:
            print(f"[Stealth] playwright-stealth failed: {exc}")
    try:
        await page.add_init_script(STEALTH_SCRIPT)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class AgentRequest(BaseModel):
    prompt: str
    max_steps: int = 50
    model: str = "gpt-5.1"


class AgentResponse(BaseModel):
    video_url: str | None = None
    steps_taken: int = 0
    extracted_data: Any = None


# ---------------------------------------------------------------------------
# Placeholder frame
# ---------------------------------------------------------------------------

def _ensure_minimum_frames(folder: str) -> None:
    if glob.glob(os.path.join(folder, "*.png")):
        return
    print("[Frames] No screenshots — writing 1920×1080 placeholder.")
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
    except Exception as exc:
        print(f"[Frames] Pillow placeholder failed ({exc}), writing raw PNG.")
        import struct, zlib
        def _chunk(tag: bytes, data: bytes) -> bytes:
            crc = zlib.crc32(tag + data) & 0xFFFFFFFF
            return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)
        raw = (
            b'\x89PNG\r\n\x1a\n'
            + _chunk(b'IHDR', struct.pack('>IIBBBBB', 1, 1, 8, 2, 0, 0, 0))
            + _chunk(b'IDAT', zlib.compress(b'\x00\xff\xff\xff'))
            + _chunk(b'IEND', b'')
        )
        with open(path, "wb") as f:
            f.write(raw)


# ---------------------------------------------------------------------------
# Screenshot extractors
# ---------------------------------------------------------------------------

def _dump_history_screenshots(history, folder: str) -> int:
    saved = 0

    def _save(raw: str, label: str) -> bool:
        nonlocal saved
        if not raw:
            return False
        if isinstance(raw, str) and "," in raw:
            raw = raw.split(",", 1)[1]
        try:
            img_bytes = base64.b64decode(raw)
            if not (img_bytes[:4] == b'\x89PNG' or img_bytes[:2] == b'\xff\xd8'):
                return False
            path = os.path.join(folder, f"{label}.png")
            with open(path, "wb") as fh:
                fh.write(img_bytes)
            saved += 1
            return True
        except Exception:
            return False

    try:
        for i, result in enumerate(getattr(history, "all_results", []) or []):
            for attr in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                if _save(getattr(result, attr, None), f"step_{i+1:04d}_result"):
                    break
        for i, h in enumerate(getattr(history, "history", []) or []):
            state = getattr(h, "state", None)
            if state is not None:
                for attr in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                    if _save(getattr(state, attr, None), f"step_{i+1:04d}_state"):
                        break
            for attr in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                if _save(getattr(h, attr, None), f"step_{i+1:04d}_h"):
                    break
    except Exception as exc:
        print(f"[History] Object extraction failed: {exc}")

    return saved


def _dump_json_screenshots(folder: str) -> int:
    saved   = 0
    pattern = os.path.join(folder, "conversation_*.json")
    files   = sorted(glob.glob(pattern))
    if not files:
        return 0
    print(f"[JSON] Found {len(files)} conversation JSON file(s)")
    for json_path in files:
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        messages = data if isinstance(data, list) else data.get("messages", [])
        for msg in messages:
            content = msg.get("content", [])
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                img_url = (block.get("image_url") or {}).get("url", "")
                source  = block.get("source") or {}
                raw     = ""
                if img_url.startswith("data:image"):
                    raw = img_url.split(",", 1)[1] if "," in img_url else ""
                elif source.get("type") == "base64":
                    raw = source.get("data", "")
                if not raw:
                    continue
                try:
                    img_bytes = base64.b64decode(raw)
                    if not (img_bytes[:4] == b'\x89PNG' or img_bytes[:2] == b'\xff\xd8'):
                        continue
                    stem  = os.path.splitext(os.path.basename(json_path))[0]
                    fname = f"{stem}_img{saved+1:03d}.png"
                    out   = os.path.join(folder, fname)
                    with open(out, "wb") as fh:
                        fh.write(img_bytes)
                    saved += 1
                except Exception:
                    pass
    print(f"[JSON] Total screenshots extracted: {saved}")
    return saved


# ---------------------------------------------------------------------------
# Prompt wrapper
# ---------------------------------------------------------------------------

def _wrap_prompt(user_prompt: str) -> str:
    return f"""You are a browser automation agent. Execute the following task:

{user_prompt}

=== CRITICAL RULES FOR ADDING AGENT TOOLS ===
When you need to add a tool via the 'Add Agent Tools' modal, follow these rules EXACTLY:

RULE 1 — ADDING A TOOL:
- After clicking the '+' button inside a tool card, you MUST wait 2 seconds and look for a GREEN TOAST notification that says "Agent Tool added successfully".
- If you see the toast → the tool was added. Do NOT click '+' again. Proceed to close the modal.
- If you do NOT see the toast after 2 seconds → the click failed. Try clicking the '+' button ONE more time.
- NEVER click '+' more than twice total. After 2 attempts, close the modal and move on.

RULE 2 — JAVASCRIPT CLICK FALLBACK:
- If you see "Could not get element geometry" warnings, the button was clicked via JavaScript.
- JavaScript clicks on this site DO register — trust them. Wait for the toast before assuming failure.

RULE 3 — DO NOT REOPEN THE MODAL:
- Once you have closed the 'Add Agent Tools' modal, do NOT reopen it.
- Even if Agent Tools sidebar still shows "No Agent Tools Added" briefly, that is a UI refresh delay — do NOT reopen the modal.

RULE 4 — PROCEED AFTER CLOSE:
- After closing the modal, immediately go to the main chat input and type the prompt.
- Do not look back at the Agent Tools sidebar.
=== END CRITICAL RULES ===
"""


# ---------------------------------------------------------------------------
# Step callback — stealth + captcha + screenshot
# ---------------------------------------------------------------------------

def make_screenshot_callback(folder: str, counter: list[int]):
    async def _callback(agent) -> None:
        counter[0] += 1
        n = counter[0]
        try:
            browser_session = getattr(agent, "browser_session", None)
            if browser_session is None:
                return
            page = await browser_session.get_current_page()
            if page is None:
                return

            await apply_stealth_to_page(page)
            await detect_and_solve_captcha(page)
            await human_delay(300, 1200)

            img_b64   = await page.screenshot()
            img_bytes = base64.b64decode(img_b64)
            path      = os.path.join(folder, f"step_{n:04d}_cb.png")
            with open(path, "wb") as fh:
                fh.write(img_bytes)
            print(f"[Callback] step {n:03d} → {path}")
        except Exception as exc:
            print(f"[Callback] step {n} screenshot failed: {exc}")

    return _callback


# ---------------------------------------------------------------------------
# LLM builder
# ---------------------------------------------------------------------------

def build_llm(model: str, api_key: str):
    try:
        from browser_use.llm import ChatOpenAI as BU
        return BU(model=model, api_key=api_key)
    except Exception:
        pass
    try:
        from browser_use.agent.llm import ChatOpenAI as BU2
        return BU2(model=model, api_key=api_key)
    except Exception:
        pass
    try:
        from openai import AsyncOpenAI
        return AsyncOpenAI(api_key=api_key)
    except Exception:
        pass
    raise RuntimeError("Could not build LLM")


# ---------------------------------------------------------------------------
# Result cleaner
# ---------------------------------------------------------------------------

def _clean_result(text: str) -> str:
    if not text:
        return text
    text = text.strip()
    import re as _r
    result_block = _r.search(r'<r>\s*(.*?)\s*</r>', text, _r.DOTALL)
    if result_block:
        text = result_block.group(1).strip()
    fenced = _r.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', text, _r.DOTALL)
    if fenced:
        text = fenced.group(1).strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    return text


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@app.post("/agent/run", response_model=AgentResponse)
async def run_agent(request: AgentRequest) -> AgentResponse:
    global _ACTIVE_PROXY

    session_id  = str(uuid.uuid4())[:8]
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"{SCAN_DIR}/{timestamp}_{session_id}"
    os.makedirs(folder_name, exist_ok=True)

    # Pick ONE proxy for this entire session — browser + CapSolver both use it
    _ACTIVE_PROXY = _pick_proxy()

    print(f"\n{'='*60}")
    print(f"[Agent] Session   : {session_id}")
    print(f"[Agent] Task      : {request.prompt}")
    print(f"[Agent] Model     : {request.model}")
    print(f"[Agent] Max steps : {request.max_steps}")
    print(f"[Proxy] Session proxy: {_ACTIVE_PROXY['host']}:{_ACTIVE_PROXY['port']}")
    print(f"{'='*60}\n")

    llm          = build_llm(request.model, os.getenv("OPENAI_API_KEY", ""))
    step_counter = [0]
    on_step_end  = make_screenshot_callback(folder_name, step_counter)

    agent_kwargs: dict = dict(
        task=_wrap_prompt(request.prompt),
        llm=llm,
        save_conversation_path=folder_name,
        max_actions_per_step=1,
        use_vision=True,
        max_failures=3,
        retry_delay=2,
    )

    browser = None
    if BrowserConfig is not None and Browser is not None:
        try:
            proxy_url = _proxy_server_url(_ACTIVE_PROXY)
            print(f"[Proxy] Browser proxy URL: {_ACTIVE_PROXY['host']}:{_ACTIVE_PROXY['port']}")

            browser_cfg = BrowserConfig(
                headless=True,
                proxy=proxy_url,
                extra_chromium_args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1920,1080",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--password-store=basic",
                    "--use-mock-keychain",
                    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/131.0.0.0 Safari/537.36',
                ],
            )
            browser = Browser(config=browser_cfg)
            agent_kwargs["browser"] = browser
            print("[Agent] BrowserConfig applied ✅")
        except Exception as e:
            print(f"[Agent] BrowserConfig failed ({e}), using defaults")

    agent        = Agent(**agent_kwargs)
    result_text  = ""
    final_status = "success"
    history      = None

    try:
        history     = await agent.run(max_steps=request.max_steps, on_step_end=on_step_end)
        result_text = ""
        try:
            all_results = history.all_results or []
            for action in reversed(all_results):
                if getattr(action, 'is_done', False):
                    raw = getattr(action, 'extracted_content', '') or ''
                    if raw:
                        result_text = raw
                        break
            if not result_text:
                for out in reversed(history.all_model_outputs or []):
                    done_block = out.get('done', {}) if isinstance(out, dict) else {}
                    if done_block.get('text'):
                        result_text = done_block['text']
                        break
            if not result_text:
                skip = ('🔗', '🔍', 'Clicked', 'Typed', 'Waited', 'Scrolled', 'Searched')
                for action in reversed(all_results):
                    text = getattr(action, 'extracted_content', '') or ''
                    if text and not any(text.startswith(s) for s in skip):
                        result_text = text
                        break
        except Exception:
            result_text = ""

        if result_text:
            import re as _re2
            json_match = _re2.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', result_text, _re2.DOTALL)
            if json_match:
                try:
                    parsed      = json.loads(json_match.group(1))
                    result_text = json.dumps(parsed, ensure_ascii=False, indent=2)
                except Exception:
                    pass

        print(f"[Agent] ✅ Completed in {step_counter[0]} steps")

    except Exception as exc:
        import traceback
        traceback.print_exc()
        result_text  = f"Agent error: {exc}"
        final_status = "failed"
        print(f"[Agent] ❌ Failed: {exc}")

    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

    if history is not None:
        _dump_history_screenshots(history, folder_name)
    _dump_json_screenshots(folder_name)

    steps_taken = step_counter[0] or (
        len(getattr(history, "all_results", []) or []) if history else 0
    )

    _ensure_minimum_frames(folder_name)

    frame_count = len(glob.glob(os.path.join(folder_name, "*.png")))
    print(f"[Agent] Building video from {frame_count} screenshot(s)…")
    video_url = await create_and_upload_video(folder_name, session_id)
    print(f"[Agent] Video URL : {video_url}")

    try:
        shutil.rmtree(folder_name)
        print(f"[Cleanup] Deleted scan folder: {folder_name}")
    except Exception as exc:
        print(f"[Cleanup] Could not delete scan folder: {exc}")

    return AgentResponse(
        video_url=video_url,
        steps_taken=steps_taken,
        extracted_data=_clean_result(result_text) or None,
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": "1.0.0"}
