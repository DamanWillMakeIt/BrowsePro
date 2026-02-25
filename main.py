"""
main.py
-------
FastAPI wrapper around browser-use 0.11.x.

Run:
    uvicorn main:app --reload --host 0.0.0.0 --port 8000

Fixes in this version:
  1. Unwrap real Playwright page from browser-use wrapper before calling stealth/add_init_script
     Root cause: browser-use passes a wrapper object, not a raw Playwright Page.
     playwright-stealth and add_init_script need the real page — we extract it via
     page.page, page._page, or context.pages[-1] fallbacks.
  2. Proxy verification moved into step 1 of callback (browser_session ready by then)
  3. proxy dict format (server/username/password) — not URL string
  4. Removed --single-process / --disable-extensions / --no-zygote (break proxy auth)
  5. page.screenshot() returns bytes — no base64.b64decode needed
  6. _capsolver_solve shallow-copies task dict before mutation
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
import sys
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
# PROXY POOL — single source of truth
# ---------------------------------------------------------------------------
PROXY_POOL = [
    {"host": os.getenv("PROXY_1_HOST", "104.252.62.99"),  "port": os.getenv("PROXY_1_PORT", "5470"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_2_HOST", "45.248.55.14"),   "port": os.getenv("PROXY_2_PORT", "6600"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_3_HOST", "103.130.178.57"), "port": os.getenv("PROXY_3_PORT", "5721"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_4_HOST", "82.22.181.141"),  "port": os.getenv("PROXY_4_PORT", "7852"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
    {"host": os.getenv("PROXY_5_HOST", "192.46.188.160"), "port": os.getenv("PROXY_5_PORT", "5819"),  "user": os.getenv("PROXY_USER", "hgfumqbe"), "pass": os.getenv("PROXY_PASS", "t8a93hs91l3r")},
]

_ACTIVE_PROXY: dict = PROXY_POOL[0]


def _pick_proxy() -> dict:
    return random.choice(PROXY_POOL)


def _proxy_browser_dict(proxy: dict) -> dict:
    """
    Dict format Playwright BrowserConfig actually accepts.
    A plain URL string is silently ignored — browser launches with no proxy.
    """
    return {
        "server":   f"http://{proxy['host']}:{proxy['port']}",
        "username": proxy["user"],
        "password": proxy["pass"],
    }


# ---------------------------------------------------------------------------
# PERSISTENT PROFILE
# ---------------------------------------------------------------------------
PROFILE_DIR = os.path.join(os.getcwd(), "browser_profile")
os.makedirs(PROFILE_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# PLAYWRIGHT-STEALTH
# ---------------------------------------------------------------------------
try:
    from playwright_stealth import stealth_async as _stealth_async
    STEALTH_LIB_AVAILABLE = True
    print("[Stealth] playwright-stealth available ✅")
except ImportError:
    STEALTH_LIB_AVAILABLE = False
    print("[Stealth] playwright-stealth not installed — using JS-only stealth")
    print("[Stealth] Run: pip install playwright-stealth")

# ---------------------------------------------------------------------------
# DEPLOY VERIFICATION
# ---------------------------------------------------------------------------
print("=" * 60)
print(f"[Deploy] Python            : {sys.version}")
print(f"[Deploy] playwright-stealth: {'INSTALLED ✅' if STEALTH_LIB_AVAILABLE else 'MISSING ❌'}")
print(f"[Deploy] CAPSOLVER_API_KEY : {'SET ✅' if CAPSOLVER_API_KEY else 'NOT SET ❌'}")
print(f"[Deploy] Proxy pool size   : {len(PROXY_POOL)}")
print("=" * 60)

# ---------------------------------------------------------------------------
# JS STEALTH SCRIPT
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
# REAL PAGE UNWRAPPER
#
# browser-use wraps Playwright pages in its own objects.
# playwright-stealth and add_init_script need the REAL Playwright Page.
# We try several known attribute paths to unwrap it.
# ---------------------------------------------------------------------------

async def _unwrap_page(page):
    """
    Extract the real Playwright Page from a browser-use wrapper.
    Returns the raw page, or the original object if unwrapping fails.
    Tries: page.page, page._page, page._playwright_page,
           then context.pages[-1] as last resort.
    """
    # Try direct attribute unwrap
    for attr in ("page", "_page", "_playwright_page", "playwright_page"):
        inner = getattr(page, attr, None)
        if inner is not None and hasattr(inner, "add_init_script"):
            print(f"[Stealth] Unwrapped real page via .{attr} ✅")
            return inner

    # If the object itself already has add_init_script, it's already raw
    if hasattr(page, "add_init_script"):
        return page

    # Last resort: grab from browser context
    try:
        context = getattr(page, "context", None) or getattr(page, "_context", None)
        if context is not None:
            pages = context.pages
            if asyncio.iscoroutine(pages):
                pages = await pages
            if pages:
                real = pages[-1]
                if hasattr(real, "add_init_script"):
                    print("[Stealth] Unwrapped real page via context.pages[-1] ✅")
                    return real
    except Exception:
        pass

    print("[Stealth] ⚠️ Could not unwrap page — stealth may not apply")
    return page


# ---------------------------------------------------------------------------
# SAFE PAGE URL / FRAMES HELPERS
# ---------------------------------------------------------------------------

async def _page_url(page) -> str:
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
    try:
        frames = page.frames
        if asyncio.iscoroutine(frames):
            frames = await frames
        return frames or []
    except Exception:
        return []


async def _frame_url(frame) -> str:
    try:
        url = frame.url
        if asyncio.iscoroutine(url):
            url = await url
        return url or ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# PROXY VERIFICATION
# ---------------------------------------------------------------------------

async def _verify_proxy(page, expected_proxy: dict) -> None:
    """Navigate to ipinfo.io/json and log the exit IP to confirm proxy is active."""
    try:
        real_page = await _unwrap_page(page)
        print("[ProxyCheck] Navigating to https://ipinfo.io/json …")
        await real_page.goto("https://ipinfo.io/json", timeout=15000)
        await asyncio.sleep(2)
        try:
            content = await real_page.evaluate("() => document.body.innerText")
        except Exception:
            content = await real_page.content()

        print(f"[ProxyCheck] Expected proxy : {expected_proxy['host']}:{expected_proxy['port']}")
        print(f"[ProxyCheck] Response       : {content[:300]}")
        try:
            info = json.loads(content)
            reported_ip = info.get("ip", "unknown")
            print(f"[ProxyCheck] Exit IP: {reported_ip}")
            if reported_ip == expected_proxy["host"]:
                print("[ProxyCheck] 🟢 Proxy IP matches — proxy ACTIVE")
            else:
                print("[ProxyCheck] 🟡 Exit IP differs from proxy host (normal for rotating residential proxies)")
        except Exception:
            print(f"[ProxyCheck] Non-JSON response: {content[:200]}")
    except Exception as exc:
        print(f"[ProxyCheck] Error (non-fatal): {exc}")


# ---------------------------------------------------------------------------
# CAPSOLVER
# ---------------------------------------------------------------------------

async def _capsolver_solve(task: dict, proxy: dict | None = None) -> dict | None:
    if not CAPSOLVER_API_KEY:
        print("[CapSolver] No API key — skipping")
        return None

    task = dict(task)  # shallow copy — never mutate caller's dict

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


# ---------------------------------------------------------------------------
# CAPTCHA DETECTION + SOLVING
# ---------------------------------------------------------------------------

async def detect_and_solve_captcha(page) -> None:
    """Detect and solve Turnstile / reCAPTCHA v2 / hCaptcha."""
    try:
        try:
            html = await page.content()
        except Exception:
            try:
                html = await page.evaluate("() => document.documentElement.outerHTML")
            except Exception:
                return

        page_url = await _page_url(page)
        frames   = await _page_frames(page)
        proxy    = _ACTIVE_PROXY

        # ── Cloudflare Turnstile ──────────────────────────────────────────
        ts_key = None
        for frame in frames:
            fu = await _frame_url(frame)
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
                {"type": "AntiTurnstileTask", "websiteURL": page_url, "websiteKey": ts_key},
                proxy=proxy,
            )
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
            print("[CAPTCHA] Cloudflare challenge persists after 15s")
            return

        # ── reCAPTCHA v2 ─────────────────────────────────────────────────
        rc_key = None
        for frame in frames:
            fu = await _frame_url(frame)
            if "recaptcha" in fu and "anchor" in fu:
                m = _re.search(r'[?&]k=([^&]+)', fu)
                if m:
                    rc_key = m.group(1)
                try:
                    cb = frame.locator(".recaptcha-checkbox-border").first
                    if await cb.count() > 0:
                        await cb.click(timeout=3000)
                        await asyncio.sleep(3)
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
            sol = await _capsolver_solve(
                {"type": "ReCaptchaV2Task", "websiteURL": page_url, "websiteKey": rc_key},
                proxy=proxy,
            )
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
                sol = await _capsolver_solve(
                    {"type": "HCaptchaTask", "websiteURL": page_url, "websiteKey": m.group(1)},
                    proxy=proxy,
                )
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
# HUMAN-LIKE DELAY
# ---------------------------------------------------------------------------

async def human_delay(min_ms: int = 500, max_ms: int = 2000) -> None:
    await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


# ---------------------------------------------------------------------------
# STEALTH APPLICATION — uses unwrapped real page
# ---------------------------------------------------------------------------

async def apply_stealth_to_page(page) -> None:
    """
    Apply stealth to the page.
    MUST unwrap the real Playwright page first — browser-use wrappers don't
    have add_init_script, which causes playwright-stealth to crash every step.
    """
    real_page = await _unwrap_page(page)

    if STEALTH_LIB_AVAILABLE:
        try:
            await _stealth_async(real_page)
            print("[Stealth] playwright-stealth applied ✅")
        except Exception as exc:
            print(f"[Stealth] playwright-stealth failed: {exc}")

    try:
        await real_page.add_init_script(STEALTH_SCRIPT)
    except Exception as exc:
        print(f"[Stealth] add_init_script failed: {exc}")


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


# ---------------------------------------------------------------------------
# PLACEHOLDER FRAME
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
        print(f"[Frames] Pillow failed ({exc}), writing raw PNG.")
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
# SCREENSHOT EXTRACTORS
# ---------------------------------------------------------------------------

def _dump_history_screenshots(history, folder: str) -> int:
    saved = 0

    def _save(raw, label: str) -> bool:
        nonlocal saved
        if not raw:
            return False
        if isinstance(raw, str) and "," in raw:
            raw = raw.split(",", 1)[1]
        try:
            img_bytes = base64.b64decode(raw) if isinstance(raw, str) else raw
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
        print(f"[History] Extraction failed: {exc}")
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
# PROMPT WRAPPER
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
# STEP CALLBACK
# ---------------------------------------------------------------------------

def make_screenshot_callback(folder: str, counter: list[int]):
    proxy_checked = [False]  # verify proxy only once, on step 1

    async def _callback(agent) -> None:
        counter[0] += 1
        n = counter[0]
        try:
            browser_session = getattr(agent, "browser_session", None)
            if browser_session is None:
                print(f"[Callback] step {n}: no browser_session")
                return
            page = await browser_session.get_current_page()
            if page is None:
                print(f"[Callback] step {n}: get_current_page() returned None")
                return

            # ── Step 1: verify proxy IP (browser is now definitely running) ──
            if not proxy_checked[0]:
                proxy_checked[0] = True
                await _verify_proxy(page, _ACTIVE_PROXY)

            # ── Stealth — unwraps real Playwright page internally ──────────
            await apply_stealth_to_page(page)

            # ── CAPTCHA check ─────────────────────────────────────────────
            await detect_and_solve_captcha(page)

            # ── Human-like pacing ─────────────────────────────────────────
            await human_delay(300, 1200)

            # ── Screenshot — page.screenshot() returns raw bytes ──────────
            img_bytes = await page.screenshot()
            if isinstance(img_bytes, str):
                img_bytes = base64.b64decode(img_bytes)  # defensive

            path = os.path.join(folder, f"step_{n:04d}_cb.png")
            with open(path, "wb") as fh:
                fh.write(img_bytes)
            print(f"[Callback] step {n:03d} → {path}")

        except Exception as exc:
            print(f"[Callback] step {n} error: {exc}")

    return _callback


# ---------------------------------------------------------------------------
# LLM BUILDER
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
# RESULT CLEANER
# ---------------------------------------------------------------------------

def _clean_result(text: str) -> Any:
    if not text:
        return text
    text = text.strip()
    m = _re.search(r'<r>\s*(.*?)\s*</r>', text, _re.DOTALL)
    if m:
        text = m.group(1).strip()
    m = _re.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', text, _re.DOTALL)
    if m:
        text = m.group(1).strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    return text


# ---------------------------------------------------------------------------
# MAIN ENDPOINT
# ---------------------------------------------------------------------------

@app.post("/agent/run", response_model=AgentResponse)
async def run_agent(request: AgentRequest) -> AgentResponse:
    global _ACTIVE_PROXY

    session_id  = str(uuid.uuid4())[:8]
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"{SCAN_DIR}/{timestamp}_{session_id}"
    os.makedirs(folder_name, exist_ok=True)

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
            proxy_dict = _proxy_browser_dict(_ACTIVE_PROXY)
            print(f"[Proxy] Browser proxy server : {proxy_dict['server']}")
            print(f"[Proxy] Browser proxy user   : {proxy_dict['username']}")

            browser_cfg = BrowserConfig(
                headless=True,
                proxy=proxy_dict,
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
                    # --single-process / --disable-extensions / --no-zygote
                    # intentionally omitted — they break Chromium proxy auth
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
            json_match = _re.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', result_text, _re.DOTALL)
            if json_match:
                try:
                    parsed      = json.loads(json_match.group(1))
                    result_text = json.dumps(parsed, ensure_ascii=False, indent=2)
                    print("[Agent] Extracted clean JSON ✅")
                except Exception:
                    pass

        print(f"[Agent] ✅ Completed in {step_counter[0]} steps")

    except Exception as exc:
        import traceback
        traceback.print_exc()
        result_text = f"Agent error: {exc}"
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
        print(f"[Cleanup] Could not delete: {exc}")

    return AgentResponse(
        video_url=video_url,
        steps_taken=steps_taken,
        extracted_data=_clean_result(result_text) or None,
    )


# ---------------------------------------------------------------------------
# HEALTH CHECK
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": "1.0.0"}
