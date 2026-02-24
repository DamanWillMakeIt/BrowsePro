"""
main.py
-------
FastAPI wrapper around browser-use 0.11.x.

Run:
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import base64
import glob
import json
import os
import shutil
import uuid
from datetime import datetime

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


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class AgentRequest(BaseModel):
    prompt: str
    max_steps: int = 50
    model: str = "gpt-5.1"


class AgentResponse(BaseModel):
    status: str
    result: str
    video_url: str | None = None
    steps_taken: int = 0


# ---------------------------------------------------------------------------
# Proper placeholder PNG (1920×1080 grey frame via Pillow)
# ---------------------------------------------------------------------------

def _ensure_minimum_frames(folder: str) -> None:
    """Write a proper 1920×1080 placeholder if no PNGs exist yet."""
    if glob.glob(os.path.join(folder, "*.png")):
        return

    print("[Frames] No screenshots — writing 1920×1080 placeholder.")
    path = os.path.join(folder, "step_0000_placeholder.png")

    try:
        from PIL import Image, ImageDraw, ImageFont
        img  = Image.new("RGB", (1920, 1080), color=(30, 30, 30))
        draw = ImageDraw.Draw(img)
        msg  = "No screenshot captured"
        # ImageDraw.textlength available in Pillow ≥ 8; bbox fallback for older
        try:
            tw = draw.textlength(msg)
        except AttributeError:
            tw = len(msg) * 8  # rough fallback
        draw.text(((1920 - tw) / 2, 520), msg, fill=(180, 180, 180))
        img.save(path, "PNG")
        print(f"[Frames] Placeholder written → {path}")
    except Exception as exc:
        print(f"[Frames] Pillow placeholder failed ({exc}), writing raw PNG.")
        # Minimal valid 1×1 white PNG (should never reach here given Pillow is installed)
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
# Extract screenshots from AgentHistoryList object
# ---------------------------------------------------------------------------

def _dump_history_screenshots(history, folder: str) -> int:
    """
    Walk every ActionResult / AgentHistory object and save embedded
    base64 screenshots to disk.  Returns number of frames saved.
    """
    saved = 0

    def _save(raw: str, label: str) -> bool:
        nonlocal saved
        if not raw:
            return False
        if isinstance(raw, str) and "," in raw:
            raw = raw.split(",", 1)[1]
        try:
            img_bytes = base64.b64decode(raw)
            # Quick sanity check — must start with PNG or JPEG magic bytes
            if not (img_bytes[:4] == b'\x89PNG' or img_bytes[:2] == b'\xff\xd8'):
                return False
            path = os.path.join(folder, f"{label}.png")
            with open(path, "wb") as fh:
                fh.write(img_bytes)
            saved += 1
            print(f"[History] Saved screenshot → {path}")
            return True
        except Exception as exc:
            print(f"[History] Could not decode {label}: {exc}")
            return False

    try:
        # all_results: list of ActionResult
        results = getattr(history, "all_results", []) or []
        for i, result in enumerate(results):
            for attr in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                if _save(getattr(result, attr, None), f"step_{i+1:04d}_result"):
                    break

        # history.history: list of AgentHistory (each step)
        histories = getattr(history, "history", []) or []
        for i, h in enumerate(histories):
            # browser-use 0.11 stores the screenshot on the state object
            state = getattr(h, "state", None)
            if state is not None:
                for attr in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                    if _save(getattr(state, attr, None), f"step_{i+1:04d}_state"):
                        break
            # also try directly on the history object
            for attr in ("screenshot", "base64_screenshot", "image", "screenshot_b64"):
                if _save(getattr(h, attr, None), f"step_{i+1:04d}_h"):
                    break

    except Exception as exc:
        print(f"[History] Object extraction failed: {exc}")

    return saved


# ---------------------------------------------------------------------------
# Extract screenshots from conversation_*.json files
# (browser-use 0.11.x saves every step as a JSON with base64 screenshots)
# ---------------------------------------------------------------------------

def _dump_json_screenshots(folder: str) -> int:
    """
    Parse every conversation_*.json in *folder* and extract embedded
    base64 screenshots.  Returns number of new frames saved.
    """
    saved   = 0
    pattern = os.path.join(folder, "conversation_*.json")
    files   = sorted(glob.glob(pattern))

    if not files:
        print("[JSON] No conversation_*.json files found.")
        return 0

    print(f"[JSON] Found {len(files)} conversation JSON file(s) — extracting screenshots…")

    for json_path in files:
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            print(f"[JSON] Could not parse {json_path}: {exc}")
            continue

        # The JSON is a list of message dicts; each message may have
        # a "content" list containing image blocks.
        messages = data if isinstance(data, list) else data.get("messages", [])

        for msg in messages:
            content = msg.get("content", [])
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                # OpenAI vision format: {"type": "image_url", "image_url": {"url": "data:..."}}
                img_url = (block.get("image_url") or {}).get("url", "")
                # Anthropic format: {"type": "image", "source": {"data": "...", "type": "base64"}}
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
                    # Name by JSON file stem + index to keep ordering
                    stem  = os.path.splitext(os.path.basename(json_path))[0]
                    fname = f"{stem}_img{saved+1:03d}.png"
                    out   = os.path.join(folder, fname)
                    with open(out, "wb") as fh:
                        fh.write(img_bytes)
                    saved += 1
                    print(f"[JSON] Extracted screenshot → {out}")
                except Exception as exc:
                    print(f"[JSON] Decode error in {json_path}: {exc}")

    print(f"[JSON] Total screenshots extracted from JSON: {saved}")
    return saved


# ---------------------------------------------------------------------------
# Prompt wrapper — adds strict tool-adding instructions to any task
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
# on_step_end callback
# ---------------------------------------------------------------------------

def make_screenshot_callback(folder: str, counter: list[int]):
    async def _callback(agent) -> None:
        """
        browser-use 0.11.x calls: await on_step_end(self)
        where self is the Agent instance.
        agent.browser_session.get_current_page() returns the live Playwright page.
        """
        counter[0] += 1
        n = counter[0]

        try:
            browser_session = getattr(agent, "browser_session", None)
            if browser_session is None:
                print(f"[Callback] step {n}: no browser_session on agent")
                return

            page = await browser_session.get_current_page()
            if page is None:
                print(f"[Callback] step {n}: get_current_page() returned None")
                return

            img_b64 = await page.screenshot()  # returns base64 string
            img_bytes = base64.b64decode(img_b64)
            path = os.path.join(folder, f"step_{n:04d}_cb.png")
            with open(path, "wb") as fh:
                fh.write(img_bytes)
            print(f"[Callback] step {n:03d} → {path}")

        except Exception as exc:
            print(f"[Callback] step {n} screenshot failed: {exc}")

    return _callback


# ---------------------------------------------------------------------------
# Build LLM
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
# Endpoint
# ---------------------------------------------------------------------------

@app.post("/agent/run", response_model=AgentResponse)
async def run_agent(request: AgentRequest) -> AgentResponse:
    session_id  = str(uuid.uuid4())[:8]
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"{SCAN_DIR}/{timestamp}_{session_id}"
    os.makedirs(folder_name, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"[Agent] Session   : {session_id}")
    print(f"[Agent] Task      : {request.prompt}")
    print(f"[Agent] Model     : {request.model}")
    print(f"[Agent] Max steps : {request.max_steps}")
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
            browser_cfg = BrowserConfig(
                headless=True,
                extra_chromium_args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1920,1080",
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
        result_text = str(history)
        print(f"[Agent] ✅ Completed in {step_counter[0]} callback steps")

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

    # ── 1. Try extracting from the history object ─────────────────────────
    if history is not None:
        saved = _dump_history_screenshots(history, folder_name)
        print(f"[Agent] Extracted {saved} screenshot(s) from history object")

    # ── 2. Extract from conversation_*.json files (primary source in 0.11.x)
    json_saved = _dump_json_screenshots(folder_name)
    print(f"[Agent] Extracted {json_saved} screenshot(s) from JSON files")

    steps_taken = step_counter[0] or (
        len(getattr(history, "all_results", []) or []) if history else 0
    )

    # ── 3. Guarantee ≥1 frame ─────────────────────────────────────────────
    _ensure_minimum_frames(folder_name)

    frame_count = len(glob.glob(os.path.join(folder_name, "*.png")))
    print(f"[Agent] Building video from {frame_count} screenshot(s)…")
    video_url = await create_and_upload_video(folder_name, session_id)
    print(f"[Agent] Video URL : {video_url}")

    # ── 4. Clean up local scan folder to save disk space ──────────────────
    # Everything is already uploaded to Cloudinary — no need to keep it.
    try:
        shutil.rmtree(folder_name)
        print(f"[Cleanup] Deleted scan folder: {folder_name}")
    except Exception as exc:
        print(f"[Cleanup] Could not delete scan folder: {exc}")

    return AgentResponse(
        status=final_status,
        result=result_text,
        video_url=video_url,
        steps_taken=steps_taken,
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": "1.0.0"}