"""
main.py
-------
FastAPI wrapper around browser-use 0.11.x.

browser-use 0.11.x dropped LangChain — it now uses its own LLM clients.
We use browser_use.llm.ChatOpenAI (or openai directly via browser-use's
built-in wrapper) and the on_step_end callback to capture screenshots.

Run:
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import os
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
# Screenshot callback
# ---------------------------------------------------------------------------

def make_screenshot_callback(folder: str, counter: list[int]):
    async def _callback(*args, **kwargs) -> None:
        counter[0] += 1
        n = counter[0]
        page = None

        for obj in list(args) + list(kwargs.values()):
            for attr in ("browser_context", "browser", "_browser_context", "_browser"):
                ctx = getattr(obj, attr, None)
                if ctx is None:
                    continue
                getter = getattr(ctx, "get_current_page", None)
                if callable(getter):
                    try:
                        page = await getter()
                        break
                    except Exception:
                        pass
                direct = getattr(ctx, "page", None)
                if direct is not None:
                    page = direct
                    break
            if page is not None:
                break

        if page is None:
            print(f"[Screenshot] step {n}: could not get page, skipping.")
            return

        try:
            img_bytes = await page.screenshot(full_page=False)
            img_path  = os.path.join(folder, f"step_{n:04d}.png")
            with open(img_path, "wb") as fh:
                fh.write(img_bytes)
            print(f"[Screenshot] step {n:03d} → {img_path}")
        except Exception as exc:
            print(f"[Screenshot] step {n}: screenshot failed — {exc}")

    return _callback


# ---------------------------------------------------------------------------
# Build LLM — browser-use 0.11.x native client
# ---------------------------------------------------------------------------

def build_llm(model: str, api_key: str):
    # Path 1: browser_use.llm (most common in 0.11.x)
    try:
        from browser_use.llm import ChatOpenAI as BUChatOpenAI
        return BUChatOpenAI(model=model, api_key=api_key)
    except Exception:
        pass

    # Path 2: browser_use.agent.llm
    try:
        from browser_use.agent.llm import ChatOpenAI as BUChatOpenAI2
        return BUChatOpenAI2(model=model, api_key=api_key)
    except Exception:
        pass

    # Path 3: raw openai client (browser-use wraps it internally)
    try:
        from openai import AsyncOpenAI
        return AsyncOpenAI(api_key=api_key)
    except Exception:
        pass

    raise RuntimeError("Could not build LLM — check browser-use version and OPENAI_API_KEY")


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

    llm = build_llm(request.model, os.getenv("OPENAI_API_KEY", ""))

    step_counter = [0]
    on_step_end  = make_screenshot_callback(folder_name, step_counter)

    agent_kwargs: dict = dict(
    task=request.prompt,
    llm=llm,
    on_step_end=on_step_end,
    save_conversation_path=folder_name,
    max_actions_per_step=1,        # re-evaluate after every single action
    use_vision=True,               # use screenshot for every decision
    max_failures=3,                # stop retrying a failed action after 3 attempts
    retry_delay=2,                 # wait 2s before retrying
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
            browser = None
    else:
        print("[Agent] BrowserConfig not available — using browser-use defaults")

    agent = Agent(**agent_kwargs)

    result_text  = ""
    final_status = "success"

    try:
        result      = await agent.run(max_steps=request.max_steps)
        result_text = str(result)
        print(f"[Agent] ✅ Completed in {step_counter[0]} steps")
        print(f"[Agent] Result: {result_text[:300]}")

    except Exception as exc:
        import traceback
        traceback.print_exc()
        result_text  = f"Agent error: {exc}"
        final_status = "failed"
        print(f"[Agent] ❌ Failed after {step_counter[0]} steps: {exc}")

    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

    steps_taken = step_counter[0]

    print(f"[Agent] Building video from {steps_taken} screenshot(s)…")
    video_url = await create_and_upload_video(folder_name, session_id)
    print(f"[Agent] Video URL : {video_url}")

    return AgentResponse(
        status=final_status,
        result=result_text,
        video_url=video_url,
        steps_taken=steps_taken,
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": "1.0.0"}