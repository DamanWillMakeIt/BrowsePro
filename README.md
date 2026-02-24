# OnDemand Agent — powered by browser-use (FIXED)

Replaces the entire custom SoM/judge/consensus/planner stack with browser-use,
which handles DOM parsing, element detection, LLM planning, and Playwright
execution out of the box.

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Install Playwright browsers
playwright install chromium

# 3. Fill in your .env
cp .env .env.local   # edit with your keys

# 4. Run
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Usage

POST to `/agent/run`:

```json
{
  "prompt": "Navigate to https://app.on-demand.io/playground and login with email damandeepsingh24090@gmail.com and password Daman@13. Once on the Playground: click the small green '+' icon button in the Agent Tools panel (top-right area). In the panel that opens, type 'XAI Video' in the search box and click the green '+' button on the 'XAI Video Generator And Editor' card. Then close the panel and click the first conversational starter chip near the chat input to run it.",
  "max_steps": 50,
  "model": "gpt-4o"
}
```

## Why browser-use instead of custom SoM?

browser-use has spent thousands of engineering hours solving exactly the problems
we kept hitting:
- Picking the wrong "+" button (wrapped in nav <a> tags)
- Search boxes not being detected by SoM tagging
- Tool card buttons not being tagged due to z-index issues

It uses its own DOM extraction + visual grounding pipeline that handles all of this.

## Supported models (pass in `model` field)

- `gpt-4o` (default, recommended)
- `gpt-4o-mini` (faster/cheaper)
- `claude-3-5-sonnet-20241022` (needs `langchain-anthropic` + `ANTHROPIC_API_KEY`)
- `gemini-2.0-flash` (needs `langchain-google-genai` + `GOOGLE_API_KEY`)
