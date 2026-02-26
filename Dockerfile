FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# Install system dependencies (ffmpeg is mandatory for your video generation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Fetch Camoufox binaries and install Playwright Chromium + its OS dependencies
RUN python -m camoufox fetch && \
    playwright install chromium && \
    playwright install-deps chromium

# Copy application code
COPY . .
EXPOSE 8000

# FIX: Added a 650-second (10.8 minute) keep-alive timeout so Uvicorn doesn't kill your long agent runs
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "650"]
