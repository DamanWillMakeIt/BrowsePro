# Python 3.11 slim base — browser-use requires 3.11+
FROM python:3.11-slim-bookworm

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# Install system deps: ffmpeg + Playwright OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    # Playwright/Chromium OS deps
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libwayland-client0 \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Install Python dependencies
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Install Playwright + Chromium
RUN playwright install chromium --with-deps

# Copy app
COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
