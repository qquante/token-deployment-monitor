# =============================================================================
# BANKR MONITOR - DOCKER IMAGE
# =============================================================================
#
# Multi-stage build for smaller final image.
# Supports all workers via different entrypoints.
#
# Build:
#   docker build -t bankr-monitor .
#
# Run specific worker:
#   docker run bankr-monitor python -m workers.twitter_listener
#   docker run bankr-monitor python -m workers.onchain_listener
#   docker run bankr-monitor python -m workers.telegram_sender
#
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements-docker.txt requirements.txt
RUN pip install --no-cache-dir --user -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local

# Ensure scripts in .local are usable
ENV PATH=/root/.local/bin:$PATH

# Set Python to run unbuffered (important for Docker logs)
ENV PYTHONUNBUFFERED=1

# Copy source code
COPY config/ ./config/
COPY src/ ./src/
COPY shared/ ./shared/
COPY workers/ ./workers/
COPY scripts/ ./scripts/

# Create data directory
RUN mkdir -p /app/data

# Default command (can be overridden)
CMD ["python", "-m", "workers.twitter_listener"]
