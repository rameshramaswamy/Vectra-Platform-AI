# -----------------------------------------------------------------------------
# Stage 1: Builder (Compiling dependencies)
# -----------------------------------------------------------------------------
FROM python:3.11-slim as builder

WORKDIR /app

# Prevent Python from writing pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required for building Python packages
# libpq-dev is needed for psycopg2 (PostgreSQL adapter)
# gcc is needed if any libraries require compilation
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and build wheels
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2: Runtime (Production Image)
# -----------------------------------------------------------------------------
FROM python:3.11-slim as runtime

WORKDIR /app

# Install runtime system libraries
# libpq5 is required for psycopg2 at runtime
# curl is required for the HEALTHCHECK command
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq5 \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN groupadd -g 1000 vectra && \
    useradd -u 1000 -g vectra -s /bin/bash vectra

# Copy artifacts from builder
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Install dependencies from wheels
RUN pip install --no-cache /wheels/*

# Copy application source code
COPY . .

# Change ownership to non-root user
CHOWN vectra:vectra /app

# Switch context to non-root user
USER vectra

# Expose the port FastAPI runs on
EXPOSE 8000

# Healthcheck to ensure the container is ready to accept traffic
# Hits the /health endpoint defined in app/main.py
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Start the application
# --host 0.0.0.0 makes it accessible outside the container
# --workers 1 is sufficient for async FastAPI in K8s (scale via replicas)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]