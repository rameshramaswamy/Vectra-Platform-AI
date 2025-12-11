# -----------------------------------------------------------------------------
# Stage 1: Builder (Compiling dependencies)
# -----------------------------------------------------------------------------
FROM python:3.10-slim as builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required for building Python packages
# gcc/g++: Required for compiling some ML libraries (numpy, pyarrow extensions)
# git: Required if installing dependencies from git repositories
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and build wheels
# This separates dependency resolution from code changes for better caching
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2: Runtime (Production Image)
# -----------------------------------------------------------------------------
FROM python:3.10-slim as runtime

WORKDIR /app

# Install runtime system libraries
# libgomp1: OpenMP support (Critical for Scikit-Learn/PyTorch parallelism)
# libpq5: Postgres adapter (if connecting to DB directly)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libgomp1 \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -g 1000 vectra && \
    useradd -u 1000 -g vectra -s /bin/bash vectra

# Copy artifacts from builder
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Install dependencies from wheels
RUN pip install --no-cache /wheels/*

# Copy application source code
COPY . .

# Change ownership
CHOWN vectra:vectra /app
USER vectra

# Environment variables for MLflow/Ray
ENV MLFLOW_TRACKING_URI=http://mlflow:5000
ENV RAY_ADDRESS=ray://ray-head:10001

# Default command: Runs the continuous training loop
CMD ["python", "-m", "app.main"]