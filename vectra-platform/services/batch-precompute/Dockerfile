# services/refinery-worker/Dockerfile

FROM python:3.11-slim as builder

WORKDIR /app
# Install build dependencies for pandas/scikit-learn (C extensions)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

FROM python:3.11-slim as runtime

WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 libgomp1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

RUN pip install --no-cache /wheels/*

COPY . .

# Run as non-root
RUN useradd -m vectra
USER vectra

CMD ["python", "-m", "app.main"]