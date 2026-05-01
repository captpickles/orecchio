FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Runtime deps:
# - ffmpeg: audio ingest/transcoding
# - libsndfile1: required by librosa/soundfile stack used by birdnetlib
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Expect orecchio.toml and output paths to be bind-mounted for persistence.
CMD ["python", "orecchio.py"]
