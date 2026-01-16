# containers/subfits/Dockerfile

# Small Linux Python base image
FROM python:3.11-slim

# Avoid interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install minimal system dependencies (same style as CASDA image)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy only what is needed for installation initially
COPY pyproject.toml README.md /app/
COPY src /app/src

# Upgrade and install packages (this installs wallaby_mw, including apply_subfits)
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir .

# Copy the vendored subfits script into a known location
RUN mkdir -p /opt/subfits
COPY containers/subfits/subfits.py /opt/subfits/subfits.py

# Runtime defaults
ENV PYTHONUNBUFFERED=1

# Default command (Skaha will override cmd/args via config.ini anyway)
CMD ["python", "-m", "wallaby_mw", "--help"]