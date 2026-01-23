# containers/hi4pi_download/Dockerfile

# Small Linux Python base image
FROM python:3.11-slim

# Avoid interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy only what is needed for installation initially
COPY pyproject.toml README.md /app/
COPY src /app/src

# Upgrade and install packages
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir .

# Runtime defaults
ENV PYTHONBUFFERED=1

# Default command to show help (can be overriden at runtime)
CMD ["python", "-m", "wallaby_mw.stages.hi4pi_download", "--help"]
