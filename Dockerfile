FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install OS packages required to build and run lxml and other deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \
       libxml2-dev \
       libxslt1-dev \
       libffi-dev \
       build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifest and install Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Create a non-root user and switch to it
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# Default entrypoint runs the runner as a module; override args to run specific source
ENTRYPOINT ["python", "-m", "engine.orchestrator"]
#ENTRYPOINT ["bash"]
CMD ["--help"]
