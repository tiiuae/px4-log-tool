# Stage 1: Build with necessary tools
FROM python:3.12-slim AS build

# Install system dependencies and Python build tools
RUN apt-get update && apt-get install -y \
        build-essential \
        python3-dev \
        python3-yaml \
        libyaml-dev \
        && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install build dependencies
RUN pip3 install --upgrade pip setuptools wheel

WORKDIR /build

# Copy requirements and install dependencies
COPY requirements.txt .
ENV PIP_NO_BUILD_ISOLATION=1
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy your application code
COPY . .

RUN pip3 install .

# Stage 2: Final image without build dependencies
FROM python:3.12-slim

# Copy installed Python packages from the build stage
COPY --from=build /usr/local /usr/local

WORKDIR /app

CMD ["px4-log-tool"]
