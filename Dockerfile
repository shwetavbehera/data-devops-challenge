# Use official Spark + Python base image
FROM bitnami/spark:latest

# Set working directory
WORKDIR /app

# Copy code into container
COPY src/ ./src/
COPY requirements.txt ./
COPY tests/ ./tests/

# Install Python packages (use system pip)
RUN pip install --upgrade pip && pip install -r requirements.txt