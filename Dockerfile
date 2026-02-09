# Use official Python slim image (smaller & faster)
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first (better caching when rebuilding)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your project code
COPY scripts/ ./scripts/
COPY plots/ ./plots/   # only if you want small PNGs inside — optional

# Default command (can be overridden)
CMD ["python", "scripts/train_daily_risk_classifier.py"]