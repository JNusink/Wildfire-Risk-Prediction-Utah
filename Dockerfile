# Use official Python slim image (smaller & faster)
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy only the requirements first (better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY scripts/ ./scripts/
COPY plots/ ./plots/   # only small files — no big HTML or data

# Optional: expose a port if you later add Streamlit/Flask
# EXPOSE 8501

# Default command — can be overridden
CMD ["python", "scripts/train_daily_risk_classifier.py"]