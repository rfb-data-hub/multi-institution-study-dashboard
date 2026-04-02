FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MARIMO_OUTPUT_MAX_BYTES=50000000
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dashboard.py .
COPY Flow_battery_comms_logo.png .
COPY institution_map.png .
EXPOSE 8080
CMD ["python", "-m", "marimo", "run", "dashboard.py", "--host", "0.0.0.0", "--port", "8080", "--no-token"]