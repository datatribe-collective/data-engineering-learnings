FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev gcc build-essential --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8502

CMD ["streamlit", "run", "app.py", "--server.port=8502", "--server.address=0.0.0.0", "--server.fileWatcherType=poll"]