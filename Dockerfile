FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN mkdir -p downloads
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt
COPY . /app

CMD ["sh", "-c", "python locomotive_tracker_test.py"]
#CMD ["sh", "-c", "python bot.py & python monitor.git initpy & python locomotive_tracker.py & uvicorn app:app --host 0.0.0.0 --port 8081"]