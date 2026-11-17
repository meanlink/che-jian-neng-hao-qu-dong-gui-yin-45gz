FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY serve.py ./serve.py
ENV EVIDENCE_DATA_DIR=/evidence-data EVIDENCE_DRILLS=true
RUN mkdir -p /evidence-data
EXPOSE 8010
CMD ["uvicorn","serve:app","--host","0.0.0.0","--port","8010"]
