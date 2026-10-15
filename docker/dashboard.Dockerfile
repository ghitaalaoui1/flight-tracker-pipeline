FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

CMD ["streamlit", "run", "src/dashboard/app.py", \
     "--server.address=0.0.0.0", "--server.port=8501"]