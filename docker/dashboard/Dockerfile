FROM python:3.11-slim

WORKDIR /app
COPY dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dashboard/ /app/
EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
