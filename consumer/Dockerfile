FROM python:3.10.12

WORKDIR /app

COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py /app

CMD ["python", "consumer.py"]
