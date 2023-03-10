FROM python:3.10.8-slim-buster

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .

# ENV FLASK_RUN_HOST=0.0.0.0

CMD ["python3", "./app.py"]
