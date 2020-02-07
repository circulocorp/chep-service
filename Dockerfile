FROM python:2.7.15-alpine
COPY . /app
WORKDIR /app
ENV RABBITMQ_URL 192.168.1.70
ENV CIRCULOCORP http://localhost:8080
RUN pip install -r requirements.txt
CMD python ./main.py
