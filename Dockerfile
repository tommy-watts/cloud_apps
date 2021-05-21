FROM python:3.7-slim-buster

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

ARG APP
RUN echo $APP

COPY ./apps/$APP /app
WORKDIR /app

EXPOSE 8080

CMD ["python", "main.py"]
