FROM python:3.7-slim-buster

ARG APP
RUN echo $APP

COPY ./apps/$APP /app
WORKDIR /app

RUN pip install -r /app/requirements.txt

EXPOSE 8080

CMD ["python", "main.py"]
