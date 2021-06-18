FROM python:3.7-slim-buster

ARG APP
RUN echo $APP

COPY envs.sh /app/envs.sh

COPY ./apps/$APP /app
WORKDIR /app

RUN pip install -r /app/requirements.txt

EXPOSE 8080

SHELL ["/bin/bash", “-c”]
CMD ["source", "envs.sh"]

CMD ["python", "main.py"]
