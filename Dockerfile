# image: ngeo/translate

FROM python:3.7

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

CMD python driver.py