# image: ngeo/translate

FROM python:3.7

COPY . /app
WORKDIR /app

RUN curl -O https://gbnci-abcc.ncifcrf.gov/geo/GEOmetadb.sqlite.gz
RUN gzip GEOmetadb.sqlite.gz

RUN pip install -r requirements.txt

CMD python driver.py