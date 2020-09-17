# image: ngeo/translate

FROM python:3.5.3

COPY . /app
WORKDIR /app

RUN curl -O https://gbnci-abcc.ncifcrf.gov/geo/GEOmetadb.sqlite.gz
RUN gunzip GEOmetadb.sqlite.gz

RUN pip install -r requirements.txt

CMD python driver.py