FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

RUN mkdir /app 
RUN mkdir /app/taxiData

WORKDIR /app
COPY data-loader.py data-loader.py

ENTRYPOINT ["python", "data-loader.py", "--user=root", "--passcode=root", "--host=pgdatabase", "--port=5432"]
