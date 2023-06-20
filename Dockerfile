FROM ubuntu:22.04

WORKDIR /usr/src/app

RUN apt-get update
RUN apt-get install -y python3-pip default-jdk scala

COPY . .
RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "./connect_to_spark.py"]