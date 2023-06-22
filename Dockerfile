FROM ubuntu:22.04

WORKDIR /usr/src/app

RUN apt-get update
RUN apt-get install -y python3-pip default-jdk scala
RUN apt-get install -y unzip unrar curl dos2unix

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
RUN dos2unix download.sh

CMD ["python3", "./connect_to_spark.py"]