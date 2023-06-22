docker compose up -d

docker build . -t datosabiertosmineduc

docker run -it --rm -w /usr/src/app -v "%CD%/datosabiertos.mineduc.cl":/usr/src/app/datosabiertos.mineduc.cl datosabiertosmineduc sh ./download.sh

docker run -it --rm -w /usr/src/app -v "%CD%/datosabiertos.mineduc.cl":/usr/src/app/datosabiertos.mineduc.cl datosabiertosmineduc
REM python ./connect_to_spark.py