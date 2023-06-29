docker compose up -d

docker build . -t datosabiertosmineduc

REM docker run -it --rm -w /usr/src/app -v "%CD%":/usr/src/app datosabiertosmineduc sh ./download.sh

docker run -it --rm -w /usr/src/app -v "%CD%":/usr/src/app datosabiertosmineduc
