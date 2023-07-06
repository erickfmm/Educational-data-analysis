REM docker compose up -d

docker run --name mypostgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -e PGDATA=/var/lib/postgresql/data/pgdata -d -v "%CD%/pgdata":/var/lib/postgresql/data postgres

docker build . -t datosabiertosmineduc

REM docker run -it --rm -w /usr/src/app -v "%CD%":/usr/src/app datosabiertosmineduc sh ./download.sh

docker run -it --rm -w /usr/src/app --link=mypostgres -v "%CD%":/usr/src/app datosabiertosmineduc
