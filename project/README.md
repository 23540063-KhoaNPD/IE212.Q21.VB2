build

docker-compose up -d


Rebuild:

docker compose down -v

docker compose up -d --build


Log:

docker compose logs -f


Log từng service

docker compose logs -f kafka
docker compose logs -f producer
docker compose logs -f spark
