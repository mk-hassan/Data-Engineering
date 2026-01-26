docker-compose down
docker rmi $(docker images -q)
docker volume rm $(docker volume ls -q)
sudo rm -rf taxiData/ ../postgres_database_infra/
docker-compose up --build -d
