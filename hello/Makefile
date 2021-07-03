traefik:
	docker service create --name hellotraefik \
		--publish 80:80 --publish 8080:8080 \
		--mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
		--network hello \
		traefik:v2.4 --api.insecure=true --providers.docker

network:
	docker network create -d overlay --attachable hello

hello:
	docker stack deploy -c docker-compose-hello.yaml hellostack

