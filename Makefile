all: build run

DOCKER_OPTS = --rm -i

IS_TTY:=$(shell [ -t 0 ] && echo 1)
ifeq ($(IS_TTY),1)
  DOCKER_OPTS += -t
endif

CMD = docker run ${DOCKER_OPTS} --name memstorage -p 8081:8081 -v $(shell pwd):/memstore --network memstorage registry.gitlab.com/picodata/dockers/memstorage-builder

dev_deps:
	docker-compose -f dev/docker-compose-dev.yml up -d

build:
	$(CMD) /bin/bash -c "cartridge build; cp /kafka/kafka/tntkafka.so /memstore/.rocks/lib/tarantool/kafka/tntkafka.so;"

run:
	$(CMD) /bin/bash -c "cartridge start --debug"

stop:
	docker exec memstorage cartridge stop
	
release:
	$(CMD) cartridge pack rpm . --version=$(shell find . -path '*.rockspec' -maxdepth 1 | sed -En 's/\.\/[a-z]*-(.*)\.[a-z]*/\1/p')

clean:
	rm -rf .rocks && rm -rf tmp && rm -rf kafka

shell:
	$(CMD) /bin/bash

test_memtx:
	$(CMD) /bin/bash -c "test/memtx.sh"

test_vinyl:
	$(CMD) /bin/bash -c "test/vinyl.sh"

test_all:
	$(CMD) /bin/bash -c "test/all.sh"
