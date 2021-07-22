[![Tests](https://github.com/arenadata/kafka-tarantool-loader/actions/workflows/tests.yml/badge.svg)](https://github.com/arenadata/kafka-tarantool-loader/actions/workflows/tests.yml) [![Build](https://github.com/arenadata/kafka-tarantool-loader/actions/workflows/build.yml/badge.svg)](https://github.com/arenadata/kafka-tarantool-loader/actions/workflows/build.yml)

# Arenadata Grid
## How to build app

Prerequisites:
* CentOS 7
* ``unzip``, ``git``, ``cmake``, ``librdkafka-devel``, ``openssl-devel``
* ``tarantool``, ``tarantool-devel``, ``cartridge-cli`` ([instructions](https://www.tarantool.io/en/download/os-installation/rhel-centos/))
* ``nodejs`` >=10 ([instructions](https://github.com/nodesource/distributions#installation-instructions-1))
* `` xz``,``xz-devel``
* ``avro c``([instructions](https://github.com/apache/avro/blob/master/lang/c/INSTALL))
* ``librdkafka``  >= v1.4.4 ([instructions](https://github.com/edenhill/librdkafka#build-from-source))

Build rpm:
```sh
cartridge pack rpm adg-kafka --version=$VERSION
```
Build deb:
```sh
cartridge pack deb adg-kafka --version=$VERSION
```
## How to build docker image
```sh
cd deploy
docker build . -t registry.gitlab.com/picodata/dockers/memstorage-builder
```
## How to run tests
1. Run zookeeper, kafka via docker-compose
```sh
make dev_deps
```
2. Prepare to read log via `tail -f`
```sh
echo '' | sudo tee tmp/tarantool.log && sudo tail -f tmp/tarantool.log
```
3. Run tests

On memtx engine
```sh
make test_memtx
```
On vinyl engine
```sh
make test_vinyl
```
On both engines
```sh
make test_all
```
