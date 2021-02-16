# Arenadata Grid
## How to build

Prerequisites:
* CentOS 7
* ``unzip``, ``git``, ``cmake``, ``librdkafka-devel``, ``openssl-devel``
* ``tarantool``, ``tarantool-devel``, ``cartridge-cli`` ([instructions](https://www.tarantool.io/en/download/os-installation/2.2/rhel-centos-6-7/))
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
