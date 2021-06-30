FROM centos:7
LABEL maintainer="sai@arenadata.io"

WORKDIR /

EXPOSE 3301 3302 3303 3304 3305 8081 8082 8083 8084 8085

RUN yum -y install epel-release && \
    yum -y update && \
    yum -y clean all

RUN set -x \
    && yum -y install \
        zip \
        unzip \
        cmake3 \
        git \
        xz-devel \
        jansson-devel \
        wget \
        make \
        gcc-c++ \
        openssl-devel \
        cmake \
        luarocks \
    && cd /


RUN echo "---------- avro 1.10.2 ----------" \
&& mkdir avro \
&& wget https://github.com/apache/avro/archive/release-1.10.2.tar.gz \
&& tar -xzf release-1.10.2.tar.gz -C /avro --strip-components=1 \
&& rm release-1.10.2.tar.gz \
&& cd /avro/lang/c && mkdir build && cd build\
&& cmake3 .. \         
-DCMAKE_INSTALL_PREFIX=$PREFIX \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
&& make && make test && make install && cd / 
RUN echo "---------- rdkafka 1.7.0 ----------" \
&& mkdir rdkafka \
&& wget https://github.com/edenhill/librdkafka/archive/v1.7.0.tar.gz \
&& tar -xzf v1.7.0.tar.gz -C /rdkafka --strip-components=1 \
&& rm v1.7.0.tar.gz \
&& cd /rdkafka \
&& ./configure \
&& make && make install && cd /
RUN curl -L https://tarantool.io/GlcTRY/release/2.7/installer.sh | bash
RUN echo "---------- tarantool 2.7 ----------" \
&& yum -y update && yum -y clean all \
&& yum -y install tarantool cartridge-cli tarantool-devel
RUN echo "---------- tnt_kafka ----------" \
&& git clone https://github.com/Asmoday/kafka.git \
&& cd kafka && cmake3 . && make && cd /
RUN echo "---------- luacov ----------" \
&& luarocks install luacov \
&& tarantoolctl rocks install luatest

ENV TARANTOOL_LOG="tmp/tarantool.log"
VOLUME [ "/memstore" ]
WORKDIR /memstore
