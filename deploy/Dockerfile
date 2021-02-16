FROM centos:8
LABEL maintainer="sai@arenadata.io"

WORKDIR /

EXPOSE 3301 3302 3303 3304 3305 8081 8082 8083 8084 8085

RUN groupadd tarantool \
    && adduser -g tarantool tarantool

RUN dnf -y install epel-release && \
    dnf -y update && \
    dnf -y clean all

RUN dnf -y install dnf-plugins-core \
&& dnf -y config-manager --set-enabled PowerTools

RUN set -x \
    && dnf -y install \
        libstdc++ \
        readline \
        openssl \
        lz4 \
        binutils \
        ncurses \
        libgomp \
        lua \
        tar \
        zip \
        zlib \
        unzip \
        libunwind \
        ca-certificates \
    && dnf -y install \
        perl \
        file \
        gcc-c++ \
        cmake \
        readline-devel \
        openssl-devel \
        zlib-devel \
        lz4-devel \
        binutils-devel \
        ncurses-devel \
        make \
        git \
        libunwind-devel \
        autoconf \
        automake \
        libtool \
        go \
        wget \
        snappy-devel \
        xz-devel \
        jansson-devel


RUN echo "---------- avro 1.10 ----------" \
&& mkdir avro \
&& wget https://github.com/apache/avro/archive/release-1.10.0.tar.gz \
&& tar -xzf release-1.10.0.tar.gz -C /avro --strip-components=1 \
&& rm release-1.10.0.tar.gz \
&& cd /avro/lang/c && mkdir build && cd build\
&& cmake .. \         
-DCMAKE_INSTALL_PREFIX=$PREFIX \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
&& make && make test && make install && cd / 
RUN echo "---------- rdkafka 1.5.0 ----------" \
&& mkdir rdkafka \
&& wget https://github.com/edenhill/librdkafka/archive/v1.5.0.tar.gz \
&& tar -xzf v1.5.0.tar.gz -C /rdkafka --strip-components=1 \
&& rm v1.5.0.tar.gz \
&& cd /rdkafka \
&& ./configure \
&& make && make install && cd /
COPY ./tarantool_2_3.repo /etc/yum.repos.d/tarantool_2_3.repo
RUN echo "---------- tarantool 2.3 ----------" \
&& dnf -y update && dnf -y clean all \
&& dnf -y install tarantool cartridge-cli tarantool-devel
RUN echo "---------- tnt_kafka ----------" \
&& git clone https://github.com/Asmoday/kafka.git \
&& cd kafka && cmake . && make && cd /
RUN echo "---------- ansible ----------" \
&& dnf -y install python3 python3-pip \
&& pip3 install ansible \
&& ansible-galaxy install tarantool.cartridge,1.4.0
COPY ./adg_nsud /adg_dtm
RUN cd /adg_dtm && cartridge build && cp /kafka/kafka/tntkafka.so /adg_dtm/.rocks/lib/tarantool/kafka/tntkafka.so
RUN chown -R tarantool:tarantool /adg_dtm 
WORKDIR /adg_dtm
VOLUME [ "/adg_dtm/tmp" ]
CMD cartridge start