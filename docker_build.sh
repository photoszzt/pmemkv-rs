#!/bin/bash
set -e

if [[ -z "$HOST_WORKDIR" ]]; then
	HOST_WORKDIR=$(readlink -f .)
fi

chmod -R a+w $HOST_WORKDIR
WORKDIR=/pmemkv-rs

docker run pmem/pmemkv:ubuntu-18.04 --privileged=true -ti \
    -v $HOST_WORKDIR:$WORKDIR \
    -v /etc/localtime:/etc/localtime \
    -w $WORKDIR \
    ./run-build.sh
