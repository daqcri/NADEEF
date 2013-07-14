#!/bin/bash

if ! [ -n "$NADEEF_HOME" ]; then 
    echo "NADEEF_HOME is not set."
    exit 1
fi
    
mkdir -p $NADEEF_HOME/out/gen/py
thrift -gen py -out $NADEEF_HOME/out/gen/py $NADEEF_HOME/service/src/service.thrift
thrift -gen java:private-members -out $NADEEF_HOME/service/src $NADEEF_HOME/service/src/service.thrift
