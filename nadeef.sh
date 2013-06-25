#!/bin/bash
export BuildVersion='1.0.1019'

type -P javac > /dev/null 2>&1 || { echo "JDK cannot be found, please check your PATH var."; exit 1; }
 
if ! [ -d "out" ]; then
    echo NADEEF is not yet compiled, please first run 'ant' to build it.
else
    cmd='java -d64 -Xmx3g -XX:MaxHeapFreeRatio=70 -cp out/bin/*:examples/:out/test qa.qcri.nadeef.console.Console'
    exec $cmd
fi
