#!/bin/bash
export BuildVersion='1.0.1019'

type -P javac > /dev/null 2>&1 || { echo "JDK cannot be found, please check your PATH var."; exit 1; }
 
if ! [ -d "out" ]; then
    echo NADEEF is not yet compiled, please first run 'ant' to build it.
else
    if [ $# -eq 1 ]; then
        if [$1 = "console"]; then
            cmd='java -Dderby.drda.startNetworkServer=true -d64 -cp out/bin/*:examples/:out/test qa.qcri.nadeef.console.Console'
            exec $cmd
        else
            echo 'Unknown parameter'
        fi
    else
        cmd='java -d64 -cp out/bin/*:. qa.qcri.nadeef.web.NadeefStart'
        exec $cmd
    fi
fi
