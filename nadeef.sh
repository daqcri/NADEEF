#!/bin/bash

if [ -f java ]; then
    echo Java cannot be found, please first check your PATH.
else if ! [ -d "out" ]; then
    echo Nadeef is not yet compiled, please first run 'ant all' to build it.
else
    cmd='java -cp out/nadeef.jar:out/production:. qa.qcri.nadeef.console.Console' 
	exec $cmd
fi
fi
