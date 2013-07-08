#!/bin/bash

mkdir -p ../../out/gen/py
thrift -gen py -out ../../out/gen/py service.thrift
thrift -gen java:private-members -out . service.thrift