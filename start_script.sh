#!/bin/bash

echo 'Build base container'
docker build .
docker build -t py_container_dev:0.1 .

echo 'Up Services'
docker-compose up -d 

