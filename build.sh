#!/bin/bash

git clone --recurse-submodules https://github.com/cwida/duckpgq-extension.git

podman build -it pgq .
