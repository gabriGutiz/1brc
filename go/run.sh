#!/bin/bash

FILE="main"

if [ -f "$FILE" ]; then
    rm "$FILE"
fi

go build main.go

time ./main -input $1 -cpuprofile cpu.prof

