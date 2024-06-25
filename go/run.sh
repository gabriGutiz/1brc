#!/bin/bash

FILE="main"

if [ -f "$FILE" ]; then
    rm "$FILE"
    echo "File $FILE deleted."
else
    echo "File $FILE does not exist."
fi

go build main.go

time ./main -input $1

