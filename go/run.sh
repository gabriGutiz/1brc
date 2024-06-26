#!/bin/bash

go build main.go

time ./main -input $1

