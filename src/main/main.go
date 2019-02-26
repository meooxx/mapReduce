package main

import (
	"../mapreduce"
	"log"
	"os"
	// "fmt"
)

var _ = log.Fatal

func main() {
	filename:= os.Args[1]
	mapreduce.RunSingle(filename, 5, 3)
	// mr.Split(mr.file)
}