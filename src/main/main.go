package main

import (
	"../mapreduce"
	"log"
	"os"
)

func main() {
	filename:= os.Args[1]

	mapreduce.RunSingle(filename, 5)
	// mr.Split(mr.file)
}