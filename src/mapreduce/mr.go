package mapreduce

import (
	"os"
	"fmt"
	"log"
	"strconv"
)

var _ = os.Args


type MapReduce struct {
	file string // name of file
  nMap int // the number of Map jobs
}
// init 
func InitMapReduce(file string, nMap int) *MapReduce {
	mr := new(MapReduce)
	return mr
}

// Runsingle run single job
func Runsingle(file string, nMap int) {
	mr := InitMapReduce(file, nMap)
	mr.Split(mr.file)
}

// Split split bytes of input into nMap splits
// but split only on white space
func (mr *MapReduce) Split(inputFile string) {
	fmt.Printf("split on file: %s\n", inputFile)
	
	inputF, err := os.Open(inputFile)
	defer inputF.Close()
	if err != nil  {
		log.Fatal("open file err: ", err)
	}

	fi, err := inputF.Stat()

	if err != nil {
		log.Fatal("Stat err: ", err)

	}

	size:= fi.Size()
	nchunk := size/int64(mr.nMap)
	nchunk ++

	outfile, err:=os.Create(MapName(mr.file, 0))
	
	// n for byte, m for nth file
	m, n := 1, 0



	for int64(i) > nchunk * int64(m) {

	}

	// outfile.




}	

// MapName name of ifle that is input for map job
func MapName(file string, mJop int) string {
	return "mtmp-" + file + "-" + strconv.Itoa(mJop) // mJop
}