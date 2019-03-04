package mapreduce

import (
	"bufio"
	"container/list"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

var _ = os.Args

type KeyValue struct {
	Key   string
	Value string
}
type MapReduce struct {
	file    string // name of file
	nMap    int    // the number of Map jobs
	nReduce int    // the number of reduce jobs
}

// init
func InitMapReduce(file string, nMap, nReduce int) *MapReduce {
	mr := new(MapReduce)
	mr.file = file
	mr.nMap = nMap
	mr.nReduce = nReduce
	return mr
}

// RunSingle run single job
func RunSingle(file string, nMap, nReduce int) {
	mr := InitMapReduce(file, nMap, nReduce)
	// mr.Split(mr.file)
	// for i := 0; i < nMap; i++ {
	// 	DoMap(file, i, nReduce)
	// }
	/* for i := 0; i < nReduce; i++ {
		DoReduce(file, i, nMap)
	} */
	mr.Merge()
}

// Split split bytes of input into nMap splits
// but split only on white space
func (mr *MapReduce) Split(inputFile string) {
	fmt.Printf("split on file  %s\n", inputFile)

	inputF, err := os.Open(inputFile)
	defer inputF.Close()
	if err != nil {
		log.Fatal("Split: ", err)
	}

	fi, err := inputF.Stat()

	if err != nil {
		log.Fatal("Stat err: ", err)

	}

	size := fi.Size()
	nchunk := size / int64(mr.nMap)
	nchunk++

	outfile, err := os.Create(MapName(mr.file, 0))
	writer := bufio.NewWriter(outfile)

	// n for byte, m for nth file
	m, n := 1, 0
	scanner := bufio.NewScanner(inputF)
	for scanner.Scan() {
		if int64(n) > nchunk*int64(m) {
			writer.Flush()
			outfile.Close()
			outfile, err = os.Create(MapName(mr.file, m))
			if err != nil {
				log.Fatal("Split Create:")
			}
			writer = bufio.NewWriter(outfile)
			m++
		}
		line := scanner.Text() + "\n"
		writer.WriteString(line)
		n += len(line)

	}

	writer.Flush()
	outfile.Close()

}

func (mr *MapReduce) Merge() {
	kvs := make(map[string] string)
	for i := 0; i < mr.nReduce; i++ {
		file, err := os.Open(MergeName(mr.file, i))
		if err != nil {
			log.Fatal("Merge:", err)
		}
		dec:=json.NewDecoder(file)

		for {
			kv:= new(KeyValue)
			err := dec.Decode(kv)
			if err!= nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}

	keys := []string{}
	for key := range kvs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	oFile, err := os.Create("mrtemp-" +  mr.file)
	if err != nil {
		log.Fatal("Merge:", err)
	}
	w := bufio.NewWriter(oFile)
	for _, k:=range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	oFile.Close()

}

// MapName name of file that is input for map job
func MapName(file string, mJop int) string {
	return "mtmp-" + file + "-" + strconv.Itoa(mJop) // mJop
}

// ReduceName the name of file that is input for reduce job
func ReduceName(file string, jNum, rJob int) string {
	return MapName(file, jNum) + "-" + strconv.Itoa(rJob)
}

func MergeName(file string, jobNum int) string {
	return "mtmp-" + file + "-res-" + strconv.Itoa(jobNum)
}

// DoMap do map
// open file
// read file byte
// create n Reduce
// json.NewEncoder
// iterate res
// encode keyvalue
// write to one of nReduce file
func DoMap(file string, jobNum, nReduce int) {
	opFile, err := os.Open(MapName(file, jobNum))
	if err != nil {
		log.Fatal("DoMap:", err)
	}
	fi, err := opFile.Stat()
	if err != nil {
		log.Fatal("DoMap:", err)

	}
	size := fi.Size()
	fmt.Printf("DoMap: read split %s %d \n", file, size)

	b := make([]byte, size)
	_, err = opFile.Read(b)

	if err != nil {
		log.Fatal("DoMap:", err)

	}
	opFile.Close()
	res := Map(string(b))
	for i := 0; i < nReduce; i++ {
		oFile, err := os.Create(ReduceName(file, jobNum, i))
		if err != nil {
			log.Fatal("DoMap:", err)
		}
		enc := json.NewEncoder(oFile)
		for f := res.Front(); f != nil; f = f.Next() {
			w := f.Value.(KeyValue)
			if ihash(w.Key)%uint32(nReduce) == uint32(i) {
				err := enc.Encode(&w)
				if err != nil {
					log.Fatal("DoMap:")
				}
			}

		}

	}
	//for f :=res.Front();f!=nil;res=res.Next(){

	//}

}

// DoReduce decode from reduce file
// use List store map[key] List{value}
// sort key
// count list length
// return sum
// creat Merge file
// encode KeyValue{key, sum}
// store it
func DoReduce(file string, jobNum, nMap int) {

	keyVs := make(map[string]*list.List)

	for i := 0; i < nMap; i++ {
		name := ReduceName(file, i, jobNum)
		file, err := os.Open(name)
		if err != nil {
			log.Fatal("DoReduce: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			kv := new(KeyValue)
			err := dec.Decode(kv)
			if err != nil {
				break
			}
			_, ok := keyVs[kv.Key]
			if !ok {
				keyVs[kv.Key] = list.New()
			}
			keyVs[kv.Key].PushBack(kv.Value)
		}
		file.Close()
	}
	var keys []string
	for key := range keyVs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// create outfile
	oFile, err := os.Create(MergeName(file, jobNum))
	if err != nil {
		log.Fatal("Reduce:", err)
	}
	enc := json.NewEncoder(oFile)
	for _, key := range keys {
		s := Reduce(key, keyVs[key]) // word1: 20, word2: 30
		enc.Encode(&KeyValue{key, s})
	}
	oFile.Close()

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Map store {word: 1 } {word1: 1} in List
func Map(s string) *list.List {
	split := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	words := strings.FieldsFunc(s, split)

	l := list.New()
	for _, word := range words {
		l.PushBack(KeyValue{word, "1"})
	}
	return l

}

// Reduce return sum of List.length
func Reduce(key string, l *list.List) string {
	sum := 0
	for e := l.Front(); e != nil; e = e.Next() {
		i, err := strconv.Atoi(e.Value.(string))
		if err != nil {
			log.Fatal("Reduce:", err)
		}
		sum += i

	}
	return strconv.Itoa(sum)
}
