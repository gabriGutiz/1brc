package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
)

// Large chunks reduce read and channel overhead while keeping enough work for
// all consumer goroutines to run in parallel.
var BUFFER_CHUNK_SIZE = 2048 * 2048

// One CPU is left for the producer and merge loop.
var N_CONSUMERS = runtime.NumCPU()

var cpuProfile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memProfile = flag.String("memprofile", "", "write memory profile to `file`")
var input = flag.String("input", "", "path to the input file")

func round(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}

// The challenge temperatures always have one decimal place. Parsing the
// digits directly is cheaper than allocating strings and calling a general
// floating-point parser. The result is stored in tenths of a degree.
func customByteToInt(byteArr []byte) (result int) {
	signal := 1
	if byteArr[0] == '-' {
		byteArr = byteArr[1:]
		signal = -1
	}

	if len(byteArr) == 3 {
		result += int(byteArr[0]-'0')*10 + int(byteArr[2]-'0')
	} else {
		result += int(byteArr[0]-'0')*100 + int(byteArr[1]-'0')*10 + int(byteArr[3]-'0')
	}

	return result * signal
}

// chunkProducer owns the input buffers. It sends only complete lines so a
// consumer never has to coordinate with another consumer to finish a record.
func chunkProducer(file os.File, chunkChan chan []byte, mapsChan chan map[string]*[4]int, wg *sync.WaitGroup) {
	buf := make([]byte, BUFFER_CHUNK_SIZE)
	leftLen := 0

	for {
		total, err := file.Read(buf[leftLen:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}

		toSend := buf[:leftLen+total]

		lastNewLine := bytes.LastIndexByte(buf, '\n')

		// Allocate a new buffer because the previous buffer is now owned by a
		// consumer through chunkChan.
		buf = make([]byte, BUFFER_CHUNK_SIZE)
		leftLen = copy(buf, toSend[lastNewLine+1:])

		chunkChan <- toSend[:lastNewLine+1]
	}

	close(chunkChan)
	// Consumers may still be sending their final maps. Wait before closing
	// mapsChan so the merge loop can safely receive every result.
	wg.Wait()
	close(mapsChan)
}

// Each consumer aggregates its entire chunk locally. The four values in the
// array are [minimum, maximum, sum, count], all in integer tenths.
func chunkConsumer(chunk []byte, mapsChan chan map[string]*[4]int) {
	tempInfoToSend := make(map[string]*[4]int)
	var cityName string
	chunkLen := len(chunk)
	lastIndex := 0

	for lastIndex < chunkLen {
		endNameIndex := bytes.IndexByte(chunk[lastIndex:], ';') + lastIndex

		cityName = string(chunk[lastIndex:endNameIndex])
		endNameIndex++

		lastIndex = bytes.IndexByte(chunk[endNameIndex:], '\n') + endNameIndex

		temp := customByteToInt(chunk[endNameIndex:lastIndex])
		lastIndex++

		c, ok := tempInfoToSend[cityName]
		if ok {
			if temp < c[0] {
				c[0] = temp
			} else if temp > c[1] {
				c[1] = temp
			}

			c[2] += temp
			c[3]++
		} else {
			tempInfoToSend[cityName] = &[4]int{temp, temp, temp, 1}
		}
	}
	mapsChan <- tempInfoToSend
}

func process() string {
	flag.Parse()

	file, err := os.Open(*input)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// A buffered work queue lets the producer read ahead while consumers parse.
	chunksChan := make(chan []byte, N_CONSUMERS-1)
	mapsChan := make(chan map[string]*[4]int)
	var wg sync.WaitGroup

	for i := 0; i < N_CONSUMERS-1; i++ {
		wg.Add(1)
		go func() {
			for chunk := range chunksChan {
				chunkConsumer(chunk, mapsChan)
			}
			wg.Done()
		}()
	}
	go chunkProducer(*file, chunksChan, mapsChan, &wg)

	// Only this goroutine owns the final map, so merging needs no mutex.
	m := make(map[string]*[4]int)
	for cityMap := range mapsChan {
		for i, val := range cityMap {
			c, ok := m[i]

			if ok {
				if val[0] < c[0] {
					c[0] = val[0]
				}
				if val[1] > c[1] {
					c[1] = val[1]
				}

				c[2] += val[2]
				c[3] += val[3]
			} else {
				m[i] = &[4]int{val[0], val[1], val[2], val[3]}
			}
		}
	}

	// Sorting is done once per distinct city, after the billion-row hot path.
	keys := make([]string, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString("{")
	for _, k := range keys {
		c, ok := m[k]

		if ok {
			avgTemp := round(float64(c[2]) / 10.0 / float64(c[3]))
			sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", k, float32(c[0])/10.0, avgTemp, float32(c[1])/10.0))
			sb.WriteString(", ")
		}
	}
	return sb.String()[:sb.Len()-2] + "}"
}

func main() {
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create("./profiles/" + *cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	fmt.Print(process())

	if *memProfile != "" {
		f, err := os.Create("./profiles/" + *memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
