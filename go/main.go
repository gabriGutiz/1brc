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
    "strconv"
    "sync"
)

var BUFFER_CHUNK_SIZE = 64 * 1024 * 1024

var cpuProfile = flag.String("cpuprofile", "cpu.prof", "write cpu profile to `file`")
var memProfile = flag.String("memprofile", "mem.prof", "write memory profile to `file`")
var input = flag.String("input", "", "path to the input file")
var debug = flag.Bool("debug", false, "enable debug mode")

type City struct {
    minTemp, maxTemp, totalTemp float64
    total int
}

func round(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}

func logDebug(message string, args ...interface{}) {
    if *debug {
        log.Printf(message, args...)
    }
}

func chunkProducer(file os.File, chunkChan chan []byte) {
    buf := make([]byte, BUFFER_CHUNK_SIZE)
    left := make([]byte, 0, BUFFER_CHUNK_SIZE)
    leftLen := 0

    for {
        total, err := file.Read(buf)
        if err != nil {
            if errors.Is(err, io.EOF) {
                break
            }
            panic(err)
        }

        buf = buf[:total]

        lastNewLine := bytes.LastIndexByte(buf, '\n')

        send := make([]byte, lastNewLine + leftLen + 1)
        copy(send, left[:leftLen])
        copy(send[leftLen:], buf[:lastNewLine + 1])

        leftLen = total - lastNewLine - 1
        left = make([]byte, leftLen)
        copy(left, buf[lastNewLine+1:])

        chunkChan <- send
    }

    close(chunkChan)
}

func chunkConsumer(chunk []byte, cities *map[string]*City) {
    lastIndex := 0
    var cityName string

    for i, b := range chunk {
        switch b {
        case ';':
            cityName = string(chunk[lastIndex:i])
            logDebug("Index: %d | City name: %s", i, cityName)
            lastIndex = i + 1
        case '\n':
            temp, err := strconv.ParseFloat(string(chunk[lastIndex:i]), 64)
            if err == nil {
                logDebug("Temperature: %f", temp)
                c, ok := (*cities)[cityName]
                if ok {
                    if temp < c.minTemp {
                        c.minTemp = temp
                    } else if temp > c.maxTemp {
                        c.maxTemp = temp
                    }

                    c.totalTemp += temp
                    c.total++
                } else {
                    (*cities)[cityName] = &City{
                        minTemp: temp,
                        maxTemp: temp,
                        totalTemp: temp,
                        total: 1,
                    }
                }
            }
            lastIndex = i + 1
        }
    }
}

func process() string {
    flag.Parse()

    file, err := os.Open(*input)
    logDebug("Opening file %s", *input)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    cpus := runtime.NumCPU()

    chunksChan := make(chan []byte, cpus - 1)
    m := make(map[string]*City)
    var wg sync.WaitGroup

    wg.Add(1)
    go func() {
        for chunk := range chunksChan {
            logDebug("Processing chunk: %s", string(chunk))
            chunkConsumer(chunk, &m)
        }
        wg.Done()
    }()
    go chunkProducer(*file, chunksChan)
    wg.Wait()

    keys := make([]string, 0, len(m))
    for key := range m {
        keys = append(keys, key)
    }
    sort.Strings(keys)

    var sb strings.Builder
    sb.WriteString("{")
    for _, k := range keys {
        c, ok := m[k]

        if ok {
            avgTemp := round(c.totalTemp / float64(c.total))
            sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", k, c.minTemp, avgTemp, c.maxTemp))
            sb.WriteString(", ")
        }
    }
    return sb.String()[:sb.Len()-2] + "}"
}

func main() {
    flag.Parse()

    if *cpuProfile != "" {
        logDebug("Creating CPU profile %s", *cpuProfile)
		f, err := os.Create("./profiles/" + *cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
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
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

