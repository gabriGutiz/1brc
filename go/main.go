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

var BUFFER_CHUNK_SIZE = 64 * 1024 * 1024
var N_CONSUMERS = runtime.NumCPU()

var cpuProfile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memProfile = flag.String("memprofile", "", "write memory profile to `file`")
var input = flag.String("input", "", "path to the input file")
var debug = flag.Bool("debug", false, "enable debug mode")

type temperatureInfo struct {
    minTemp, maxTemp, totalTemp, total int
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

func customByteToInt(byteArr []byte) (result int) {
    signal := 1
    if byteArr[0] == '-' {
        byteArr = byteArr[1:]
        signal = -1
    }

    if len(byteArr) == 3 {
        result += int(byteArr[0] - '0') * 10 + int(byteArr[2] - '0')
    } else {
        result += int(byteArr[0] - '0') * 100 + int(byteArr[1] - '0') * 10 + int(byteArr[3] - '0')
    }

    return result * signal
}

func chunkProducer(file os.File, chunkChan chan []byte, mapsChan chan map[string]*temperatureInfo, wg *sync.WaitGroup) {
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

        toSend := buf[:leftLen + total]

        lastNewLine := bytes.LastIndexByte(buf, '\n')

        buf = make([]byte, BUFFER_CHUNK_SIZE)
        leftLen = copy(buf, toSend[lastNewLine+1:])

        chunkChan <- toSend[:lastNewLine+1]
    }

    close(chunkChan)
    wg.Wait()
    close(mapsChan)
}

func chunkConsumer(chunk []byte, mapsChan chan map[string]*temperatureInfo) {
    tempInfoToSend := make(map[string]*temperatureInfo)
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
            if temp < c.minTemp {
                c.minTemp = temp
            } else if temp > c.maxTemp {
                c.maxTemp = temp
            }

            c.totalTemp += temp
            c.total++
        } else {
            tempInfoToSend[cityName] = &temperatureInfo{
                minTemp: temp,
                maxTemp: temp,
                totalTemp: temp,
                total: 1,
            }
        }
    }
    mapsChan <- tempInfoToSend
}

func process() string {
    flag.Parse()

    file, err := os.Open(*input)
    //logDebug("Opening file %s", *input)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    chunksChan := make(chan []byte, N_CONSUMERS - 1)
    mapsChan := make(chan map[string]*temperatureInfo)
    var wg sync.WaitGroup

    for i := 0; i < N_CONSUMERS - 1; i++ {
        wg.Add(1)
        go func() {
            for chunk := range chunksChan {
                //logDebug("Processing chunk: %s", string(chunk))
                chunkConsumer(chunk, mapsChan)
            }
            wg.Done()
        }()
    }
    go chunkProducer(*file, chunksChan, mapsChan, &wg)

    m := make(map[string]*temperatureInfo)
    for cityMap := range mapsChan {
        for i, val := range cityMap {
            c, ok := m[i]

            if ok {
                if val.minTemp < c.minTemp {
                    c.minTemp = val.minTemp
                }
                if val.maxTemp > c.maxTemp {
                    c.maxTemp = val.maxTemp
                }

                c.totalTemp += val.totalTemp
                c.total += val.total
            } else {
                m[i] = val
            }
        }
    }


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
            avgTemp := round(float64(c.totalTemp) / 10.0 / float64(c.total))
            sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", k, float32(c.minTemp)/10.0, avgTemp, float32(c.maxTemp)/10.0))
            sb.WriteString(", ")
        }
    }
    return sb.String()[:sb.Len()-2] + "}"
}

func main() {
    flag.Parse()

    if *cpuProfile != "" {
        //logDebug("Creating CPU profile %s", *cpuProfile)
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

