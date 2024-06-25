package main

import (
    "bufio"
    "flag"
    "fmt"
    "log"
    "math"
    "os"
    "sort"
    "strings"
    "strconv"
)

var input = flag.String("input", "", "path to the input file")
var debug = flag.Bool("debug", false, "enable debug mode")

type City struct {
    name string
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

func main() {
    flag.Parse()

    file, err := os.Open(*input)
    logDebug("Opening file %s", *input)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    m := make(map[string]*City)

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()

        i := strings.Index(line, ";")
        name := line[:i]

        c, ok := m[name]

        if ok {
            f, err := strconv.ParseFloat(line[i+1:], 64)
            if err == nil {
                if f < c.minTemp {
                    c.minTemp = f
                } else if f > c.maxTemp {
                    c.maxTemp = f
                }

                c.totalTemp += f
                c.total++
            }
        } else {
            f, err := strconv.ParseFloat(line[i+1:], 64)
            if err == nil {
                m[name] = &City{
                    name: name,
                    minTemp: f,
                    maxTemp: f,
                    totalTemp: f,
                    total: 1,
                }
            }
        }

        logDebug(line)
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
        return
    }

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
            sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", c.name, c.minTemp, avgTemp, c.maxTemp))
            sb.WriteString(", ")
        }
    }
    fmt.Print(sb.String()[:sb.Len()-2], "}")
}

