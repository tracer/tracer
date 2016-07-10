// Command tracer-cli provides a CLI query client.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/client"
)

var fHost string

func init() {
	flag.StringVar(&fHost, "h", "http://localhost:9998", "The Tracer query server")
}

func main() {
	flag.Parse()
	q := client.NewQueryClient(fHost)
	num, err := strconv.ParseUint(os.Args[1], 16, 64)
	if err != nil {
		log.Fatalln("Invalid ID:", err)
	}
	trace, err := q.TraceByID(num)
	if err != nil {
		log.Fatal(err)
	}

	// printSpan(span)
	printTrace(trace)
}

func formatTags(tags map[string]interface{}) string {
	var out []string
	for k, v := range tags {
		if v == nil {
			out = append(out, k)
		} else {
			out = append(out, fmt.Sprintf("%s=%#v", k, v))
		}
	}
	return strings.Join(out, ", ")
}

func printSpan(sp tracer.RawSpan) {
	const format = `%s:%s (trace %016x) [%s]
%s – %s (%s)
`

	fmt.Printf(format,
		sp.ServiceName, sp.OperationName, sp.TraceID, formatTags(sp.Tags),
		sp.StartTime.Format("15:04:05"), sp.FinishTime.Format("15:04:05"), sp.FinishTime.Sub(sp.StartTime))
	if len(sp.Logs) > 0 {

	}
}

func printTrace(tr tracer.RawTrace) {
	parents := map[uint64]uint64{}
	for _, rel := range tr.Relations {
		parents[rel.ChildID] = rel.ParentID
	}
	var printSubtrace func(parent uint64, level int)
	printSubtrace = func(parent uint64, level int) {
		for _, sp := range tr.Spans {
			if parents[sp.SpanID] != parent {
				continue
			}
			for i := 0; i < level; i++ {
				fmt.Print("\t")
			}
			fmt.Printf("%s (%s) [%s]\n", sp.OperationName, sp.FinishTime.Sub(sp.StartTime), formatTags(sp.Tags))
			printSubtrace(sp.SpanID, level+1)
		}
	}
	printSubtrace(0, 0)
}
