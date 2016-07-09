package zipkinhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"path"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/server"
)

func init() {
	server.RegisterQueryTransport("zipkinhttp", setup)
}

func setup(srv *server.Server, conf map[string]interface{}) (server.QueryTransport, error) {
	listen, ok := conf["listen"].(string)
	if !ok {
		return nil, errors.New("missing listen setting for HTTP transport")
	}
	h := &HTTP{
		srv:    srv,
		listen: listen,
		mux:    http.NewServeMux(),
	}

	h.mux.HandleFunc("/api/v1/services", h.Services)
	h.mux.HandleFunc("/api/v1/spans", h.Spans)
	h.mux.HandleFunc("/api/v1/traces", h.Traces)
	h.mux.HandleFunc("/api/v1/trace/", h.Trace)
	h.mux.HandleFunc("/api/v1/dependencies", h.Dependencies)
	return h, nil
}

type HTTP struct {
	srv    *server.Server
	listen string
	mux    *http.ServeMux
}

func (h *HTTP) Start() error {
	return http.ListenAndServe(h.listen, h.mux)
}

func (h *HTTP) Services(w http.ResponseWriter, r *http.Request) {
	services, err := h.srv.Storage.Services()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(services)
}

func (h *HTTP) Spans(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("serviceName")
	spans, err := h.srv.Storage.Spans(service)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(spans)
}

type zipkinTrace []zipkinSpan
type zipkinEndpoint struct {
	IPv4        string `json:"ipv4"`
	Port        int    `json:"port"`
	ServiceName string `json:"serviceName"`
}
type zipkinAnnotation struct {
	Endpoint  zipkinEndpoint `json:"endpoint"`
	Timestamp int            `json:"timestamp"`
	Value     string         `json:"value"`
}
type zipkinBinaryAnnotation struct {
	Endpoint zipkinEndpoint `json:"endpoint"`
	Key      string         `json:"key"`
	Value    string         `json:"value"`
}
type zipkinSpan struct {
	Annotations       []zipkinAnnotation       `json:"annotations"`
	BinaryAnnotations []zipkinBinaryAnnotation `json:"binaryAnnotations"`
	Debug             bool                     `json:"debug"`
	Duration          int                      `json:"duration"`
	ID                string                   `json:"id"`
	Name              string                   `json:"name"`
	ParentID          string                   `json:"parentId,omitempty"`
	Timestamp         int                      `json:"timestamp"`
	TraceID           string                   `json:"traceId"`
}

type zipkinBinaryAnnotations []zipkinBinaryAnnotation

func (s zipkinBinaryAnnotations) Len() int {
	return len(s)
}

func (s zipkinBinaryAnnotations) Less(i int, j int) bool {
	return s[i].Key < s[j].Key
}

func (s zipkinBinaryAnnotations) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func traceToZipkin(trace tracer.RawTrace) zipkinTrace {
	ztrace := zipkinTrace{}
	parents := map[uint64]uint64{}
	for _, rel := range trace.Relations {
		parents[rel.ChildID] = rel.ParentID
	}
	for _, span := range trace.Spans {
		var kind, opKind string
		switch span.Tags["span.kind"] {
		case "server":
			kind = "sr"
			opKind = "ss"
		case "client":
			kind = "cs"
			opKind = "cr"
		}
		zspan := zipkinSpan{
			Annotations: []zipkinAnnotation{
				{
					Endpoint: zipkinEndpoint{
						ServiceName: span.ServiceName,
					},
					Timestamp: int(span.StartTime.UnixNano()) / 1000,
					Value:     kind,
				},
				{
					Endpoint: zipkinEndpoint{
						ServiceName: span.ServiceName,
					},
					Timestamp: int(span.FinishTime.UnixNano()) / 1000,
					Value:     opKind,
				},
			},
			BinaryAnnotations: []zipkinBinaryAnnotation{},
			Debug:             false,
			Duration:          int(span.FinishTime.Sub(span.StartTime)) / 1000,
			ID:                fmt.Sprintf("%016x", span.SpanID),
			Name:              span.OperationName,
			ParentID:          fmt.Sprintf("%016x", parents[span.SpanID]),
			Timestamp:         int(span.StartTime.UnixNano() / 1000),
			TraceID:           fmt.Sprintf("%016x", trace.TraceID),
		}
		if parents[span.SpanID] == 0 {
			zspan.ParentID = ""
		}
		for k, v := range span.Tags {
			vs := fmt.Sprintf("%v", v)
			zspan.BinaryAnnotations = append(zspan.BinaryAnnotations, zipkinBinaryAnnotation{
				Key:   k,
				Value: vs,
			})
		}
		sort.Sort(zipkinBinaryAnnotations(zspan.BinaryAnnotations))
		ztrace = append(ztrace, zspan)
	}
	return ztrace
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func (h *HTTP) Traces(w http.ResponseWriter, r *http.Request) {
	limit := atoi(r.URL.Query().Get("limit"))
	if limit == 0 {
		limit = 10
	}
	minDuration := time.Duration(atoi(r.URL.Query().Get("minDuration"))) * time.Microsecond
	maxDuration := time.Duration(atoi(r.URL.Query().Get("maxDuration"))) * time.Microsecond
	msec := int64(atoi(r.URL.Query().Get("endTs")))
	endTs := time.Unix(msec/1000, (msec%1000)*1000)
	if msec == 0 {
		endTs = time.Now()
	}
	lookback := time.Duration(atoi(r.URL.Query().Get("lookback"))) * time.Millisecond

	traces, err := h.srv.Storage.QueryTraces(server.Query{
		StartTime:     endTs.Add(-lookback),
		FinishTime:    endTs,
		OperationName: "",
		MinDuration:   minDuration,
		MaxDuration:   maxDuration,
		AndTags:       nil,
		OrTags:        nil,
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	out := []zipkinTrace{}
	for _, trace := range traces {
		ztrace := traceToZipkin(trace)
		out = append(out, ztrace)
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (h *HTTP) Trace(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(path.Base(r.URL.Path), 16, 64)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	trace, err := h.srv.Storage.TraceByID(id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(traceToZipkin(trace))
}

func (h *HTTP) Dependencies(w http.ResponseWriter, r *http.Request) {
	type zipkinDependency struct {
		CallCount int    `json:"callCount"`
		Child     string `json:"child"`
		Parent    string `json:"parent"`
	}
	deps, err := h.srv.Storage.Dependencies()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	out := []zipkinDependency{}
	for _, dep := range deps {
		out = append(out, zipkinDependency{
			CallCount: int(dep.Count),
			Child:     dep.Child,
			Parent:    dep.Parent,
		})
	}
	_ = json.NewEncoder(w).Encode(out)
}
