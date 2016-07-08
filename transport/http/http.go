package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/tracer/tracer/server"
)

func init() {
	server.RegisterQueryTransport("http", setup)
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

	h.mux.HandleFunc("/trace/", h.TraceByID)
	h.mux.HandleFunc("/span/", h.SpanByID)
	h.mux.HandleFunc("/trace/query/", h.QueryTraces)
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

func (h *HTTP) TraceByID(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 16, 64)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	trace, err := h.srv.Storage.TraceByID(id)
	if err != nil {
		// TODO(dh): handle 404 special
		http.Error(w, err.Error(), 500)
		return
	}
	// TODO(dh): embed error in the JSON
	_ = json.NewEncoder(w).Encode(trace)
}

func (h *HTTP) SpanByID(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 16, 64)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	span, err := h.srv.Storage.SpanByID(id)
	if err != nil {
		// TODO(dh): handle 404 special
		http.Error(w, err.Error(), 500)
		return
	}
	// TODO(dh): embed error in the JSON
	_ = json.NewEncoder(w).Encode(span)
}

func (h *HTTP) QueryTraces(w http.ResponseWriter, r *http.Request) {

}
