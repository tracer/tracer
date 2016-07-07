package transport

import "github.com/tracer/tracer/server"

type Engine func(conf map[string]interface{}) (Transport, error)

var engines = map[string]Engine{}

func Register(name string, engine Engine) {
	engines[name] = engine
}

func Get(name string) (Engine, bool) {
	transport, ok := engines[name]
	return transport, ok
}

type Transport interface {
	Start(srv *server.Server) error
}
