package storage

import "github.com/tracer/tracer"

type Engine func(conf map[string]interface{}) (tracer.Storer, error)

var engines = map[string]Engine{}

func Register(name string, engine Engine) {
	engines[name] = engine
}

func Get(name string) (Engine, bool) {
	storer, ok := engines[name]
	return storer, ok
}
