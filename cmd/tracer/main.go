package main

import (
	"fmt"
	"log"
	"os"

	"github.com/tracer/tracer/cmd/tracer/config"
	"github.com/tracer/tracer/server"
	"github.com/tracer/tracer/storage"
	_ "github.com/tracer/tracer/storage/postgres"
	"github.com/tracer/tracer/transport"
	_ "github.com/tracer/tracer/transport/grpc"
)

func loadStorage(conf config.Config) (server.Storage, error) {
	name, err := conf.Storage()
	if err != nil {
		return nil, err
	}
	storer, ok := storage.Get(name)
	if !ok {
		return nil, fmt.Errorf("unsupported storage engine: %s", name)
	}
	storageConf, err := conf.StorageConfig()
	if err != nil {
		return nil, err
	}
	return storer(storageConf)
}

func listen(srv *server.Server, conf config.Config) error {
	name, err := conf.Transport()
	if err != nil {
		return err
	}
	fn, ok := transport.Get(name)
	if !ok {
		return fmt.Errorf("unsupported transport: %s", name)
	}
	transportConf, err := conf.TransportConfig()
	if err != nil {
		return err
	}
	transport, err := fn(srv, transportConf)
	if err != nil {
		return err
	}
	return transport.Start()
}

func main() {
	f, err := os.Open("example.conf")
	if err != nil {
		log.Fatalln("Couldn't load config:", err)
	}
	conf, err := config.Load(f)
	if err != nil {
		log.Fatalln("Couldn't load config:", err)
	}
	_ = f.Close()

	storage, err := loadStorage(conf)
	if err != nil {
		log.Fatal(err)
	}
	srv := &server.Server{Storage: storage}
	if err := listen(srv, conf); err != nil {
		log.Fatalln("Error running transport:", err)
	}
}
