// Command tracer is the Tracer query and storage server.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/lygo/tracer/cmd/tracer/config"
	"github.com/lygo/tracer/server"
	_ "github.com/lygo/tracer/storage/null"
	_ "github.com/lygo/tracer/storage/postgres"
	_ "github.com/lygo/tracer/transport/grpc"
	_ "github.com/lygo/tracer/transport/http"
	_ "github.com/lygo/tracer/transport/zipkinhttp"
)

func loadStorage(conf config.Config) (server.Storage, error) {
	name, err := conf.Storage()
	if err != nil {
		return nil, err
	}
	storer, ok := server.GetStorage(name)
	if !ok {
		return nil, fmt.Errorf("unsupported storage engine: %s", name)
	}
	storageConf, err := conf.StorageConfig()
	if err != nil {
		return nil, err
	}
	return storer(storageConf)
}

func loadStorageTransport(srv *server.Server, conf config.Config) (server.StorageTransport, error) {
	name, err := conf.StorageTransport()
	if err != nil {
		return nil, err
	}
	fn, ok := server.GetStorageTransport(name)
	if !ok {
		return nil, fmt.Errorf("unsupported storage transport: %s", name)
	}
	transportConf, err := conf.StorageTransportConfig()
	if err != nil {
		return nil, err
	}
	return fn(srv, transportConf)
}

func loadQueryers(srv *server.Server, conf config.Config) ([]server.QueryTransport, error) {
	var out []server.QueryTransport

	transports, err := conf.QueryTransports()
	if err != nil {
		return nil, err
	}
	for _, name := range transports {
		fn, ok := server.GetQueryTransport(name)
		if !ok {
			return nil, fmt.Errorf("unsupported query transport: %s", name)
		}
		transportConf, err := conf.QueryTransportConfig(name)
		if err != nil {
			return nil, err
		}
		transport, err := fn(srv, transportConf)
		if err != nil {
			return nil, err
		}

		out = append(out, transport)
	}
	return out, nil
}

var fConfig string

func init() {
	flag.StringVar(&fConfig, "c", "", "Path to config `file`")
}

func main() {
	flag.Parse()

	f, err := os.Open(fConfig)
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
	srv.StorageTransport, err = loadStorageTransport(srv, conf)
	if err != nil {
		log.Fatal(err)
	}
	srv.QueryTransports, err = loadQueryers(srv, conf)
	if err != nil {
		log.Fatal(err)
	}
	srv.Start()
}
