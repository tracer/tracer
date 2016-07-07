package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/cmd/tracer/config"
	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/server"
	"github.com/tracer/tracer/storage"
	_ "github.com/tracer/tracer/storage/postgres"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
)

func loadStorage(conf config.Config) (tracer.Storer, error) {
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
	engine, err := conf.Transport()
	if err != nil {
		return err
	}
	switch engine {
	case "grpc":
		return listenGRPC(srv, conf)
	default:
		return fmt.Errorf("unsupported transport engine: %s", engine)
	}
}

func listenGRPC(srv *server.Server, conf config.Config) error {
	transport, err := conf.TransportConfig()
	if err != nil {
		return err
	}
	listen, ok := transport["listen"].(string)
	if !ok {
		return errors.New("missing listen setting for gRPC transport")
	}
	l, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterStorerServer(s, srv)
	return s.Serve(l)
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
	srv := &server.Server{Storer: storage}
	if err := listen(srv, conf); err != nil {
		log.Fatalln("Error running transport:", err)
	}
}
