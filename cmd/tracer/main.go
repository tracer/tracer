package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/cmd/tracer/config"
	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/server"
	"github.com/tracer/tracer/storage/postgres"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
)

func loadStorage(conf config.Config) (tracer.Storer, error) {
	engine, err := conf.Storage()
	if err != nil {
		return nil, err
	}
	switch engine {
	case "postgres":
		return loadPostgres(conf)
	default:
		return nil, fmt.Errorf("unsupported storage engine: %s", engine)
	}
}

func loadPostgres(conf config.Config) (tracer.Storer, error) {
	storageConf, err := conf.StorageConfig()
	if err != nil {
		return nil, err
	}
	url, ok := storageConf["url"].(string)
	if !ok {
		return nil, errors.New("missing url for postgres backend")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("drror connecting to PostgreSQL database: %s", err)
	}
	return postgres.New(db), nil
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
