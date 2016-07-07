package grpc

import (
	"errors"
	"net"

	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/server"
	"github.com/tracer/tracer/transport"
	"google.golang.org/grpc"
)

func init() {
	transport.Register("grpc", setup)
}

type GRPC struct {
	listen string
}

func (g *GRPC) Start(srv *server.Server) error {
	l, err := net.Listen("tcp", g.listen)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterStorerServer(s, srv)
	return s.Serve(l)
}

func setup(conf map[string]interface{}) (transport.Transport, error) {
	listen, ok := conf["listen"].(string)
	if !ok {
		return nil, errors.New("missing listen setting for gRPC transport")
	}
	return &GRPC{
		listen: listen,
	}, nil
}
