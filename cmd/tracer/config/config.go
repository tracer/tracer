// Package config parses Tracer configuration files.
package config

import (
	"fmt"
	"io"

	"github.com/BurntSushi/toml"
)

// MissingSectionError is returned when a configuration section is
// missing.
type MissingSectionError string

func (err MissingSectionError) Error() string {
	return fmt.Sprintf("missing configuration section: %s", string(err))
}

// MissingKeyError is returned when a configuration key is missing.
type MissingKeyError string

func (err MissingKeyError) Error() string {
	return fmt.Sprintf("missing configuration key: %s", string(err))
}

// WrongValueTypeError is returned when a configuration key has the
// wrong type.
type WrongValueTypeError struct {
	Key  string
	Type string
}

func (err WrongValueTypeError) Error() string {
	return fmt.Sprintf("wrong type for configuration %s; expected type %s",
		err.Key, err.Type)
}

// Config is the Tracer configuration file.
type Config struct {
	cfg map[string]interface{}
}

// Load loads a configuration file.
func Load(r io.Reader) (Config, error) {
	cfg := Config{map[string]interface{}{}}
	_, err := toml.DecodeReader(r, &cfg.cfg)
	return cfg, err
}

func (cfg Config) storage() (map[string]interface{}, error) {
	gen, ok := cfg.cfg["storage"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("storage")
	}
	return gen, nil
}

func (cfg Config) query() (map[string]interface{}, error) {
	gen, ok := cfg.cfg["query"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("query")
	}
	return gen, nil
}

// Storage returns the name of the storage engine.
func (cfg Config) Storage() (string, error) {
	storage, err := cfg.storage()
	if err != nil {
		return "", err
	}
	engine, ok := storage["engine"]
	if !ok {
		return "", MissingKeyError("storage.engine")
	}
	s, ok := engine.(string)
	if !ok {
		return "", WrongValueTypeError{"storage.engine", "string"}
	}
	return s, nil
}

// StorageConfig returns the configuration of the storage engine.
func (cfg Config) StorageConfig() (map[string]interface{}, error) {
	engine, err := cfg.Storage()
	if err != nil {
		return nil, err
	}
	storage, ok := cfg.cfg["storage"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("storage")
	}
	conf, ok := storage[engine].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("storage." + engine)
	}
	return conf, nil
}

// StorageTransport returns the name of the storage transport.
func (cfg Config) StorageTransport() (string, error) {
	storage, err := cfg.storage()
	if err != nil {
		return "", err
	}
	transport, ok := storage["transport"]
	if !ok {
		return "", MissingKeyError("storage.transport")
	}
	s, ok := transport.(string)
	if !ok {
		return "", WrongValueTypeError{"storage.transport", "string"}
	}
	return s, nil
}

// StorageTransportConfig returns the configuration of the storage transport.
func (cfg Config) StorageTransportConfig() (map[string]interface{}, error) {
	engine, err := cfg.StorageTransport()
	if err != nil {
		return nil, err
	}
	transport, ok := cfg.cfg["storage"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("storage")
	}
	conf, ok := transport[engine].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("storage." + engine)
	}
	return conf, nil
}

// QueryTransports returns the names of the query transports.
func (cfg Config) QueryTransports() ([]string, error) {
	query, err := cfg.query()
	if err != nil {
		return nil, err
	}
	transport, ok := query["transports"]
	if !ok {
		return nil, MissingKeyError("query.transports")
	}
	s, ok := transport.([]interface{})
	if !ok {
		return nil, WrongValueTypeError{"query.transports", "[]string"}
	}
	var ss []string
	for _, v := range s {
		vs, ok := v.(string)
		if !ok {
			return nil, WrongValueTypeError{"query.transports", "[]string"}
		}
		ss = append(ss, vs)
	}
	return ss, nil
}

// QueryTransportConfig returns the configuration of a query transport.
func (cfg Config) QueryTransportConfig(engine string) (map[string]interface{}, error) {
	transport, ok := cfg.cfg["query"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("query")
	}
	conf, ok := transport[engine].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("query." + engine)
	}
	return conf, nil
}
