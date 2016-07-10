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

func (cfg Config) general() (map[string]interface{}, error) {
	gen, ok := cfg.cfg["general"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("general")
	}
	return gen, nil
}

// Storage returns the name of the storage engine.
func (cfg Config) Storage() (string, error) {
	gen, err := cfg.general()
	if err != nil {
		return "", err
	}
	storage, ok := gen["storage"]
	if !ok {
		return "", MissingKeyError("general.storage")
	}
	s, ok := storage.(string)
	if !ok {
		return "", WrongValueTypeError{"general.storage", "string"}
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
	gen, err := cfg.general()
	if err != nil {
		return "", err
	}
	transport, ok := gen["storage_transport"]
	if !ok {
		return "", MissingKeyError("general.storage_transport")
	}
	s, ok := transport.(string)
	if !ok {
		return "", WrongValueTypeError{"general.storage_transport", "string"}
	}
	return s, nil
}

// StorageTransportConfig returns the configuration of the storage transport.
func (cfg Config) StorageTransportConfig() (map[string]interface{}, error) {
	engine, err := cfg.StorageTransport()
	if err != nil {
		return nil, err
	}
	transport, ok := cfg.cfg["storage_transport"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("storage_transport")
	}
	conf, ok := transport[engine].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("storage_transport." + engine)
	}
	return conf, nil
}

// QueryTransports returns the names of the query transports.
func (cfg Config) QueryTransports() ([]string, error) {
	gen, err := cfg.general()
	if err != nil {
		return nil, err
	}
	transport, ok := gen["query_transports"]
	if !ok {
		return nil, MissingKeyError("general.query_transports")
	}
	s, ok := transport.([]interface{})
	if !ok {
		return nil, WrongValueTypeError{"general.query_transports", "[]string"}
	}
	var ss []string
	for _, v := range s {
		vs, ok := v.(string)
		if !ok {
			return nil, WrongValueTypeError{"general.query_transports", "[]string"}
		}
		ss = append(ss, vs)
	}
	return ss, nil
}

// QueryTransportConfig returns the configuration of a query transport.
func (cfg Config) QueryTransportConfig(engine string) (map[string]interface{}, error) {
	transport, ok := cfg.cfg["query_transport"].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("query_transport")
	}
	conf, ok := transport[engine].(map[string]interface{})
	if !ok {
		return nil, MissingSectionError("query_transport." + engine)
	}
	return conf, nil
}
