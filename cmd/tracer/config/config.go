package config

import (
	"fmt"
	"io"

	"github.com/BurntSushi/toml"
)

type MissingSectionError string

func (err MissingSectionError) Error() string {
	return fmt.Sprintf("missing configuration section: %s", string(err))
}

type MissingKeyError string

func (err MissingKeyError) Error() string {
	return fmt.Sprintf("missing configuration key: %s", string(err))
}

type WrongValueTypeError struct {
	Key  string
	Type string
}

func (err WrongValueTypeError) Error() string {
	return fmt.Sprintf("wrong type for configuration %s; xpected type %s",
		err.Key, err.Type)
}

type Config struct {
	cfg map[string]interface{}
}

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
