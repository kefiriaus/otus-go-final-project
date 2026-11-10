package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type fileConfig struct {
	MaxWindowSeconds json.RawMessage `json:"max_window_seconds"`
	Subsystems       json.RawMessage `json:"subsystems"`
}

type fileSubsystems struct {
	LoadAverage json.RawMessage `json:"load_average"`
	CPU         json.RawMessage `json:"cpu"`
	DiskIO      json.RawMessage `json:"disk_io"`
	Filesystems json.RawMessage `json:"filesystems"`
	Traffic     json.RawMessage `json:"traffic"`
	Connections json.RawMessage `json:"connections"`
}

// LoadJSON overlays a single strict JSON object on defaults. Errors return no configuration.
func LoadJSON(r io.Reader) (Config, error) {
	var raw json.RawMessage
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("read configuration: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return Config{}, fmt.Errorf("after configuration: %w", err)
		}
		return Config{}, fmt.Errorf("expected a single JSON object")
	}
	var file fileConfig
	if err := decodeObject(raw, &file); err != nil {
		return Config{}, fmt.Errorf("configuration: %w", err)
	}
	cfg := Default()
	if err := decodeValue("max_window_seconds", file.MaxWindowSeconds, &cfg.MaxWindowSeconds); err != nil {
		return Config{}, err
	}
	if len(file.Subsystems) > 0 {
		var sub fileSubsystems
		if err := decodeObject(file.Subsystems, &sub); err != nil {
			return Config{}, fmt.Errorf("subsystems: %w", err)
		}
		for _, field := range []struct {
			name   string
			raw    json.RawMessage
			target *bool
		}{
			{"load_average", sub.LoadAverage, &cfg.Subsystems.LoadAverage},
			{"cpu", sub.CPU, &cfg.Subsystems.CPU},
			{"disk_io", sub.DiskIO, &cfg.Subsystems.DiskIO},
			{"filesystems", sub.Filesystems, &cfg.Subsystems.Filesystems},
			{"traffic", sub.Traffic, &cfg.Subsystems.Traffic},
			{"connections", sub.Connections, &cfg.Subsystems.Connections},
		} {
			if err := decodeValue("subsystems."+field.name, field.raw, field.target); err != nil {
				return Config{}, err
			}
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func decodeObject(raw json.RawMessage, target any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("must be an object, not null")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func decodeValue(name string, raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("%s: null is not allowed", name)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}
