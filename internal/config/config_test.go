package config

import "testing"

func TestDefault(t *testing.T) {
	want := Config{Port: 50051, MaxWindowSeconds: 3600, Subsystems: Subsystems{true, true, true, true, true, true}}
	if got := Default(); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	first := Default()
	first.Subsystems.CPU = false
	if Default() != want {
		t.Fatal("defaults share mutable state")
	}
}
func TestValidate(t *testing.T) {
	for _, tt := range []struct {
		name         string
		port, window int
		valid        bool
	}{
		{"minimum", 1, 1, true}, {"maximum", 65535, 3600, true},
		{"negative port", -1, 3600, false}, {"zero port", 0, 3600, false}, {"large port", 65536, 3600, false},
		{"negative window", 50051, -1, false}, {"zero window", 50051, 0, false}, {"large window", 50051, 3601, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := Default()
			c.Port = tt.port
			c.MaxWindowSeconds = tt.window
			if err := c.Validate(); (err == nil) != tt.valid {
				t.Fatalf("valid=%v, error=%v", tt.valid, err)
			}
		})
	}
	c := Default()
	c.Subsystems = Subsystems{}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
