package riffdbopenfga

import (
	"testing"

	riffdb "riffdb.dev/application"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{name: "valid", config: Config{Session: &riffdb.Session{}, CommandAttempts: 3}},
		{name: "missing session", config: Config{CommandAttempts: 3}, wantErr: true},
		{name: "zero attempts", config: Config{Session: &riffdb.Session{}}, wantErr: true},
		{name: "too many attempts", config: Config{Session: &riffdb.Session{}, CommandAttempts: 11}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.config.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
