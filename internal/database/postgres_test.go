package database

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfiguredDatabaseSchema(t *testing.T) {
	t.Setenv("WHATSAPP_BRIDGE_SCHEMA", "")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("database:\n  schema: custom_bridge\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	schema, err := configuredDatabaseSchema(path)
	if err != nil {
		t.Fatal(err)
	}
	if schema != "custom_bridge" {
		t.Fatalf("schema=%q", schema)
	}
	t.Setenv("WHATSAPP_BRIDGE_SCHEMA", "bridge_override")
	schema, err = configuredDatabaseSchema(path)
	if err != nil {
		t.Fatal(err)
	}
	if schema != "bridge_override" {
		t.Fatalf("override=%q", schema)
	}
}
