package dalgo2files

import (
	"os"
	"testing"
)

func TestNewDB_DirectoryValidation(t *testing.T) {
	t.Run("non-existent directory", func(t *testing.T) {
		_, err := NewDB("/non/existent/path", schemaDefinition{})
		if err == nil {
			t.Error("Expected error for non-existent directory, but got none")
		}
		expectedMsg := "directory does not exist: /non/existent/path"
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
		}
	})

	t.Run("valid directory", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test_dir")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)

		_, err = NewDB(tempDir, schemaDefinition{})
		if err != nil {
			t.Errorf("Unexpected error for valid directory: %v", err)
		}
	})

	t.Run("file instead of directory", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())
		tempFile.Close()

		_, err = NewDB(tempFile.Name(), schemaDefinition{})
		if err == nil {
			t.Error("Expected error for file instead of directory, but got none")
		}
		expectedMsg := "path is not a directory: " + tempFile.Name()
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
		}
	})
}
