package dalgo2files

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

type userDTO struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestNewDB(t *testing.T) {
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
		tempDir := t.TempDir()
		_, err := NewDB(tempDir, schemaDefinition{})
		if err != nil {
			t.Errorf("Unexpected error for valid directory: %v", err)
		}
	})

	t.Run("file instead of directory", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer func() { _ = os.Remove(tempFile.Name()) }()
		_ = tempFile.Close()

		_, err = NewDB(tempFile.Name(), schemaDefinition{})
		if err == nil {
			t.Error("Expected error for file instead of directory, but got none")
		}
		expectedMsg := "path is not a directory: " + tempFile.Name()
		if err.Error() != expectedMsg {
			t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
		}
	})

	t.Run("stat error not not-exist", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer func() { _ = os.Remove(tempFile.Name()) }()
		_ = tempFile.Close()

		_, err = NewDB(tempFile.Name()+"/sub", schemaDefinition{})
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "failed to check directory") {
			t.Errorf("expected failed to check directory, got: %v", err)
		}
	})

	t.Run("schema validation failure", func(t *testing.T) {
		tempDir := t.TempDir()
		schema := NewSchemaDefinition(map[string]*CollectionDef{
			"bad": {},
		})
		_, err := NewDB(tempDir, schema)
		if err == nil {
			t.Fatal("expected error from invalid schema definition")
		}
	})
}

func TestDatabase_BasicMethods(t *testing.T) {
	tempDir := t.TempDir()
	schema := NewSchemaDefinition(map[string]*CollectionDef{})
	dbInstance, err := NewDB(tempDir, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	d := database{
		dirPath:          tempDir,
		SchemaDefinition: schema,
	}

	if d.ID() != tempDir {
		t.Errorf("expected %q, got %q", tempDir, d.ID())
	}
	if d.Adapter().Name() != DalgoProviderID {
		t.Errorf("expected %q, got %q", DalgoProviderID, d.Adapter().Name())
	}
	if d.Schema() != nil {
		t.Errorf("expected nil schema, got %v", d.Schema())
	}

	ctx := context.Background()
	if err := dbInstance.RunReadonlyTransaction(ctx, nil); !errors.Is(err, dal.ErrNotSupported) {
		t.Errorf("expected ErrNotSupported, got %v", err)
	}
	if err := dbInstance.RunReadwriteTransaction(ctx, nil); !errors.Is(err, dal.ErrNotImplementedYet) {
		t.Errorf("expected ErrNotImplementedYet, got %v", err)
	}
	if _, err := dbInstance.ExecuteQueryToRecordsReader(ctx, nil); !errors.Is(err, dal.ErrNotImplementedYet) {
		t.Errorf("expected ErrNotImplementedYet, got %v", err)
	}
	if _, err := dbInstance.ExecuteQueryToRecordsetReader(ctx, nil); !errors.Is(err, dal.ErrNotImplementedYet) {
		t.Errorf("expected ErrNotImplementedYet, got %v", err)
	}
}

func TestDatabase_IndividualFiles(t *testing.T) {
	tempDir := t.TempDir()
	colDir := filepath.Join(tempDir, "users")
	if err := os.MkdirAll(colDir, 0o755); err != nil {
		t.Fatalf("failed to create collection dir: %v", err)
	}

	schema := NewSchemaDefinition(map[string]*CollectionDef{
		"users": {
			StoreRecordsAs: StoreCollectionRecordsIndividualFiles,
			RecordFormat:   RecordFormatJSON,
		},
	})
	db := database{
		dirPath:          tempDir,
		SchemaDefinition: schema,
	}
	ctx := context.Background()

	// 1. Valid record
	u1File := filepath.Join(colDir, "u1.json")
	if err := os.WriteFile(u1File, []byte(`{"name":"Alice","age":30}`), 0o600); err != nil {
		t.Fatalf("failed to write u1: %v", err)
	}

	var data userDTO
	rec1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u1"), &data)
	if err := db.Get(ctx, rec1); err != nil {
		t.Fatalf("unexpected Get error: %v", err)
	}
	if data.Name != "Alice" || data.Age != 30 {
		t.Errorf("expected Alice 30, got %+v", data)
	}
	exists, err := db.Exists(ctx, rec1.Key())
	if err != nil || !exists {
		t.Errorf("expected exists=true, got %v, err=%v", exists, err)
	}

	// 2. Missing record
	var data2 userDTO
	rec2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "u2"), &data2)
	if err := db.Get(ctx, rec2); err != nil {
		t.Fatalf("expected nil error on missing record, got %v", err)
	}
	if rec2.Exists() {
		t.Errorf("expected record.Exists() to be false")
	}
	exists, err = db.Exists(ctx, rec2.Key())
	if err != nil || exists {
		t.Errorf("expected exists=false, got %v, err=%v", exists, err)
	}

	// 3. Invalid JSON file
	badFile := filepath.Join(colDir, "bad.json")
	if err := os.WriteFile(badFile, []byte(`{invalid`), 0o600); err != nil {
		t.Fatalf("failed to write bad file: %v", err)
	}
	var dataBad userDTO
	recBad := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "bad"), &dataBad)
	if err := db.Get(ctx, recBad); err == nil {
		t.Fatal("expected error on invalid JSON")
	}

	// 4. File read error (directory named dir.json)
	dirFile := filepath.Join(colDir, "dir.json")
	if err := os.Mkdir(dirFile, 0o755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	var dataDir userDTO
	recDir := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("users", "dir"), &dataDir)
	if err := db.Get(ctx, recDir); err == nil {
		t.Fatal("expected error when reading a directory as a file")
	}

	// 5. Stat error on Exists (e.g. parent collection path component is a regular file)
	dbBad := database{
		dirPath:          badFile,
		SchemaDefinition: schema,
	}
	if _, err := dbBad.Exists(ctx, rec1.Key()); err == nil {
		t.Fatal("expected error from Exists when dir is a file")
	}
}

func TestDatabase_SingleFile(t *testing.T) {
	tempDir := t.TempDir()
	colDir := filepath.Join(tempDir, "posts")
	if err := os.MkdirAll(colDir, 0o755); err != nil {
		t.Fatalf("failed to create collection dir: %v", err)
	}

	schema := NewSchemaDefinition(map[string]*CollectionDef{
		"posts": {
			StoreRecordsAs: StoreCollectionRecordsInSingleFile,
			RecordFormat:   RecordFormatJSON,
		},
	})
	db := database{
		dirPath:          tempDir,
		SchemaDefinition: schema,
	}
	ctx := context.Background()

	// 1. Missing records.json
	recKey := dalrecord.NewKeyWithID("posts", "p1")
	var data userDTO
	rec := dalrecord.NewRecordWithData(recKey, &data)
	if err := db.Get(ctx, rec); err != nil {
		t.Fatalf("expected nil error on missing single file, got: %v", err)
	}
	if rec.Exists() {
		t.Errorf("expected not found (Exists() == false)")
	}
	exists, err := db.Exists(ctx, recKey)
	if err != nil || exists {
		t.Errorf("expected exists=false, got %v, err=%v", exists, err)
	}

	// 2. Open error (records.json is a directory)
	recordsPath := filepath.Join(colDir, "records.json")
	if err := os.Mkdir(recordsPath, 0o755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	if err := db.Get(ctx, rec); err == nil {
		t.Fatal("expected error when records.json is a directory")
	}
	if _, err := db.Exists(ctx, recKey); err == nil {
		t.Fatal("expected Exists error when records.json is a directory")
	}
	_ = os.Remove(recordsPath)

	// 3. Empty file (EOF on token)
	if err := os.WriteFile(recordsPath, []byte(""), 0o600); err != nil {
		t.Fatalf("failed to write empty file: %v", err)
	}
	if err := db.Get(ctx, rec); err != nil {
		t.Fatalf("expected nil error on EOF, got: %v", err)
	}
	if rec.Exists() {
		t.Errorf("expected not found on EOF")
	}
	exists, err = db.Exists(ctx, recKey)
	if err != nil || exists {
		t.Errorf("expected exists=false on EOF, got %v, err=%v", exists, err)
	}

	// 4. Invalid JSON token (not an array, e.g. an object)
	if err := os.WriteFile(recordsPath, []byte(`{"not":"array"}`), 0o600); err != nil {
		t.Fatalf("failed to write object JSON: %v", err)
	}
	if err := db.Get(ctx, rec); err == nil {
		t.Fatal("expected error when records.json is not an array")
	}
	if _, err := db.Exists(ctx, recKey); err == nil {
		t.Fatal("expected Exists error when records.json is not an array")
	}

	// 5. Valid JSON array with entries
	validJSON := `[
		{"id":"p1","data":{"name":"Post One","age":1}},
		{"id":"p2","data":{"name":"Post Two","age":2}},
		{"id":"nodata"},
		{"id":"bad_data","data":"not a valid struct"}
	]`
	if err := os.WriteFile(recordsPath, []byte(validJSON), 0o600); err != nil {
		t.Fatalf("failed to write valid JSON: %v", err)
	}

	// 5a. Match with data
	var p1Data userDTO
	recP1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("posts", "p1"), &p1Data)
	if err := db.Get(ctx, recP1); err != nil {
		t.Fatalf("unexpected Get error: %v", err)
	}
	if p1Data.Name != "Post One" || p1Data.Age != 1 {
		t.Errorf("expected Post One, got %+v", p1Data)
	}
	exists, err = db.Exists(ctx, recP1.Key())
	if err != nil || !exists {
		t.Errorf("expected exists=true for p1, got %v, err=%v", exists, err)
	}

	// 5b. Match without data field
	var noData userDTO
	recNoData := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("posts", "nodata"), &noData)
	if err := db.Get(ctx, recNoData); err == nil {
		t.Fatal("expected error when record has no data field")
	}
	exists, err = db.Exists(ctx, recNoData.Key())
	if err != nil || !exists {
		t.Errorf("expected exists=true for nodata, got %v, err=%v", exists, err)
	}

	// 5c. Match with invalid data field
	var badData userDTO
	recBadData := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("posts", "bad_data"), &badData)
	if err := db.Get(ctx, recBadData); err == nil {
		t.Fatal("expected unmarshal error on bad data")
	}

	// 5d. Non-matching ID
	var missingData userDTO
	recMissing := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("posts", "p_missing"), &missingData)
	if err := db.Get(ctx, recMissing); err != nil {
		t.Fatalf("expected nil error on missing record, got: %v", err)
	}
	if recMissing.Exists() {
		t.Errorf("expected not found for missing")
	}
	exists, err = db.Exists(ctx, recMissing.Key())
	if err != nil || exists {
		t.Errorf("expected exists=false for missing, got %v, err=%v", exists, err)
	}

	// 6. Malformed JSON object in array
	malformedJSON := `[{"id":"ok"}, not-json-element]`
	if err := os.WriteFile(recordsPath, []byte(malformedJSON), 0o600); err != nil {
		t.Fatalf("failed to write malformed JSON: %v", err)
	}
	if err := db.Get(ctx, recMissing); err == nil {
		t.Fatal("expected decode error on malformed array element")
	}
	if _, err := db.Exists(ctx, recMissing.Key()); err == nil {
		t.Fatal("expected Exists decode error on malformed array element")
	}

	// 7. Open error when path component is a regular file (ENOTDIR, not NotExist)
	dummyFile := filepath.Join(tempDir, "file_not_dir")
	if err := os.WriteFile(dummyFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("failed to write dummy file: %v", err)
	}
	dbBadSingle := database{
		dirPath:          dummyFile,
		SchemaDefinition: schema,
	}
	if err := dbBadSingle.Get(ctx, rec); err == nil {
		t.Fatal("expected Get error when path is invalid")
	}
	if _, err := dbBadSingle.Exists(ctx, recKey); err == nil {
		t.Fatal("expected Exists error when path is invalid")
	}
}

func TestDatabase_UnsupportedStorageAndGetMulti(t *testing.T) {
	tempDir := t.TempDir()
	schema := NewSchemaDefinition(map[string]*CollectionDef{
		"tape": {
			StoreRecordsAs: "magnetic_tape",
			RecordFormat:   RecordFormatJSON,
		},
		"files": {
			StoreRecordsAs: StoreCollectionRecordsIndividualFiles,
			RecordFormat:   RecordFormatJSON,
		},
	})
	db := database{
		dirPath:          tempDir,
		SchemaDefinition: schema,
	}
	ctx := context.Background()

	// Unsupported StoreRecordsAs
	recTape := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("tape", "t1"), &userDTO{})
	if err := db.Get(ctx, recTape); err == nil {
		t.Fatal("expected error on unsupported StoreRecordsAs in Get")
	}
	if _, err := db.Exists(ctx, recTape.Key()); err == nil {
		t.Fatal("expected error on unsupported StoreRecordsAs in Exists")
	}

	// GetMulti
	fDir := filepath.Join(tempDir, "files")
	_ = os.MkdirAll(fDir, 0o755)
	_ = os.WriteFile(filepath.Join(fDir, "f1.json"), []byte(`{"name":"F1"}`), 0o600)
	_ = os.WriteFile(filepath.Join(fDir, "f2.json"), []byte(`{"name":"F2"}`), 0o600)

	var d1, d2 userDTO
	r1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("files", "f1"), &d1)
	r2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("files", "f2"), &d2)

	if err := db.GetMulti(ctx, []dalrecord.Record{r1, r2}); err != nil {
		t.Fatalf("unexpected error on GetMulti: %v", err)
	}

	// GetMulti with an error
	rErr := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("tape", "t1"), &userDTO{})
	if err := db.GetMulti(ctx, []dalrecord.Record{r1, rErr}); err == nil {
		t.Fatal("expected error on GetMulti with failing record")
	}
}
