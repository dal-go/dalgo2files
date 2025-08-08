package dalgo2files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"io"
	"os"
	"path/filepath"
)

func NewDB(dirPath string, schemaDefinition SchemaDefinition) (db dal.DB, err error) {
	if err = schemaDefinition.Validate(); err != nil {
		return
	}
	// Check if directory exists
	if stat, err := os.Stat(dirPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("directory does not exist: %s", dirPath)
		}
		return nil, fmt.Errorf("failed to check directory: %w", err)
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", dirPath)
	}
	db = database{
		dirPath:          dirPath,
		SchemaDefinition: schemaDefinition,
	}
	return
}

type database struct {
	dirPath string
	SchemaDefinition
}

func (d database) ID() string {
	return d.dirPath
}

func (d database) Adapter() dal.Adapter {
	return adapter{}
}

// RunReadonlyTransaction: for file-based read operations, we don't support real transactions.
// For now, return ErrNotSupported to indicate transactions aren't supported.
func (d database) RunReadonlyTransaction(_ context.Context, _ dal.ROTxWorker, _ ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}

// RunReadwriteTransaction: writing is not implemented yet for this provider.
func (d database) RunReadwriteTransaction(_ context.Context, _ dal.RWTxWorker, _ ...dal.TransactionOption) error {
	return dal.ErrNotImplementedYet
}

// collectionDir returns absolute path to collection directory
func (d database) collectionDir(key *dal.Key) string {
	return filepath.Join(d.dirPath, key.Collection())
}

// singleFilePath returns path to single-file storage for a collection
func (d database) singleFilePath(key *dal.Key) string {
	return filepath.Join(d.collectionDir(key), "records.json")
}

// individualFilePath returns path to a single-record file storage
func (d database) individualFilePath(key *dal.Key) string {
	filename := fmt.Sprintf("%v.json", key.ID)
	return filepath.Join(d.collectionDir(key), filename)
}

// Get loads a record by key according to schema definitions.
func (d database) Get(_ context.Context, record dal.Record) error {
	key := record.Key()
	collectionDef := d.SchemaDefinition.GetCollectionDef(key)
	// Ensure collection directory exists for read path (no creation). If not exists -> not found
	switch collectionDef.StoreRecordsAs {
	case StoreCollectionRecordsIndividualFiles:
		path := d.individualFilePath(key)
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				record.SetError(dal.NewErrNotFoundByKey(key, nil))
				return nil
			}
			return record.SetError(err).Error()
		}
		// Allow accessing target data by marking no error temporarily
		record.SetError(dal.ErrNoError)
		target := record.Data()
		if err = json.Unmarshal(data, target); err != nil {
			return record.SetError(err).Error()
		}
		return nil
	case StoreCollectionRecordsInSingleFile:
		path := d.singleFilePath(key)
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				record.SetError(dal.NewErrNotFoundByKey(key, nil))
				return nil
			}
			return record.SetError(err).Error()
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		// Expect an array of objects: [{"id": <id>, "data": <record>}, ...]
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				record.SetError(dal.NewErrNotFoundByKey(key, nil))
				return nil
			}
			return record.SetError(err).Error()
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '[' {
			return record.SetError(fmt.Errorf("invalid JSON format for single-file collection: expected array")).Error()
		}
		idStr := fmt.Sprintf("%v", key.ID)
		for dec.More() {
			var wrapper map[string]json.RawMessage
			if err = dec.Decode(&wrapper); err != nil {
				return record.SetError(err).Error()
			}
			var wid string
			if v, ok := wrapper["id"]; ok {
				_ = json.Unmarshal(v, &wid)
			}
			if wid == idStr {
				// Found, unmarshal data
				record.SetError(dal.ErrNoError)
				target := record.Data()
				if v, ok := wrapper["data"]; ok {
					if err = json.Unmarshal(v, target); err != nil {
						return record.SetError(err).Error()
					}
					return nil
				}
				// If no "data" field, try unmarshal entire wrapper into target
				if err = json.Unmarshal(wrapper["data"], target); err != nil {
					return record.SetError(fmt.Errorf("record data not found in single-file entry for id=%s", idStr)).Error()
				}
				return nil
			}
		}
		// not found
		record.SetError(dal.NewErrNotFoundByKey(key, nil))
		return nil
	default:
		return record.SetError(fmt.Errorf("unsupported StoreRecordsAs: %s", collectionDef.StoreRecordsAs)).Error()
	}
}

func (d database) Exists(_ context.Context, key *dal.Key) (bool, error) {
	collectionDef := d.SchemaDefinition.GetCollectionDef(key)
	switch collectionDef.StoreRecordsAs {
	case StoreCollectionRecordsIndividualFiles:
		if _, err := os.Stat(d.individualFilePath(key)); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	case StoreCollectionRecordsInSingleFile:
		path := d.singleFilePath(key)
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		// Expect array as above
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, err
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '[' {
			return false, fmt.Errorf("invalid JSON format for single-file collection: expected array")
		}
		idStr := fmt.Sprintf("%v", key.ID)
		for dec.More() {
			var wrapper map[string]json.RawMessage
			if err = dec.Decode(&wrapper); err != nil {
				return false, err
			}
			var wid string
			if v, ok := wrapper["id"]; ok {
				_ = json.Unmarshal(v, &wid)
				if wid == idStr {
					return true, nil
				}
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported StoreRecordsAs: %s", collectionDef.StoreRecordsAs)
	}
}

func (d database) GetMulti(ctx context.Context, records []dal.Record) error {
	var firstErr error
	for _, r := range records {
		if err := d.Get(ctx, r); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (d database) QueryReader(_ context.Context, _ dal.Query) (dal.Reader, error) {
	return nil, dal.ErrNotImplementedYet
}

func (d database) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	// Use helper to keep consistent behavior when QueryReader is implemented later.
	return dal.NewQueryExecutor(d.QueryReader).QueryAllRecords(ctx, query)
}
