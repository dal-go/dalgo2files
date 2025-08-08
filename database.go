package dalgo2files

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"os"
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

func (d database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	//TODO implement me
	panic("implement me")
}

func (d database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	//TODO implement me
	panic("implement me")
}

func (d database) Get(ctx context.Context, record dal.Record) error {
	//TODO implement me
	panic("implement me")
}

func (d database) Exists(ctx context.Context, key *dal.Key) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (d database) GetMulti(ctx context.Context, records []dal.Record) error {
	//TODO implement me
	panic("implement me")
}

func (d database) QueryReader(ctx context.Context, query dal.Query) (dal.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (d database) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	//TODO implement me
	panic("implement me")
}
