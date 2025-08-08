package dalgo2files

import (
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
)

type SchemaDefinition interface {
	GetCollectionDef(key *dal.Key) CollectionDef
	Validate() error
}

func NewSchemaDefinition(collections map[string]*CollectionDef) SchemaDefinition {
	return schemaDefinition{Collections: collections}
}

type schemaDefinition struct {
	Collections map[string]*CollectionDef
}

func (v schemaDefinition) GetCollectionDef(key *dal.Key) CollectionDef {
	collectionDef := v.Collections[key.Collection()]
	return *collectionDef
}
func (v schemaDefinition) Validate() error {
	for id, def := range v.Collections {
		if err := def.Validate(); err != nil {
			return fmt.Errorf("invalid definition for collection %s: %w", id, err)
		}
	}
	return nil
}

type ByDefault struct {
	StoreRecordsAs StoreCollectionRecordsAs
	RecordFormat   RecordFormat
}

type StoreCollectionRecordsAs string

const (
	StoreCollectionRecordsInSingleFile    StoreCollectionRecordsAs = "single_file"
	StoreCollectionRecordsIndividualFiles StoreCollectionRecordsAs = "individual_files"
)

type RecordFormat string

const RecordFormatJSON RecordFormat = "json"

type CollectionDef struct {
	StoreRecordsAs StoreCollectionRecordsAs
	RecordFormat   RecordFormat
}

func (v CollectionDef) Validate() error {
	switch v.StoreRecordsAs {
	case StoreCollectionRecordsInSingleFile, StoreCollectionRecordsIndividualFiles:
	// OK
	case "":
		return errors.New("must have StoreRecordsAs for a collection definition")
	default:
		return fmt.Errorf("unknown StoreRecordsAs: %q", v.StoreRecordsAs)
	}
	switch v.RecordFormat {
	case RecordFormatJSON: // OK
	case "":
		return errors.New("must have RecordFormat for a collection definition")
	default:
		return fmt.Errorf("unknown RecordFormat: %q", v.RecordFormat)
	}
	return nil
}
