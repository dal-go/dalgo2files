package dalgo2files

import (
	"testing"
)

func TestCollectionDef_Validate(t *testing.T) {
	tests := []struct {
		name            string
		collectionDef   CollectionDef
		expectedError   string
		shouldHaveError bool
	}{
		{
			name: "valid collection def with single file storage and JSON format",
			collectionDef: CollectionDef{
				StoreRecordsAs: StoreCollectionRecordsInSingleFile,
				RecordFormat:   RecordFormatJSON,
			},
			expectedError:   "",
			shouldHaveError: false,
		},
		{
			name: "valid collection def with individual files storage and JSON format",
			collectionDef: CollectionDef{
				StoreRecordsAs: StoreCollectionRecordsIndividualFiles,
				RecordFormat:   RecordFormatJSON,
			},
			expectedError:   "",
			shouldHaveError: false,
		},
		{
			name: "empty StoreRecordsAs field",
			collectionDef: CollectionDef{
				StoreRecordsAs: "",
				RecordFormat:   RecordFormatJSON,
			},
			expectedError:   "must have StoreRecordsAs for a collection definition",
			shouldHaveError: true,
		},
		{
			name: "unknown StoreRecordsAs value",
			collectionDef: CollectionDef{
				StoreRecordsAs: "unknown_storage_type",
				RecordFormat:   RecordFormatJSON,
			},
			expectedError:   `unknown StoreRecordsAs: "unknown_storage_type"`,
			shouldHaveError: true,
		},
		{
			name: "empty RecordFormat field",
			collectionDef: CollectionDef{
				StoreRecordsAs: StoreCollectionRecordsInSingleFile,
				RecordFormat:   "",
			},
			expectedError:   "must have RecordFormat for a collection definition",
			shouldHaveError: true,
		},
		{
			name: "unknown RecordFormat value",
			collectionDef: CollectionDef{
				StoreRecordsAs: StoreCollectionRecordsInSingleFile,
				RecordFormat:   "unknown_format",
			},
			expectedError:   `unknown RecordFormat: "unknown_format"`,
			shouldHaveError: true,
		},
		{
			name: "both fields empty",
			collectionDef: CollectionDef{
				StoreRecordsAs: "",
				RecordFormat:   "",
			},
			expectedError:   "must have StoreRecordsAs for a collection definition",
			shouldHaveError: true,
		},
		{
			name: "both fields invalid",
			collectionDef: CollectionDef{
				StoreRecordsAs: "invalid_storage",
				RecordFormat:   "invalid_format",
			},
			expectedError:   `unknown StoreRecordsAs: "invalid_storage"`,
			shouldHaveError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.collectionDef.Validate()

			if tt.shouldHaveError {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if err.Error() != tt.expectedError {
					t.Errorf("expected error %q, got %q", tt.expectedError, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestCollectionDef_Validate_EdgeCases(t *testing.T) {
	t.Run("zero value CollectionDef", func(t *testing.T) {
		var cd CollectionDef
		err := cd.Validate()
		if err == nil {
			t.Error("expected error for zero value CollectionDef")
		}
		expectedError := "must have StoreRecordsAs for a collection definition"
		if err.Error() != expectedError {
			t.Errorf("expected error %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("whitespace only values", func(t *testing.T) {
		cd := CollectionDef{
			StoreRecordsAs: "   ",
			RecordFormat:   "   ",
		}
		err := cd.Validate()
		if err == nil {
			t.Error("expected error for whitespace-only values")
		}
		expectedError := `unknown StoreRecordsAs: "   "`
		if err.Error() != expectedError {
			t.Errorf("expected error %q, got %q", expectedError, err.Error())
		}
	})
}
