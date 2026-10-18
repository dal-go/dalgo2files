package dalgo2files

import (
	"testing"
)

func TestAdapter(t *testing.T) {
	a := adapter{}
	if got := a.Name(); got != DalgoProviderID {
		t.Errorf("expected %q, got %q", DalgoProviderID, got)
	}
	if got := a.Version(); got != "0.0.1" {
		t.Errorf("expected 0.0.1, got %q", got)
	}
}
