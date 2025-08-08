package dalgo2files

import "github.com/dal-go/dalgo/dal"

var _ dal.Adapter = (*adapter)(nil)

type adapter struct {
}

func (a adapter) Name() string {
	return DalgoProviderID
}

func (a adapter) Version() string {
	return "0.0.1"
}
