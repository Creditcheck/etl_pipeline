package postgres

import "etl/internal/storage"

func init() {
	// registers the multi-table backend factory
	storage.RegisterMulti("postgres", NewMulti)
}
