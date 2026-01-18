package mssql

import "etl/internal/storage"

func init() {
	storage.RegisterMulti("mssql", NewMulti)
}
