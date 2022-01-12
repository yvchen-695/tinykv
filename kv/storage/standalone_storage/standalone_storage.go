package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{
		engine_util.CreateDB(conf.DBPath, false),
	}

	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := StandAloneStorageReader{
		s.db.NewTransaction(false),
	}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	// start txn
	txn := s.db.NewTransaction(true)

	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			if err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
				return err
			}
		case storage.Delete:
			del := modify.Data.(storage.Delete)
			if err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key)); err != nil {
				return err
			}
		}
	}

	// commit txn
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}
