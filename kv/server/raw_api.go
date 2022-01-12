package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	rsp := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()

	if err != nil {
		rsp.Error = err.Error()
		return rsp, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		rsp.Error = err.Error()
		return rsp, err
	}
	if val == nil {
		rsp.NotFound = true
	}

	rsp.Value = val

	return rsp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	rsp := &kvrpcpb.RawPutResponse{}

	modify := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}

	err := server.storage.Write(req.Context, modify)
	if err != nil {
		rsp.Error = err.Error()
	}

	return rsp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	rsp := &kvrpcpb.RawDeleteResponse{}

	modifies := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}

	err := server.storage.Write(req.Context, modifies)
	if err != nil {
		rsp.Error = err.Error()
	}

	return rsp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rsp := &kvrpcpb.RawScanResponse{}

	if req.Limit == 0 {
		return rsp, nil
	}

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		rsp.Error = err.Error()
		return rsp, err
	}

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	var kvPairs []*kvrpcpb.KvPair
	n := req.Limit
	for iter.Seek(req.StartKey); iter.Valid() && n > 0; iter.Next() {
		item := iter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			rsp.Error = err.Error()
			return rsp, err
		}
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
		n--
	}

	rsp.Kvs = kvPairs

	return rsp, nil
}
