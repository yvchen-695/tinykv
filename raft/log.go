// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, _ := storage.LastIndex()
	firstIndex, _ := storage.FirstIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState()

	snapshot, _ := storage.Snapshot()
	log := &RaftLog{
		storage:         storage,
		stabled:         lastIndex,
		applied:         firstIndex - 1,
		entries:         entries,
		pendingSnapshot: &snapshot,
		firstIndex:      firstIndex,
		committed:       hardState.Commit,
	}
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) <= 0 {
		return nil
	}
	if l.stabled-l.FirstIndex()+1 < 0 || l.stabled-l.FirstIndex()+1 > uint64(len(l.entries)) {
		return nil
	}

	ents, _ := l.Entries(l.stabled+1, l.LastIndex()+1)
	//DPrintf("stabled-%d last-%d", l.stabled, l.LastIndex())

	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) <= 0 {
		return nil
	}
	if l.applied+1 == l.committed {
		return []pb.Entry{l.entries[l.committed-l.FirstIndex()]}
	}
	ents, _ = l.Entries(l.applied+1, l.committed+1)
	return ents
}

func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {
	if lo >= hi {
		return []pb.Entry{}, nil
	}
	var entries []pb.Entry
	for i := l.toSliceIndex(lo); i < len(l.entries) && i < l.toSliceIndex(hi); i++ {
		entries = append(entries, l.entries[i])
	}

	return entries, nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.FirstIndex() && int(i-l.FirstIndex()) < len(l.entries) {
		return l.entries[i-l.FirstIndex()].Term, nil
	}
	return l.storage.Term(i)
}

func (l *RaftLog) FirstIndex() uint64 {
	//return l.firstIndex
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i
	}
	return l.entries[0].Index
}

func (l *RaftLog) toSliceIndex(i uint64) int {
	idx := int(i - l.FirstIndex())
	if idx < 0 {
		panic("toSliceIndex: index < 0")
	}
	return idx
}

func (l *RaftLog) toLogIndex(i int) uint64 {
	return uint64(i) + l.FirstIndex()
}
