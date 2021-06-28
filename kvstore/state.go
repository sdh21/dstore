package kvstore

import (
	"github.com/sdh21/dstore/utils"
	"hash/fnv"
	"log"
	"sync"
)

type DBState struct {
	Collections           map[string]*CollectionState
	mu                    sync.RWMutex
	LastAppliedProposalId int64

	SavedTransactionResults map[int64]*TransactionResult
}

type CollectionState struct {
	Data *MVCCStore
	mu   sync.Mutex

	dirty                               bool
	lastIncrementalCheckPointProposalId int64
}

func NewDBState() *DBState {
	return &DBState{
		Collections: make(map[string]*CollectionState),
	}
}

func newCollectionState() *CollectionState {
	return &CollectionState{
		Data:                                NewMVCCStore(),
		dirty:                               false,
		lastIncrementalCheckPointProposalId: 0,
	}
}

func (db *KeyValueDB) applyTransaction(state *DBState, transaction *Transaction) *TransactionResult {
	if !db.discardResults {
		result, found := state.SavedTransactionResults[transaction.TxnTimestamp]
		if found {
			return result
		}
	}

	result := &TransactionResult{}
	result.Values = map[string]*AliasValue{}
	result.Status = TransactionResult_OK

	type intentDocument struct {
		value *AnyValue
		cell  *DocCell
	}

	type intentCollection struct {
		intentDocuments map[string]*intentDocument
	}

	intentCollections := map[string]*intentCollection{}

	for _, op := range transaction.Ops {
		if op.CollectionId == "" {
			result.Status = TransactionResult_CollectionNotFound
			return result
		}
		state.mu.RLock()
		collection, found := state.Collections[op.CollectionId]
		state.mu.RUnlock()
		if op.Type == OpType_CreateCollection {
			if found && op.CreateOption.ErrIfExists {
				result.Status = TransactionResult_CollectionAlreadyExists
				return result
			} else if !found {
				collection = newCollectionState()
				state.mu.Lock()
				state.Collections[op.CollectionId] = collection
				state.mu.Unlock()
			}
			continue
		} else if !found {
			result.Status = TransactionResult_CollectionNotFound
			return result
		}

		var docCell *DocCell
		docCell, found = collection.Data.GetDoc(op.DocumentId)

		if op.Type == OpType_CreateDocument {
			if found && op.CreateOption.ErrIfExists {
				result.Status = TransactionResult_DocumentAlreadyExists
				return result
			} else if !found {
				docCell, _ = collection.Data.CreateDoc(op.DocumentId)
			}
			continue
		} else if !found {
			result.Status = TransactionResult_DocumentNotFound
			return result
		}

		var value *AnyValue = nil
		var intentCol *intentCollection = nil
		var intentDoc *intentDocument = nil
		// Find in intent first, we may already have a clone
		intentCol, found = intentCollections[op.CollectionId]
		if !found {
			intentCol = &intentCollection{map[string]*intentDocument{}}
			intentCollections[op.CollectionId] = intentCol
		}
		intentDoc, found = intentCol.intentDocuments[op.DocumentId]
		if found {
			value = intentDoc.value
		} else {
			// Now we get a clone of AnyValue from MVCC
			dataCell, ok := docCell.Read(transaction.TxnTimestamp)
			if !ok {
				result.Status = TransactionResult_Aborted
				return result
			}
			if dataCell == nil {
				value = &AnyValue{Type: ValueType_HashMap, Map: map[string]*AnyValue{}}
			} else {
				if dataCell.ts == transaction.TxnTimestamp {
					log.Fatalf("txn timestamp duplicate!")
				}
				value = dataCell.data.(*AnyValue).Clone()
				value.Version = transaction.TxnTimestamp
			}
			// store the clone in intent
			intentCol.intentDocuments[op.DocumentId] = &intentDocument{
				value: value,
				cell:  docCell,
			}
		}

		// ops in one transaction are applied one by one
		err := op.Apply(value, result)
		if err != nil || result.Status != TransactionResult_OK {
			return result
		}
	}

	// OK, now we can prepare for committing
	for _, col := range intentCollections {
		for _, doc := range col.intentDocuments {
			canCommit := doc.cell.PrepareWrite(transaction.TxnTimestamp, doc.value)
			if !canCommit {
				result.Status = TransactionResult_Aborted
				break
			}
		}
	}

	// abort if prepare fails
	if result.Status == TransactionResult_Aborted {
		for _, col := range intentCollections {
			for _, doc := range col.intentDocuments {
				doc.cell.AbortWrite(transaction.TxnTimestamp)
			}
		}
		return result
	}

	for _, col := range intentCollections {
		for _, doc := range col.intentDocuments {
			doc.cell.CommitWrite(transaction.TxnTimestamp)
		}
	}

	if result.Status == TransactionResult_OK {
		result.Message = "OK"
		result.TransactionId = transaction.TxnTimestamp

		if !db.discardResults {
			state.SavedTransactionResults[transaction.TxnTimestamp] = result
		}
	}

	return result
}

func (db *KeyValueDB) applyTransactions(state *DBState, args *BatchSubmitArgs, proposalId int64) *BatchSubmitReply {

	result := &BatchSubmitReply{
		OK:                 true,
		TransactionResults: make([]*TransactionResult, args.Count),
	}

	if args.Count != int64(len(args.Transactions)) {
		result.OK = false
		utils.Error("count != len")
		return result
	}

	// TODO: we need some sort of ordering mechanism to allow more concurrency and avoid aborting
	//  Currently, we rely on client to make sure there are no conflict transactions in a BatchSubmit.
	state.mu.Lock()
	defer state.mu.Unlock()
	state.LastAppliedProposalId = proposalId
	applyWaitGroup := &sync.WaitGroup{}
	for i := range args.Transactions {
		idx := i
		transaction := args.Transactions[idx]
		applyWaitGroup.Add(1)
		go func() {
			result.TransactionResults[idx] = db.applyTransaction(state, transaction)
			applyWaitGroup.Done()
		}()
	}
	applyWaitGroup.Wait()
	return result
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
