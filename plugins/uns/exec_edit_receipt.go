// Edit idempotency: the in-memory replay cache and the durable receipt
// that outlives it.
//
// A edit command is identified by its operation_id and fingerprinted by a
// canonical digest of its whole payload, so a client that retries gets the
// ORIGINAL outcome back rather than a second execution — and a client that
// reuses an operation_id for different content gets 409 rather than a silent
// overwrite. The cache is bounded and therefore lossy across a restart, which
// is why the receipt is also written into the same atomic batch as the state
// it describes: the durable copy is what makes the guarantee survive the
// process, and committing it with the state is what stops a receipt from ever
// claiming a write that did not land.

package uns

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

func (w *EditExec) remember(
	operationID string,
	digest [sha256.Size]byte,
	code int,
	message, result string,
	writes []StateWrite,
) (int, string, string, []StateWrite) {
	writes = cloneStateWrites(writes)
	w.replays[operationID] = editReplay{
		digest: digest, code: code, message: message, result: result, writes: writes,
	}
	w.replayOrder = append(w.replayOrder, operationID)
	if len(w.replayOrder) > editReplayLimit {
		oldest := w.replayOrder[0]
		w.replayOrder = w.replayOrder[1:]
		delete(w.replays, oldest)
	}
	return code, message, result, cloneStateWrites(writes)
}

func (w *EditExec) persistStandaloneReceipt(
	operationID string,
	digest [sha256.Size]byte,
	message, result string,
	writes []StateWrite,
) error {
	batch := make([]StateRecord, 0, 2)
	operationRecords := w.operationRecords()
	prune := len(operationRecords) - editReplayLimit + 1
	if prune > 0 {
		for _, record := range operationRecords[:prune] {
			batch = append(batch, StateRecord{Topic: record.Topic})
		}
	}
	topics := make([]string, len(writes))
	for index, write := range writes {
		topics[index] = write.Topic
	}
	receipt := editOperationReceipt{
		ID: operationID, Digest: fmt.Sprintf("%x", digest), Message: message,
		Result: result, Topics: topics, Writes: cloneStateWrites(writes),
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	batch = append(batch, StateRecord{
		Topic: editOperationTopic(w.store.NodeID(), operationID), Payload: payload,
	})
	_, err = w.store.PublishBatch(batch)
	return err
}

func cloneStateWrites(in []StateWrite) []StateWrite {
	return append([]StateWrite(nil), in...)
}

func canonicalJSONDigest(payload []byte) ([sha256.Size]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return [sha256.Size]byte{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return [sha256.Size]byte{}, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func validateOperationID(operationID string) error {
	if len(operationID) > 128 {
		return fmt.Errorf("operation_id is longer than 128 characters")
	}
	if strings.ContainsAny(operationID, "/+#") {
		return fmt.Errorf("operation_id must be one canonical path segment")
	}
	for _, char := range operationID {
		if char < 0x20 || char == 0x7f {
			return fmt.Errorf("operation_id contains a control character")
		}
	}
	return nil
}

func (w *EditExec) operationRecords() []KVRecord {
	records := w.store.KVScan("_EditOperation", w.store.NodeID())
	sort.Slice(records, func(i, j int) bool { return records[i].Offset < records[j].Offset })
	return records
}

func (w *EditExec) durableReplay(operationID string) (editReplay, bool, error) {
	wantTopic := editOperationTopic(w.store.NodeID(), operationID)
	for _, record := range w.operationRecords() {
		if record.Topic != wantTopic {
			continue
		}
		var receipt editOperationReceipt
		if err := json.Unmarshal(record.Payload, &receipt); err != nil {
			return editReplay{}, false, err
		}
		if receipt.ID != operationID || receipt.Digest == "" {
			return editReplay{}, false, fmt.Errorf("receipt identity or digest does not match its topic")
		}
		digestBytes, err := parseHexDigest(receipt.Digest)
		if err != nil {
			return editReplay{}, false, err
		}
		writes := cloneStateWrites(receipt.Writes)
		if len(writes) == 0 {
			if record.Offset <= uint64(len(receipt.Topics)) {
				return editReplay{}, false, fmt.Errorf("receipt offset cannot reconstruct its state writes")
			}
			first := record.Offset - uint64(len(receipt.Topics))
			writes = make([]StateWrite, len(receipt.Topics))
			for index, topic := range receipt.Topics {
				writes[index] = StateWrite{
					Stream: "entities", Offset: first + uint64(index), Topic: topic,
				}
			}
		}
		return editReplay{
			digest: digestBytes, code: 200, message: receipt.Message, result: receipt.Result, writes: writes,
		}, true, nil
	}
	return editReplay{}, false, nil
}

func parseHexDigest(value string) ([sha256.Size]byte, error) {
	if len(value) != sha256.Size*2 {
		return [sha256.Size]byte{}, fmt.Errorf("receipt digest has invalid length")
	}
	var digest [sha256.Size]byte
	for index := range digest {
		parsed, err := strconv.ParseUint(value[index*2:index*2+2], 16, 8)
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("receipt digest is not hexadecimal")
		}
		digest[index] = byte(parsed)
	}
	return digest, nil
}

func (w *EditExec) withDurableReceipt(
	operationID string,
	digest [sha256.Size]byte,
	message, result string,
	records []StateRecord,
) ([]StateRecord, int, error) {
	if len(records) == 0 {
		return nil, 0, fmt.Errorf("a successful operation must produce state")
	}
	batch := make([]StateRecord, 0, len(records)+2)
	operationRecords := w.operationRecords()
	prune := len(operationRecords) - editReplayLimit + 1
	if prune > 0 {
		for _, record := range operationRecords[:prune] {
			batch = append(batch, StateRecord{Topic: record.Topic})
		}
	}
	stateStart := len(batch)
	batch = append(batch, records...)
	topics := make([]string, len(records))
	for index, record := range records {
		topics[index] = record.Topic
	}
	receipt := editOperationReceipt{
		ID: operationID, Digest: fmt.Sprintf("%x", digest), Message: message,
		Result: result, Topics: topics,
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return nil, 0, err
	}
	batch = append(batch, StateRecord{
		Topic: editOperationTopic(w.store.NodeID(), operationID), Payload: payload,
	})
	return batch, stateStart, nil
}

func editOperationTopic(nodeID, operationID string) string {
	return Prefix() + "_EditOperation/" + nodeID + "/_colca/edit/operations/" + operationID
}

const editReplayLimit = 1024

type editOperationReceipt struct {
	ID      string       `json:"id"`
	Digest  string       `json:"digest"`
	Message string       `json:"message"`
	Result  string       `json:"result"`
	Topics  []string     `json:"topics"`
	Writes  []StateWrite `json:"writes,omitempty"`
}
