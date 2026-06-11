package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

type Handler struct {
	topics   *TopicManager
	brokerID int32
	host     string
	port     int32
	logger   *slog.Logger
}

func NewHandler(topics *TopicManager, brokerID int32, host string, port int32, logger *slog.Logger) *Handler {
	return &Handler{
		topics:   topics,
		brokerID: brokerID,
		host:     host,
		port:     port,
		logger:   logger,
	}
}

func (h *Handler) Handle(header protocol.RequestHeader, body []byte) ([]byte, error) {
	switch header.APIKey {
	case protocol.APIKeyApiVersions:
		return h.handleApiVersions(header)
	case protocol.APIKeyProduce:
		return h.handleProduce(header, body)
	case protocol.APIKeyFetch:
		return h.handleFetch(header, body)
	case protocol.APIKeyListOffsets:
		return h.handleListOffsets(header, body)
	case protocol.APIKeyMetadata:
		return h.handleMetadata(header, body)
	default:
		return nil, fmt.Errorf("unsupported API key %d", header.APIKey)
	}
}

func (h *Handler) handleApiVersions(header protocol.RequestHeader) ([]byte, error) {
	switch header.APIVersion {
	case 0, 1, 2:
		return protocol.EncodeApiVersionsResponseV0(0), nil
	case 3:
		return protocol.EncodeApiVersionsResponseV3(0), nil
	default:
		// dont close conn, let client retry with lower version
		return protocol.EncodeApiVersionsResponseV0(protocol.ErrCodeUnsupportedVersion), nil
	}
}

func (h *Handler) handleProduce(header protocol.RequestHeader, body []byte) ([]byte, error) {
	v := header.APIVersion

	// v0 - v2 not actually supported, dont end conn
	if v < 3 || v > 8 {
		return h.encodeUnsupportedVersionError(header), nil
	}

	req, err := protocol.DecodeProduceRequest(body, v)
	if err != nil {
		return nil, fmt.Errorf("decode produce v%d: %w", v, err)
	}

	acks := Acks(req.Acks)

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	if acks == AcksAll && req.TimeoutMs > 0 {
		ctx, cancel = context.WithTimeout(context.Background(),
			time.Duration(req.TimeoutMs)*time.Millisecond)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	type pendingResult struct {
		topicIdx int
		partIdx  int
		result   AppendResult
	}
	var pending []pendingResult

	resp := &protocol.ProduceResponse{
		Topics: make([]protocol.ProduceResponseTopic, len(req.Topics)),
	}

	for ti, reqTopic := range req.Topics {
		respTopic := &resp.Topics[ti]
		respTopic.Name = reqTopic.Name
		respTopic.Partitions = make([]protocol.ProduceResponsePartition, len(reqTopic.Partitions))

		for pi, reqPart := range reqTopic.Partitions {
			respPart := &respTopic.Partitions[pi]
			respPart.Index = reqPart.Index
			respPart.BaseOffset = -1
			respPart.LogAppendTime = -1
			respPart.LogStartOffset = -1

			partition, errCode := h.topics.GetPartition(reqTopic.Name, reqPart.Index)
			if errCode != ErrNone {
				respPart.ErrorCode = int16(errCode)
				continue
			}

			if len(reqPart.Records) == 0 {
				continue
			}

			result, err := partition.AppendRaw(ctx, reqPart.Records, acks)
			if err != nil {
				h.logger.Warn("append failed",
					"topic", reqTopic.Name,
					"partition", reqPart.Index,
					"err", err)
				respPart.ErrorCode = int16(ErrStorageError)
				continue
			}

			respPart.BaseOffset = int64(result.FirstOffset)
			respPart.LogStartOffset = int64(partition.log.OldestOffset())

			if acks == AcksAll {
				pending = append(pending, pendingResult{
					topicIdx: ti,
					partIdx:  pi,
					result:   result,
				})
			}
		}
	}

	// acks=0
	if acks == AcksNone {
		return nil, nil
	}

	// acks=-1 waiting for ISRs
	for _, p := range pending {
		<-p.result.Done
		if errFn := p.result.ErrFn(); errFn != nil {
			resp.Topics[p.topicIdx].Partitions[p.partIdx].ErrorCode = int16(*errFn)
		}
	}

	return protocol.EncodeProduceResponse(resp, v), nil
}

func (h *Handler) handleFetch(header protocol.RequestHeader, body []byte) ([]byte, error) {
	v := header.APIVersion
	if v < 4 || v > 11 {
		return h.encodeUnsupportedVersionError(header), nil
	}

	req, err := protocol.DecodeFetchRequest(body, v)
	if err != nil {
		return nil, fmt.Errorf("decode fetch v%d: %w", v, err)
	}

	maxBytes := req.MaxBytes

	resp := &protocol.FetchResponse{
		SessionID: 0,
		Topics:    make([]protocol.FetchResponseTopic, len(req.Topics)),
	}

	for ti, reqTopic := range req.Topics {
		respTopic := &resp.Topics[ti]
		respTopic.Name = reqTopic.Name
		respTopic.Partitions = make([]protocol.FetchResponsePartition, len(reqTopic.Partitions))

		for pi, reqPart := range reqTopic.Partitions {
			respPart := &respTopic.Partitions[pi]
			respPart.Index = reqPart.Index
			respPart.LastStableOffset = -1
			respPart.LogStartOffset = -1
			respPart.PreferredReadReplica = -1

			partition, errCode := h.topics.GetPartition(reqTopic.Name, reqPart.Index)
			if errCode != ErrNone {
				respPart.ErrorCode = int16(errCode)
				continue
			}

			partMax := min(maxBytes, reqPart.MaxBytes)

			result, err := partition.FetchRaw(
				uint64(reqPart.FetchOffset),
				req.ReplicaID,
				partMax,
			)
			if err != nil {
				respPart.ErrorCode = int16(ErrStorageError)
				h.logger.Warn("fetch failed",
					"topic", reqTopic.Name,
					"partition", reqPart.Index,
					"offset", reqPart.FetchOffset,
					"err", err)
				continue
			}

			respPart.HighWatermark = int64(result.HighWatermark)
			respPart.LastStableOffset = int64(result.HighWatermark) // non-transactional
			respPart.LogStartOffset = int64(result.LogStartOffset)
			respPart.Records = result.Records

		}
	}

	return protocol.EncodeFetchResponse(resp, v), nil
}

func (h *Handler) handleMetadata(header protocol.RequestHeader, body []byte) ([]byte, error) {
	v := header.APIVersion
	if v < 0 || v > 8 {
		return h.encodeUnsupportedVersionError(header), nil
	}

	req, err := protocol.DecodeMetadataRequest(body, v)
	if err != nil {
		return nil, fmt.Errorf("decode metadata v%d: %w", v, err)
	}

	resp := &protocol.MetadataResponse{
		Brokers: []protocol.MetadataBroker{
			{NodeID: h.brokerID, Host: h.host, Port: h.port},
		},
		ControllerID: h.brokerID, // TODO assuming single-node: we are always the controller
	}

	var topicNames []string
	if req.AllTopics {
		topicNames = h.topics.TopicNames()
	} else {
		topicNames = req.Topics
		if req.AllowAutoTopicCreation {
			for _, name := range topicNames {
				if _, errCode := h.topics.GetPartition(name, 0); errCode == ErrUnknownTopicOrPartition {
					if createErr := h.topics.CreateTopic(name, 1, true); createErr != nil {
						h.logger.Warn("auto-create topic failed", "topic", name, "err", createErr)
					}
				}
			}
		}
	}

	resp.Topics = make([]protocol.MetadataTopicResponse, len(topicNames))
	for i, name := range topicNames {
		topicResp := &resp.Topics[i]
		topicResp.Name = name

		count, errCode := h.topics.PartitionCount(name)
		if errCode != ErrNone {
			topicResp.ErrorCode = int16(errCode)
			continue
		}

		topicResp.Partitions = make([]protocol.MetadataPartitionResponse, count)
		for pid := 0; pid < count; pid++ {
			partResp := &topicResp.Partitions[pid]
			partResp.PartitionIndex = int32(pid)

			p, ec := h.topics.GetPartition(name, int32(pid))
			if ec != ErrNone {
				partResp.ErrorCode = int16(ec)
				partResp.LeaderID = -1
				continue
			}

			if p.IsLeader() {
				partResp.LeaderID = h.brokerID
			} else {
				partResp.LeaderID = -1
			}
			partResp.LeaderEpoch = int32(p.LeaderEpoch())

			isr := p.ISRSnapshot()
			partResp.ReplicaNodes = []int32{h.brokerID}
			partResp.ISRNodes = isr
		}
	}

	return protocol.EncodeMetadataResponse(resp, v), nil
}

// TODO header is redundant for now, will implement more fine grained error later
func (h *Handler) encodeUnsupportedVersionError(header protocol.RequestHeader) []byte {
	e := protocol.NewEncoder(2)
	e.PutInt16(protocol.ErrCodeUnsupportedVersion)
	return e.Bytes()
}

func (h *Handler) handleListOffsets(header protocol.RequestHeader, body []byte) ([]byte, error) {
	v := header.APIVersion
	if v < 1 || v > 5 {
		return h.encodeUnsupportedVersionError(header), nil
	}

	req, err := protocol.DecodeListOffsetsRequest(body, v)
	if err != nil {
		return nil, fmt.Errorf("decode list_offsets v%d: %w", v, err)
	}

	resp := &protocol.ListOffsetsResponse{
		Topics: make([]protocol.ListOffsetsResponseTopic, len(req.Topics)),
	}

	for ti, reqTopic := range req.Topics {
		respTopic := &resp.Topics[ti]
		respTopic.Name = reqTopic.Name
		respTopic.Partitions = make([]protocol.ListOffsetsResponsePartition, len(reqTopic.Partitions))

		for pi, reqPart := range reqTopic.Partitions {
			respPart := &respTopic.Partitions[pi]
			respPart.Index = reqPart.Index
			respPart.Timestamp = -1

			partition, errCode := h.topics.GetPartition(reqTopic.Name, reqPart.Index)
			if errCode != ErrNone {
				respPart.ErrorCode = int16(errCode)
				respPart.Offset = -1
				continue
			}
			respPart.LeaderEpoch = int32(partition.LeaderEpoch())

			switch reqPart.Timestamp {
			case -1: // latest
				respPart.Offset = int64(partition.LEO())
				respPart.Timestamp = -1
			case -2: // earliest
				respPart.Offset = int64(partition.log.OldestOffset())
				respPart.Timestamp = -1
			default:
				// TODO implement timestamp based lookup, stub for now
				respPart.Offset = -1
				respPart.Timestamp = -1
			}
		}
	}

	return protocol.EncodeListOffsetsResponse(resp, v), nil
}
