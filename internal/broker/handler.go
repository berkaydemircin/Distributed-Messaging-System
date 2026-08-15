package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/server"
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

func (h *Handler) Handle(header protocol.RequestHeader, body []byte) (server.Response, error) {
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

func (h *Handler) handleApiVersions(header protocol.RequestHeader) (server.Response, error) {
	switch header.APIVersion {
	case 0, 1, 2:
		return server.BytesResponse(protocol.EncodeApiVersionsResponseV0(0)), nil
	case 3:
		return server.BytesResponse(protocol.EncodeApiVersionsResponseV3(0)), nil
	default:
		// dont close conn, let client retry with lower version
		return server.BytesResponse(protocol.EncodeApiVersionsResponseV0(protocol.ErrCodeUnsupportedVersion)), nil
	}
}

func (h *Handler) handleProduce(header protocol.RequestHeader, body []byte) (server.Response, error) {
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

	return server.BytesResponse(protocol.EncodeProduceResponse(resp, v)), nil
}

func (h *Handler) handleFetch(header protocol.RequestHeader, body []byte) (server.Response, error) {
	v := header.APIVersion
	if v < 4 || v > 11 {
		return h.encodeUnsupportedVersionError(header), nil
	}

	req, err := protocol.DecodeFetchRequest(body, v)
	if err != nil {
		return nil, fmt.Errorf("decode fetch v%d: %w", v, err)
	}

	maxBytes := req.MaxBytes

	resp := &fetchStreamResponse{
		sessionID: 0,
		topics:    make([]fetchStreamTopic, len(req.Topics)),
	}

	for ti, reqTopic := range req.Topics {
		respTopic := &resp.topics[ti]
		respTopic.name = reqTopic.Name
		respTopic.partitions = make([]fetchStreamPartition, len(reqTopic.Partitions))

		for pi, reqPart := range reqTopic.Partitions {
			respPart := &respTopic.partitions[pi]
			respPart.index = reqPart.Index
			respPart.lastStableOffset = -1
			respPart.logStartOffset = -1
			respPart.preferredReadReplica = -1

			partition, errCode := h.topics.GetPartition(reqTopic.Name, reqPart.Index)
			if errCode != ErrNone {
				respPart.errorCode = int16(errCode)
				respPart.recordsSize = 0
				continue
			}

			if reqPart.FetchOffset < 0 {
				respPart.errorCode = int16(ErrOffsetOutOfRange)
				respPart.recordsSize = 0
				continue
			}

			partMax := min(maxBytes, reqPart.MaxBytes)

			result, err := partition.FetchRawRanges(
				uint64(reqPart.FetchOffset),
				req.ReplicaID,
				partMax,
			)
			if err != nil {
				var ec ErrorCode
				if errors.As(err, &ec) {
					respPart.errorCode = int16(ec)
				} else {
					respPart.errorCode = int16(ErrStorageError)
				}
				respPart.recordsSize = 0
				h.logger.Warn("fetch failed",
					"topic", reqTopic.Name,
					"partition", reqPart.Index,
					"offset", reqPart.FetchOffset,
					"err", err)
				continue
			}

			respPart.highWatermark = int64(result.HighWatermark)
			respPart.lastStableOffset = int64(result.HighWatermark)
			respPart.logStartOffset = int64(result.LogStartOffset)
			respPart.recordsSize = result.RecordsSize
			respPart.ranges = result.Ranges
		}
	}

	return encodeFetchStreamResponse(resp, v), nil
}

func (h *Handler) handleMetadata(header protocol.RequestHeader, body []byte) (server.Response, error) {
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

	return server.BytesResponse(protocol.EncodeMetadataResponse(resp, v)), nil
}

// TODO header is redundant for now, will implement more fine grained error later
func (h *Handler) encodeUnsupportedVersionError(header protocol.RequestHeader) server.Response {
	e := protocol.NewEncoder(2)
	e.PutInt16(protocol.ErrCodeUnsupportedVersion)
	return server.BytesResponse(e.Bytes())
}

func (h *Handler) handleListOffsets(header protocol.RequestHeader, body []byte) (server.Response, error) {
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

	return server.BytesResponse(protocol.EncodeListOffsetsResponse(resp, v)), nil
}
