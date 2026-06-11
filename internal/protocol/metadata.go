package protocol

/*
 * Metadata (API key 3) - v0–v8 (legacy encoding, non flexible)
 *
 * Request field additions by version:
 *   v1+:  null topics array means "all topics" (v0 uses empty array)
 *   v4+:  allow_auto_topic_creation BOOL
 *   v8:   include_cluster_authorized_operations BOOL
 *   v8+:  include_topic_authorized_operations BOOL
 *
 * Response field additions by version:
 *   v1+:  rack NULLABLE_STRING per broker, is_internal BOOL per topic,
 *         controller_id INT32
 *   v2+:  cluster_id NULLABLE_STRING
 *   v3+:  throttle_time_ms INT32
 *   v5+:  offline_replicas ARRAY per partition
 *   v7+:  leader_epoch INT32 per partition
 *   v8+:  topic_authorized_operations INT32 per topic
 *   v8:   cluster_authorized_operations INT32 at top level
 */

const (
	// noAuthorizedOperations is the sentinel value Kafka uses when it does
	// not report authorisation information (INT32_MIN).
	noAuthorizedOperations int32 = -2147483648
)

type MetadataRequest struct {
	// AllTopics is true when the client requested metadata for all topics
	// (null array in v1+, or empty array in v0).
	AllTopics bool
	Topics    []string

	AllowAutoTopicCreation             bool // v4+
	IncludeClusterAuthorizedOperations bool // v8 only
	IncludeTopicAuthorizedOperations   bool // v8+
}

func DecodeMetadataRequest(body []byte, apiVersion int16) (*MetadataRequest, error) {
	d := NewDecoder(body)
	req := &MetadataRequest{AllowAutoTopicCreation: true}

	numTopics := d.ArrayLen()
	switch {
	case numTopics < 0:
		// null array in v1+: "all topics"
		req.AllTopics = true
	case numTopics == 0 && apiVersion == 0:
		// empty array in v0: "all topics"
		req.AllTopics = true
	default:
		req.Topics = make([]string, numTopics)
		for i := range req.Topics {
			req.Topics[i] = d.String()
		}
	}

	if apiVersion >= 4 {
		req.AllowAutoTopicCreation = d.Bool()
	}
	if apiVersion == 8 {
		req.IncludeClusterAuthorizedOperations = d.Bool()
	}
	if apiVersion >= 8 {
		req.IncludeTopicAuthorizedOperations = d.Bool()
	}

	if d.Error() != nil {
		return nil, d.Error()
	}
	return req, nil
}

type MetadataResponse struct {
	ThrottleTimeMs int32 // v3+
	Brokers        []MetadataBroker
	ClusterID      *string // v2+: nil → null
	ControllerID   int32   // v1+
	Topics         []MetadataTopicResponse
}

type MetadataBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   *string // v1+: nil → null
}

type MetadataTopicResponse struct {
	ErrorCode  int16
	Name       string
	IsInternal bool // v1+
	Partitions []MetadataPartitionResponse
}

type MetadataPartitionResponse struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32 // v7+
	ReplicaNodes    []int32
	ISRNodes        []int32
	OfflineReplicas []int32 // v5+
}

func EncodeMetadataResponse(resp *MetadataResponse, apiVersion int16) []byte {
	e := NewEncoder(512)

	if apiVersion >= 3 {
		e.PutInt32(resp.ThrottleTimeMs)
	}

	// brokers
	e.PutArrayLen(int32(len(resp.Brokers)))
	for _, b := range resp.Brokers {
		e.PutInt32(b.NodeID)
		e.PutString(b.Host)
		e.PutInt32(b.Port)
		if apiVersion >= 1 {
			e.PutNullableString(b.Rack)
		}
	}

	if apiVersion >= 2 {
		e.PutNullableString(resp.ClusterID)
	}
	if apiVersion >= 1 {
		e.PutInt32(resp.ControllerID)
	}

	// topics
	e.PutArrayLen(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.PutInt16(t.ErrorCode)
		e.PutString(t.Name)
		if apiVersion >= 1 {
			e.PutBool(t.IsInternal)
		}

		e.PutArrayLen(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.PutInt16(p.ErrorCode)
			e.PutInt32(p.PartitionIndex)
			e.PutInt32(p.LeaderID)
			if apiVersion >= 7 {
				e.PutInt32(p.LeaderEpoch)
			}

			e.PutArrayLen(int32(len(p.ReplicaNodes)))
			for _, r := range p.ReplicaNodes {
				e.PutInt32(r)
			}
			e.PutArrayLen(int32(len(p.ISRNodes)))
			for _, r := range p.ISRNodes {
				e.PutInt32(r)
			}
			if apiVersion >= 5 {
				e.PutArrayLen(int32(len(p.OfflineReplicas)))
				for _, r := range p.OfflineReplicas {
					e.PutInt32(r)
				}
			}
		}

		if apiVersion >= 8 {
			e.PutInt32(noAuthorizedOperations)
		}
	}

	if apiVersion == 8 {
		e.PutInt32(noAuthorizedOperations)
	}

	return e.Bytes()
}
