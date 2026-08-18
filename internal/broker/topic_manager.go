package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/log"
)

/*
 * TODO change fmt.printf to slog
 */

// partitions are immutable after creation for now, ordering is also local to partitions. (no global ordering)
type Topic struct {
	name       string
	partitions []*Partition
}

// sync.Map could also be used but i chose type safety, the performance of this path is not critical anyways
type TopicManager struct {
	dataDir       string
	localBrokerID int32
	logConfig     log.LogConfig

	mu     sync.RWMutex
	topics map[string]*Topic
}

func NewTopicManager(dataDir string, localBrokerID int32, logConfig log.LogConfig) (*TopicManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	tm := &TopicManager{
		dataDir:       dataDir,
		localBrokerID: localBrokerID,
		logConfig:     logConfig,
		topics:        make(map[string]*Topic),
	}
	if err := tm.recoverFromDisk(); err != nil {
		return nil, fmt.Errorf("recover topics: %w", err)
	}
	return tm, nil
}

// WARNING this assumes a single node and automatically promotes to leader
func (tm *TopicManager) recoverFromDisk() error {
	entries, err := os.ReadDir(tm.dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	type partitionEntry struct {
		id  int
		dir string
	}
	byTopic := make(map[string][]partitionEntry)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		idx := strings.LastIndex(name, "-")
		if idx < 0 {
			continue
		}
		topicName := name[:idx]
		if err := validateTopicName(topicName); err != nil {
			continue
		}

		partID, err := strconv.Atoi(name[idx+1:])
		if err != nil || partID < 0 {
			continue
		}
		byTopic[topicName] = append(byTopic[topicName], partitionEntry{id: partID, dir: name})
	}

	for topicName, dirs := range byTopic {
		maxID := 0
		for _, d := range dirs {
			if d.id > maxID {
				maxID = d.id
			}
		}

		// adding as a sanity check
		if maxID > 9999 {
			fmt.Printf("warn: skipping topic %q: suspiciously large partition ID %d\n",
				topicName, maxID)
			continue
		}

		partitions := make([]*Partition, maxID+1)
		for _, d := range dirs {
			partDir := filepath.Join(tm.dataDir, d.dir)
			l, err := log.NewLog(partDir, tm.logConfig)
			if err != nil {
				fmt.Printf("warn: partition %s-%d offline on recovery: %v\n",
					topicName, d.id, err)
				continue
			}
			p := NewPartition(topicName, int32(d.id), tm.localBrokerID, l)

			epoch, ok := l.LatestLeaderEpoch()
			if !ok {
				epoch = 1
			}

			if err := p.MakeLeader(
				epoch,
				[]int32{tm.localBrokerID},
				l.NextOffset(),
			); err != nil {
				fmt.Printf(
					"warn: partition %s-%d offline on recovery: MakeLeader: %v\n",
					topicName,
					d.id,
					err,
				)
				_ = p.Close()
				continue
			}

			partitions[d.id] = p
		}

		tm.topics[topicName] = &Topic{name: topicName, partitions: partitions}
	}

	return nil
}

func (tm *TopicManager) CreateTopic(name string, numPartitions int, initialLeader bool) error {
	if err := validateTopicName(name); err != nil {
		return err
	}
	if numPartitions <= 0 {
		return fmt.Errorf("numPartitions must be > 0, got %d", numPartitions)
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partDir := tm.partitionDir(name, i)
		l, err := log.NewLog(partDir, tm.logConfig)
		if err != nil {
			for j := 0; j < i; j++ {
				if partitions[j] != nil {
					_ = partitions[j].Close()
				}
				_ = os.RemoveAll(tm.partitionDir(name, j))
			}
			return fmt.Errorf("create log for %s-%d: %w", name, i, err)
		}
		p := NewPartition(name, int32(i), tm.localBrokerID, l)
		if initialLeader {
			if err := p.MakeLeader(1, []int32{tm.localBrokerID}, l.NextOffset()); err != nil {
				_ = p.Close()

				for j := 0; j < i; j++ {
					if partitions[j] != nil {
						_ = partitions[j].Close()
					}
					_ = os.RemoveAll(tm.partitionDir(name, j))
				}

				_ = os.RemoveAll(tm.partitionDir(name, i))

				return fmt.Errorf(
					"make leader for %s-%d: %w",
					name,
					i,
					err,
				)
			}
		}
		partitions[i] = p
	}

	tm.topics[name] = &Topic{name: name, partitions: partitions}
	return nil
}

func (tm *TopicManager) GetPartition(topic string, partitionID int32) (*Partition, ErrorCode) {
	tm.mu.RLock()
	t, ok := tm.topics[topic]
	tm.mu.RUnlock()

	if !ok {
		return nil, ErrUnknownTopicOrPartition
	}
	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return nil, ErrUnknownTopicOrPartition
	}
	p := t.partitions[partitionID]
	if p == nil {
		return nil, ErrStorageError
	}
	return p, ErrNone
}

func (tm *TopicManager) DeleteTopic(name string) error {
	tm.mu.Lock()
	t, ok := tm.topics[name]
	if !ok {
		tm.mu.Unlock()
		return fmt.Errorf("unknown topic %q", name)
	}
	delete(tm.topics, name)
	tm.mu.Unlock()

	var firstErr error
	for i, p := range t.partitions {
		if p == nil {
			continue
		}
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := os.RemoveAll(tm.partitionDir(name, i)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (tm *TopicManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var firstErr error
	for _, t := range tm.topics {
		for _, p := range t.partitions {
			if p == nil {
				continue
			}
			if err := p.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	tm.topics = make(map[string]*Topic)
	return firstErr
}

// sort for determinism
func (tm *TopicManager) TopicNames() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (tm *TopicManager) PartitionCount(topic string) (int, ErrorCode) {
	tm.mu.RLock()
	t, ok := tm.topics[topic]
	tm.mu.RUnlock()
	if !ok {
		return 0, ErrUnknownTopicOrPartition
	}
	return len(t.partitions), ErrNone
}

func (tm *TopicManager) partitionDir(topic string, partitionID int) string {
	return filepath.Join(tm.dataDir, fmt.Sprintf("%s-%d", topic, partitionID))
}

func validateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("topic name cannot be %q", name)
	}
	if len(name) > 249 {
		return fmt.Errorf("topic name too long: %d chars (max 249)", len(name))
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return fmt.Errorf("topic name %q contains invalid character %q", name, c)
		}
	}
	return nil
}
