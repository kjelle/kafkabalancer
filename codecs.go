package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	kazoo "github.com/wvanbergen/kazoo-go"
)

func GetPartitionListFromReader(in io.Reader, isJSON bool, tns TopicNames) (*PartitionList, error) {
	pl := &PartitionList{}

	if isJSON {
		dec := json.NewDecoder(in)
		err := dec.Decode(pl)
		if err != nil {
			return nil, fmt.Errorf("failed parsing json: %s", err)
		}
		if pl.Version != 1 {
			return nil, fmt.Errorf("wrong partition list version: expected 1, got %d", pl.Version)
		}
	} else {
		scanner := bufio.NewScanner(in)
		re := regexp.MustCompile("^\tTopic: ([^\t]*)\tPartition: ([0-9]*)\tLeader: ([0-9]*)\tReplicas: ([0-9,]*)\tIsr: ([0-9,]*)")
		for scanner.Scan() {
			m := re.FindStringSubmatch(scanner.Text())
			if m == nil {
				continue
			}

			if len(tns) > 0 && !tns.Contains(TopicName(m[1])) {
				continue
			}

			partition, _ := strconv.Atoi(m[2])
			strreplicas := strings.Split(m[4], ",")
			var replicas []BrokerID
			for _, strreplica := range strreplicas {
				replica, _ := strconv.Atoi(strreplica)
				replicas = append(replicas, BrokerID(replica))
			}
			pl.Partitions = append(pl.Partitions, Partition{
				Topic:     TopicName(m[1]),
				Partition: PartitionID(partition),
				Replicas:  replicas,
			})
		}

		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("failed reading file: %s", err)
		}
	}

	if len(pl.Partitions) == 0 {
		return nil, fmt.Errorf("empty partition list")
	}

	return pl, nil
}

// FilterPartitionList will ensure that a topic+partition only exists once
func FilterPartitionList(pl *PartitionList) *PartitionList {
	ppl := &PartitionList{Version: pl.Version}
	hpl := make(map[TopicName]map[PartitionID]bool)

	for _, p := range pl.Partitions {
		if _, ok := hpl[p.Topic]; !ok {
			hpl[p.Topic] = make(map[PartitionID]bool)
		}

		if !hpl[p.Topic][p.Partition] {
			hpl[p.Topic][p.Partition] = true
			ppl.Partitions = append(ppl.Partitions, p)
		}
	}
	return ppl
}

func WritePartitionList(out io.Writer, pl *PartitionList) error {
	enc := json.NewEncoder(out)
	pl.Version = 1
	err := enc.Encode(pl)
	if err != nil {
		return fmt.Errorf("failed serializing json: %s", err)
	}

	return nil
}

func GetPartitionListFromZookeeper(zkConnStr string, tns TopicNames) (*PartitionList, error) {
	zk, err := kazoo.NewKazooFromConnectionString(zkConnStr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed parsing zk connection string: %v", err)
	}
	defer zk.Close()

	pl := &PartitionList{}

	topics, err := zk.Topics()
	if err != nil {
		return nil, fmt.Errorf("failed reading topic list from zk: %v", err)
	}

	for _, topic := range topics {
		if len(tns) > 0 && !tns.Contains(TopicName(topic.Name)) {
			continue
		}

		partitions, err := topic.Partitions()
		if err != nil {
			return nil, fmt.Errorf("failed reading partition list for topic %s from zk: %v", topic.Name, err)
		}

		for _, partition := range partitions {
			replicas := make([]BrokerID, 0, len(partition.Replicas))
			for _, replica := range partition.Replicas {
				replicas = append(replicas, BrokerID(replica))
			}
			pl.Partitions = append(pl.Partitions, Partition{
				Topic:     TopicName(topic.Name),
				Partition: PartitionID(partition.ID),
				Replicas:  replicas,
				// NumConsumers: <number of consumer groups>,
				// Weight: <number of messages> or <size of messages>,
			})
		}
	}

	return pl, nil
}
