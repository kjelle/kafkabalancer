package main

import "fmt"

// ValidateWeights make sure that either all partitions have an explicit,
// strictly positive weight or that all partitions have no weight
func ValidateWeights(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	hasWeights := pl.Partitions[0].Weight != 0

	for _, p := range pl.Partitions {
		if hasWeights && p.Weight == 0 {
			return nil, fmt.Errorf("partition %v has no weight", p)
		}
		if !hasWeights && p.Weight != 0 {
			return nil, fmt.Errorf("partition %v has no weight", pl.Partitions[0])
		}
		if p.Weight < 0 {
			return nil, fmt.Errorf("partition %v has negative weight", p)
		}
	}

	return nil, nil
}

// ValidateReplicas checks that partitions don't have more than one replica per
// broker
func ValidateReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	for _, p := range pl.Partitions {
		replicaset := toBrokerSet(p.Replicas)
		if len(replicaset) != len(p.Replicas) {
			return nil, fmt.Errorf("partition %v has duplicated replicas", p)
		}
	}

	return nil, nil
}

// FillDefaults fills in default values for Weight, Brokers and NumReplicas
func FillDefaults(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	// if the weights are 0, set them to 1
	if pl.Partitions[0].Weight == 0 {
		for idx := range pl.Partitions {
			pl.Partitions[idx].Weight = 1.0
		}
	}

	// if the set of candidate brokers is empty, fill it with the default set
	brokers := cfg.Brokers
	if brokers == nil {
		brokers = getBrokerList(pl)
	}
	for idx := range pl.Partitions {
		if pl.Partitions[idx].Brokers == nil {
			pl.Partitions[idx].Brokers = brokers
		}
	}

	// if the desired number of replicas is 0, fill it with the current number
	for idx := range pl.Partitions {
		if pl.Partitions[idx].NumReplicas == 0 {
			pl.Partitions[idx].NumReplicas = len(pl.Partitions[idx].Replicas)
		}
	}

	return nil, nil
}

// RemoveExtraReplicas removes replicas from partitions having lower NumReplicas
// than the current number of replicas
func RemoveExtraReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	loads := getBrokerLoad(pl)

	for _, p := range pl.Partitions {
		if p.NumReplicas >= len(p.Replicas) {
			continue
		}

		brokersByLoad := getBrokerListByLoad(loads, p.Brokers)
		for _, b := range brokersByLoad {
			if inBrokerList(p.Replicas, b) {
				return replacepl(p, b, -1), nil
			}
		}

		return nil, fmt.Errorf("partition %v unable to pick replica to remove", p)
	}

	return nil, nil
}

// AddMissingReplicas adds replicas to partitions having NumReplicas greater
// than the current number of replicas
func AddMissingReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	loads := getBrokerLoad(pl)
	// add missing replicas
	for _, p := range pl.Partitions {
		if p.NumReplicas <= len(p.Replicas) {
			continue
		}

		brokersByLoad := getBrokerListByLoad(loads, p.Brokers)
		for idx := len(brokersByLoad) - 1; idx >= 0; idx-- {
			b := brokersByLoad[idx]
			if !inBrokerList(p.Replicas, b) {
				return addpl(p, b), nil
			}
		}

		return nil, fmt.Errorf("partition %v unable to pick replica to add", p)
	}

	return nil, nil
}

// MoveDisallowedReplicas moves replicas from non-allowed brokers to the least
// loaded ones
func MoveDisallowedReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	loads := getBrokerLoad(pl)
	bl := getBL(loads)

	for _, p := range pl.Partitions {
		brokersByLoad := getBrokerListByLoadBL(bl, p.Brokers)

		for _, id := range p.Replicas {
			if inBrokerList(brokersByLoad, id) {
				continue
			}

			for idx := len(brokersByLoad) - 1; idx >= 0; idx-- {
				b := brokersByLoad[idx]
				if inBrokerList(p.Replicas, b) {
					continue
				}

				return replacepl(p, id, b), nil
			}

			return nil, fmt.Errorf("partition %v unable to pick replica to replace broker %d", p, id)
		}
	}

	return nil, nil
}

func move(pl *PartitionList, cfg RebalanceConfig, leaders bool) (*PartitionList, error) {
	var cp Partition
	var cr, cb BrokerID

	//fmt.Printf("Brokers: %#q\n", cfg.Brokers)
	loads := getBrokerLoad(pl)
	for _, id := range cfg.Brokers {
		if _, found := loads[id]; !found {
			loads[id] = 0 // default, a broker has no load.
		}
	}

	bl := getBL(loads)
	/*	for _, bl := range bl {
		fmt.Printf("%d: %f\n", bl.ID, bl.Load)
	}*/

	su := getUnbalanceBL(bl)
	cu := su

	//	fmt.Printf("su: %f\n", su)

	for _, p := range pl.Partitions {
		if p.NumReplicas < cfg.MinReplicasForRebalancing {
			continue
		}

		replicas := p.Replicas[1:]
		if leaders {
			replicas = p.Replicas[0:1]
		}

		for _, r := range replicas {
			//fmt.Printf("assessing Partition: %s Replica: %d\n", p, r)
			ridx := -1
			var rload float64
			for idx, b := range bl {
				if b.ID == r {
					ridx = idx
					rload = b.Load
					bl[idx].Load -= p.Weight
				}
			}
			if ridx == -1 {
				return nil, fmt.Errorf("assertion failed: replica %d not in broker loads %v", r, bl)
			}

			for idx, b := range bl {
				if !inBrokerList(p.Brokers, b.ID) {
					//fmt.Printf("Broker: %d not in brokers: %#q\n", b.ID, p.Brokers)
					continue
				}

				// allow moving the actual leader, even if all replicas are adjusted.
				if inBrokerList(p.Replicas, b.ID) {
					continue
				}

				//				fmt.Printf("Broker#%d (load: %f) not in replicas: %#q\n", b.ID, bl[idx].Load, p.Replicas)

				bload := bl[idx].Load
				//fmt.Printf("We try to add %f to broker#%d which will get %f load to see if it is less unbalanced.\n", p.Weight, bl[idx].ID, bl[idx].Load+p.Weight)
				bl[idx].Load += p.Weight
				u := getUnbalanceBL(bl)
				//				fmt.Printf("u: %f\n", u)

				if u < cu {
					//					fmt.Printf("> that helped! updateding cu from %f -> %f\n", cu, u)
					cu, cp, cr, cb = u, p, r, b.ID
				} /*  else {
					fmt.Printf("<< That did not help.\n")
				} */

				bl[idx].Load = bload
			}

			bl[ridx].Load = rload
		}
	}

	//fmt.Printf("cu: %f su: %f\n", cu, su)
	//fmt.Printf("Unbalance? %f < %f\n", cu, su-cfg.MinUnbalance)
	if cu < su-cfg.MinUnbalance {
		return replacepl(cp, cr, cb), nil
	}

	return nil, nil
}

func distributeLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	loads := getBrokerLoad(pl)
	for _, id := range cfg.Brokers {
		if _, found := loads[id]; !found {
			loads[id] = 0 // default, a broker has no load.
		}
	}

	bl := getBL(loads)
	//	for _, bl := range bl {
	//		fmt.Printf("balance leader: %d: %f\n", bl.ID, bl.Load)
	//	}

	su := getUnbalanceBL(bl)
	//	fmt.Printf("balance leader su: %f\n", su)
	if su < cfg.MinUnbalance {
		//		fmt.Printf("balance leader not enough unbalance (%f) compared to MinUnbalance %f\n", su, cfg.MinUnbalance)
		return nil, nil

	}

	heavyLoadedBroker := bl[len(bl)-1]
	//	fmt.Printf("balance leader Find the partiitons for the broker with the hardest load: %s\n", heavyLoadedBroker.ID.String())
	pp := []Partition{}
	for _, p := range pl.Partitions {
		if p.Replicas[0] == heavyLoadedBroker.ID {
			pp = append(pp, p)
		}
	}
	//	fmt.Printf("balance leader Heavy Partitions to balance away! %+v\n", pp)
	for _, p := range pp {
		if p.NumReplicas < cfg.MinReplicasForRebalancing {
			continue
		}

		//		fmt.Printf("balance leader for Partition: %s\n", p)

		// We select all brokers from the replica list, including the leader (from idx 0)
		//replicas := p.Replicas[0:1]

		// From that list we find the broker with the least load.
		//	bl := getBL(loads)
		//		fmt.Printf("balance leader  give leadership of partition %s to broker %s from broker %s\n", p.Partition.String(), bl[0].ID.String(), p.Replicas[0])

		return replacepl(p, p.Replicas[0], bl[0].ID), nil
	}

	return nil, nil
}

// MoveNonLeaders moves non-leader replicas from overloaded brokers to
// underloaded brokers
func MoveNonLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	return move(pl, cfg, false)
}

// MoveLeaders moves leader replicas from overloaded brokers to underloaded
// brokers
func MoveLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	if !cfg.AllowLeaderRebalancing {
		return nil, nil
	}

	return move(pl, cfg, true)
}

// RebalanceLeaders rebalances the leaders which already have replicas
func ReassignLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	if !cfg.RebalanceLeaders {
		return nil, nil
	}

	return distributeLeaders(pl, cfg)
}
