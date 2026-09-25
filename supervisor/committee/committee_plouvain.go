package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"sort"
	"strconv"
	"time"
)

type PLouvainCommittee struct {
	csvPath         string
	dataTotalNum    int
	nowDataNum      int
	batchDataNum    int
	reconfigTimeGap int
	IpNodeTable     map[uint64]map[uint64]string
	partitionMap    map[string]uint64
	Ss              *signal.StopSignal
	sl              *supervisor_log.SupervisorLog
}

func NewPLouvainCommitteeModule(
	Ip_nodeTable map[uint64]map[uint64]string,
	Ss *signal.StopSignal,
	sl *supervisor_log.SupervisorLog,
	csvFilePath string,
	dataNum int,
	batchNum int,
	reconfigTimeGap int,
) *PLouvainCommittee {
	return &PLouvainCommittee{
		csvPath:         csvFilePath,
		dataTotalNum:    dataNum,
		nowDataNum:      0,
		batchDataNum:    batchNum,
		reconfigTimeGap: reconfigTimeGap,
		IpNodeTable:     Ip_nodeTable,
		partitionMap:    make(map[string]uint64),
		Ss:              Ss,
		sl:              sl,
	}
}

// 实现 CommitteeModule 接口
func (p *PLouvainCommittee) HandleBlockInfo(*message.BlockInfoMsg) {}

func (p *PLouvainCommittee) HandleOtherMessage([]byte) {}

func (p *PLouvainCommittee) MsgSendingControl() {
	p.RunPLouvain()
}

// Transaction 表示一笔交易
type Transaction struct {
	Sender    string
	Recipient string
	Value     float64
}

// 运行 PLouvain 分区算法
func (p *PLouvainCommittee) RunPLouvain() {
	log.Println("=== Phase 0: Preprocessing - Scanning ALL transactions ===")
	startTime := time.Now()

	// 读取所有交易
	allTxs, err := p.loadAllTransactions()
	if err != nil {
		log.Fatalf("Failed to load transactions: %v", err)
	}

	totalTxs := len(allTxs)
	log.Println("=== Preprocessing Complete ===")
	log.Printf("  Processed: %d transactions\n", totalTxs)

	// 构建图（只保留有交易的边）
	graph := make(map[string]map[string]float64)
	addresses := make(map[string]bool)

	for _, tx := range allTxs {
		addresses[tx.Sender] = true
		addresses[tx.Recipient] = true

		if graph[tx.Sender] == nil {
			graph[tx.Sender] = make(map[string]float64)
		}
		if graph[tx.Recipient] == nil {
			graph[tx.Recipient] = make(map[string]float64)
		}

		// 双向边，权重为交易价值
		graph[tx.Sender][tx.Recipient] += tx.Value
		graph[tx.Recipient][tx.Sender] += tx.Value
	}

	// 统计实际的边数
	actualEdges := 0
	for _, neighbors := range graph {
		actualEdges += len(neighbors)
	}

	log.Printf("  Graph: %d vertices, %d edges\n", len(addresses), actualEdges)
	log.Printf("  Time: %v\n", time.Since(startTime))

	// 运行改进的 PLouvain
	log.Println("=== Phase 1: Running Improved P-Louvain ===")
	partition := ImprovedPLouvain(graph, allTxs, params.ShardNum)

	// 保存分区映射
	for addr, shard := range partition {
		p.partitionMap[addr] = shard
	}

	preprocessTime := time.Since(startTime)
	log.Printf("=== Initial Partitioning Complete (Time: %v) ===\n", preprocessTime)
	log.Printf("  Partition map size: %d addresses\n", len(p.partitionMap))

	// 发送交易
	log.Println("=== Phase 2: Sending transactions with optimized partition ===")
	p.sendTransactionsWithPartition(allTxs)
}

// 改进的 PLouvain 算法
func ImprovedPLouvain(graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	log.Println("=== Starting Improved P-Louvain ===")

	// Phase 0: 基于交易频率的初始分区
	log.Println("Phase 0: Transaction-based initial partitioning...")
	partition := transactionBasedPartitioning(graph, allTxs, numShards)

	// Phase 1: Louvain 社区检测优化
	log.Println("Phase 1: Louvain-based community optimization...")
	partition = louvainOptimization(partition, graph, numShards)

	// Phase 2: 边界节点优化
	log.Println("Phase 2: Boundary node optimization...")
	partition = optimizeBoundaryNodes(partition, graph, allTxs, numShards)

	// Phase 3: 最终负载均衡
	log.Println("Phase 3: Final load balancing...")
	partition = finalLoadBalancing(partition, graph, allTxs, numShards)

	// 统计结果
	printPartitionStats(partition, allTxs, numShards)

	return partition
}

// 基于交易频率的初始分区
func transactionBasedPartitioning(graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	// 统计每个地址的交易频率
	addrTxCount := make(map[string]int)
	for _, tx := range allTxs {
		addrTxCount[tx.Sender]++
		addrTxCount[tx.Recipient]++
	}

	// 按交易频率排序地址
	type AddrFreq struct {
		Addr  string
		Count int
	}
	addrList := make([]AddrFreq, 0)
	for addr, count := range addrTxCount {
		addrList = append(addrList, AddrFreq{addr, count})
	}
	sort.Slice(addrList, func(i, j int) bool {
		return addrList[i].Count > addrList[j].Count
	})

	// 初始化分片
	partition := make(map[string]uint64)
	shardWorkload := make([]int, numShards)

	// 贪心分配：高频地址优先分配到负载轻的分片
	for _, af := range addrList {
		// 找到负载最轻的分片
		minShard := 0
		minWorkload := shardWorkload[0]
		for sid := 1; sid < numShards; sid++ {
			if shardWorkload[sid] < minWorkload {
				minShard = sid
				minWorkload = shardWorkload[sid]
			}
		}

		partition[af.Addr] = uint64(minShard)
		shardWorkload[minShard] += af.Count
	}

	log.Println("  Initial partitioning complete:")
	for sid := 0; sid < numShards; sid++ {
		nodeCount := 0
		for _, shard := range partition {
			if shard == uint64(sid) {
				nodeCount++
			}
		}
		log.Printf("    Shard %d: %d nodes, workload=%d\n", sid, nodeCount, shardWorkload[sid])
	}

	return partition
}

// Louvain 优化
func louvainOptimization(partition map[string]uint64, graph map[string]map[string]float64, numShards int) map[string]uint64 {
	maxIterations := 5

	for iter := 0; iter < maxIterations; iter++ {
		moved := 0

		// 遍历所有节点
		for node := range graph {
			currentShard := partition[node]

			// 计算该节点与每个分片的连接权重
			shardWeights := make([]float64, numShards)
			for neighbor, weight := range graph[node] {
				neighborShard := partition[neighbor]
				shardWeights[neighborShard] += weight
			}

			// 找到连接权重最大的分片
			maxShard := uint64(0)
			maxWeight := shardWeights[0]
			for sid := 1; sid < numShards; sid++ {
				if shardWeights[sid] > maxWeight {
					maxShard = uint64(sid)
					maxWeight = shardWeights[sid]
				}
			}

			// 如果移动能显著增加片内连接，则移动
			currentShardWeight := shardWeights[currentShard]
			if maxWeight > currentShardWeight*1.2 {
				partition[node] = maxShard
				moved++
			}
		}

		log.Printf("  Louvain optimization iteration %d: %d nodes moved\n", iter+1, moved)
		if moved == 0 {
			break
		}
	}

	return partition
}

// 边界节点优化
func optimizeBoundaryNodes(partition map[string]uint64, graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	maxIterations := 3

	for iter := 0; iter < maxIterations; iter++ {
		moved := 0

		// 遍历所有节点
		for node := range graph {
			currentShard := partition[node]

			// 计算该节点与每个分片的连接权重
			shardWeights := make([]float64, numShards)
			for neighbor, weight := range graph[node] {
				neighborShard := partition[neighbor]
				shardWeights[neighborShard] += weight
			}

			// 找到连接权重最大的分片
			maxShard := uint64(0)
			maxWeight := shardWeights[0]
			for sid := 1; sid < numShards; sid++ {
				if shardWeights[sid] > maxWeight {
					maxShard = uint64(sid)
					maxWeight = shardWeights[sid]
				}
			}

			// 如果移动能显著减少跨片边，则移动
			currentShardWeight := shardWeights[currentShard]
			if maxWeight > currentShardWeight*1.5 {
				partition[node] = maxShard
				moved++
			}
		}

		log.Printf("  Boundary optimization iteration %d: %d nodes moved\n", iter+1, moved)
		if moved == 0 {
			break
		}
	}

	return partition
}

// 最终负载均衡
func finalLoadBalancing(partition map[string]uint64, graph map[string]map[string]float64, allTxs []Transaction, numShards int) map[string]uint64 {
	// 计算每个分片的负载（片内交易数）
	shardWorkload := make([]int, numShards)
	shardNodes := make([][]string, numShards)

	for node, shard := range partition {
		shardNodes[shard] = append(shardNodes[shard], node)
	}

	// 计算负载（片内交易数）
	for _, tx := range allTxs {
		senderShard, senderExists := partition[tx.Sender]
		recipientShard, recipientExists := partition[tx.Recipient]
		
		if senderExists && recipientExists && senderShard == recipientShard {
			shardWorkload[senderShard]++
		}
	}

	avgWorkload := float64(len(allTxs)) / float64(numShards)

	// 迭代均衡
	maxIterations := 10
	for iter := 0; iter < maxIterations; iter++ {
		// 找到负载最重和最轻的分片
		maxShard, maxWorkload := 0, shardWorkload[0]
		minShard, minWorkload := 0, shardWorkload[0]

		for sid := 1; sid < numShards; sid++ {
			if shardWorkload[sid] > maxWorkload {
				maxShard, maxWorkload = sid, shardWorkload[sid]
			}
			if shardWorkload[sid] < minWorkload {
				minShard, minWorkload = sid, shardWorkload[sid]
			}
		}

		// 如果负载已经均衡（最大不超过平均的 1.3 倍），则停止
		if float64(maxWorkload) < avgWorkload*1.3 {
			log.Printf("  Load balanced at iteration %d (max=%d, avg=%.0f, ratio=%.2f)\n",
				iter+1, maxWorkload, avgWorkload, float64(maxWorkload)/avgWorkload)
			break
		}

		// 找到最重分片中跨片连接最多的节点
		bestNode := ""
		maxCrossShardEdges := 0

		for _, node := range shardNodes[maxShard] {
			crossShardEdges := 0
			for neighbor := range graph[node] {
				if neighborShard, exists := partition[neighbor]; exists && neighborShard != uint64(maxShard) {
					crossShardEdges++
				}
			}
			if crossShardEdges > maxCrossShardEdges {
				maxCrossShardEdges = crossShardEdges
				bestNode = node
			}
		}

		if bestNode == "" || maxCrossShardEdges == 0 {
			break
		}

		// 移动节点
		partition[bestNode] = uint64(minShard)

		// 更新分片节点列表
		for i, node := range shardNodes[maxShard] {
			if node == bestNode {
				shardNodes[maxShard] = append(shardNodes[maxShard][:i], shardNodes[maxShard][i+1:]...)
				break
			}
		}
		shardNodes[minShard] = append(shardNodes[minShard], bestNode)

		// 重新计算负载
		shardWorkload = make([]int, numShards)
		for _, tx := range allTxs {
			senderShard, senderExists := partition[tx.Sender]
			recipientShard, recipientExists := partition[tx.Recipient]
			
			if senderExists && recipientExists && senderShard == recipientShard {
				shardWorkload[senderShard]++
			}
		}

		if iter%2 == 0 {
			log.Printf("  Balancing iteration %d: moved node from S%d to S%d (workloads: ",
				iter+1, maxShard, minShard)
			for sid := 0; sid < numShards; sid++ {
				fmt.Printf("S%d=%d ", sid, shardWorkload[sid])
			}
			fmt.Println(")")
		}
	}

	return partition
}

// 打印分区统计信息
func printPartitionStats(partition map[string]uint64, allTxs []Transaction, numShards int) {
	shardNodes := make([]int, numShards)
	shardInternalTxs := make([]int, numShards)
	totalCrossShardTxs := 0

	for _, shard := range partition {
		shardNodes[shard]++
	}

	for _, tx := range allTxs {
		senderShard, senderExists := partition[tx.Sender]
		recipientShard, recipientExists := partition[tx.Recipient]

		if senderExists && recipientExists {
			if senderShard == recipientShard {
				shardInternalTxs[senderShard]++
			} else {
				totalCrossShardTxs++
			}
		}
	}

	log.Println("=== P-Louvain Stats ===")
	for sid := 0; sid < numShards; sid++ {
		log.Printf("Shard %d: Nodes=%d, InternalTxs=%d (%.1f%% of total)\n",
			sid, shardNodes[sid], shardInternalTxs[sid],
			float64(shardInternalTxs[sid])/float64(len(allTxs))*100)
	}

	crossShardRatio := float64(totalCrossShardTxs) / float64(len(allTxs))
	log.Printf("Cross-Shard Transactions: %d / %d (%.2f%%)\n",
		totalCrossShardTxs, len(allTxs), crossShardRatio*100)
	log.Println("=======================")
}

// 加载所有交易
func (p *PLouvainCommittee) loadAllTransactions() ([]Transaction, error) {
	file, err := os.Open(p.csvPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	allTxs := make([]Transaction, 0)
	totalToRead := p.dataTotalNum

	for i, record := range records {
		if i == 0 || i > totalToRead {
			continue
		}

		if len(record) < 5 {
			continue
		}

		value, _ := strconv.ParseFloat(record[4], 64)
		tx := Transaction{
			Sender:    record[2],
			Recipient: record[3],
			Value:     value,
		}
		allTxs = append(allTxs, tx)

		if i%50000 == 0 {
			log.Printf("  Preprocessing: %d txs (%.1f%%)\n", i, float64(i)/float64(totalToRead)*100)
		}
	}

	return allTxs, nil
}

// 发送交易
func (p *PLouvainCommittee) sendTransactionsWithPartition(allTxs []Transaction) {
	startTime := time.Now()
	totalSent := 0

	// 按分片组织交易
	shardTxs := make(map[uint64][]*core.Transaction)

	for _, tx := range allTxs {
		// 获取发送方的分片
		shard, exists := p.partitionMap[tx.Sender]
		if !exists {
			// 如果地址不在分区映射中，使用默认分片策略
			shard = uint64(utils.Addr2Shard(tx.Sender))
		}

		// 将 float64 转换为 *big.Int
		value := big.NewInt(int64(tx.Value))

		coreTx := &core.Transaction{
			Sender:    tx.Sender,
			Recipient: tx.Recipient,
			Value:     value,
		}

		shardTxs[shard] = append(shardTxs[shard], coreTx)
		totalSent++

		// 按批次发送
		if len(shardTxs[shard]) >= p.batchDataNum {
			p.sendBatchToShard(shard, shardTxs[shard])
			shardTxs[shard] = nil
		}

		// 控制注入速度
		if params.InjectSpeed > 0 && totalSent%params.InjectSpeed == 0 {
			time.Sleep(1 * time.Second)
		}

		if totalSent%10000 == 0 {
			elapsed := time.Since(startTime)
			log.Printf("  Sending: %d / %d txs (%.1f%%). Time: %v\n",
				totalSent, len(allTxs), float64(totalSent)/float64(len(allTxs))*100, elapsed)
		}
	}

	// 发送剩余交易
	for shard, txs := range shardTxs {
		if len(txs) > 0 {
			p.sendBatchToShard(shard, txs)
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("=== Transaction Injection Complete ===\n")
	log.Printf("  Total sent: %d txs\n", totalSent)
	log.Printf("  Time: %v\n", elapsed)
	log.Printf("  Average speed: %.2f txs/s\n", float64(totalSent)/elapsed.Seconds())
}

// 发送一批交易到指定分片
func (p *PLouvainCommittee) sendBatchToShard(shardID uint64, txs []*core.Transaction) {
	it := message.InjectTxs{
		Txs:       txs,
		ToShardID: shardID,
	}

	itByte, err := json.Marshal(it)
	if err != nil {
		log.Panic(err)
	}

	send_msg := message.MergeMessage(message.CInject, itByte)
	
	// 获取目标分片的第一个节点IP
	if nodeIPs, exists := p.IpNodeTable[shardID]; exists {
		for _, ip := range nodeIPs {
			go networks.TcpDial(send_msg, ip)
			break // 只发送给第一个节点
		}
	}
}
