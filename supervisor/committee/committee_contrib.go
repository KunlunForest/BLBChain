package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ContribCommitteeModule 基于贡献值的委员会模块
type ContribCommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// ContribChain相关变量
	curEpoch               int32
	contribLock            sync.Mutex
	contribStates          map[uint64]*partition.ContribState // shardID -> ContribState
	modifiedMap            map[string]uint64
	contribLastRunningTime time.Time
	contribFreq            int // 重配置频率（秒）

	// logger模块
	sl *supervisor_log.SupervisorLog

	// 控制组件
	Ss          *signal.StopSignal
	IpNodeTable map[uint64]map[uint64]string
}

func NewContribCommitteeModule(
	Ip_nodeTable map[uint64]map[uint64]string,
	Ss *signal.StopSignal,
	sl *supervisor_log.SupervisorLog,
	csvFilePath string,
	dataNum, batchNum, contribFrequency int,
) *ContribCommitteeModule {
	
	contribStates := make(map[uint64]*partition.ContribState)
	for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
		cs := new(partition.ContribState)
		// 参数: shardID, shardNum, decay, increment, transferThresh, retainThresh
		cs.Init_ContribState(sid, uint64(params.ShardNum), 0.9, 1.0, 5.0, 20.0)
		contribStates[sid] = cs
	}

	return &ContribCommitteeModule{
		csvPath:                csvFilePath,
		dataTotalNum:           dataNum,
		batchDataNum:           batchNum,
		nowDataNum:             0,
		contribStates:          contribStates,
		modifiedMap:            make(map[string]uint64),
		contribFreq:            contribFrequency,
		contribLastRunningTime: time.Time{},
		IpNodeTable:            Ip_nodeTable,
		Ss:                     Ss,
		sl:                     sl,
		curEpoch:               0,
	}
}

func (ccm *ContribCommitteeModule) HandleOtherMessage([]byte) {}

func (ccm *ContribCommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := ccm.modifiedMap[key]; ok {
		return val
	}
	return uint64(utils.Addr2Shard(key))
}

// 交易发送控制
func (ccm *ContribCommitteeModule) txSending(txlist []*core.Transaction) {
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// 发送到各分片
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, ccm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := ccm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

// 消息发送控制（主循环）
func (ccm *ContribCommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(ccm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0)
	contribCnt := 0

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		
		if tx, ok := data2tx(data, uint64(ccm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			ccm.nowDataNum++
		} else {
			continue
		}

		// 批量发送条件
		if len(txlist) == int(ccm.batchDataNum) || ccm.nowDataNum == ccm.dataTotalNum {
			if ccm.contribLastRunningTime.IsZero() {
				ccm.contribLastRunningTime = time.Now()
			}
			ccm.txSending(txlist)
			txlist = make([]*core.Transaction, 0)
			ccm.Ss.StopGap_Reset()
		}

		// ContribChain重配置触发
		if params.ShardNum > 1 && !ccm.contribLastRunningTime.IsZero() && 
		   time.Since(ccm.contribLastRunningTime) >= time.Duration(ccm.contribFreq)*time.Second {
			
			ccm.contribLock.Lock()
			contribCnt++
			
			// 执行贡献值衰减
			for _, cs := range ccm.contribStates {
				cs.DecayAllContributions()
				cs.PrintContribStats()
			}
			
			// 计算重新分配方案
			globalReallocationMap := make(map[string]uint64)
			for _, cs := range ccm.contribStates {
				realloc := cs.ComputeReallocation()
				for addr, targetShard := range realloc {
					globalReallocationMap[addr] = targetShard
				}
			}
			
			// 发送重配置消息
			ccm.contribMapSend(globalReallocationMap)
			for key, val := range globalReallocationMap {
				ccm.modifiedMap[key] = val
			}
			
			ccm.contribLock.Unlock()

			// 等待epoch同步
			for atomic.LoadInt32(&ccm.curEpoch) != int32(contribCnt) {
				time.Sleep(time.Second)
			}
			
			ccm.contribLastRunningTime = time.Now()
			ccm.sl.Slog.Println("Next ContribChain epoch begins.")
		}

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// 所有交易发送完毕，继续发送分区消息
	for !ccm.Ss.GapEnough() {
		time.Sleep(time.Second)
		if params.ShardNum > 1 && time.Since(ccm.contribLastRunningTime) >= time.Duration(ccm.contribFreq)*time.Second {
			ccm.contribLock.Lock()
			contribCnt++
			
			for _, cs := range ccm.contribStates {
				cs.DecayAllContributions()
			}
			
			globalReallocationMap := make(map[string]uint64)
			for _, cs := range ccm.contribStates {
				realloc := cs.ComputeReallocation()
				for addr, targetShard := range realloc {
					globalReallocationMap[addr] = targetShard
				}
			}
			
			ccm.contribMapSend(globalReallocationMap)
			for key, val := range globalReallocationMap {
				ccm.modifiedMap[key] = val
			}
			
			ccm.contribLock.Unlock()

			for atomic.LoadInt32(&ccm.curEpoch) != int32(contribCnt) {
				time.Sleep(time.Second)
			}
			
			ccm.sl.Slog.Println("Next ContribChain epoch begins.")
			ccm.contribLastRunningTime = time.Now()
		}
	}
}

// 发送分区修改映射
func (ccm *ContribCommitteeModule) contribMapSend(m map[string]uint64) {
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, ccm.IpNodeTable[i][0])
	}
	ccm.sl.Slog.Println("Supervisor: all ContribChain partition map messages sent.")
}

// 处理区块信息（更新贡献值）
func (ccm *ContribCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("curEpoch updated to", b.Epoch)
	}
	
	if b.BlockBodyLength == 0 {
		return
	}

	ccm.contribLock.Lock()
	defer ccm.contribLock.Unlock()

	cs := ccm.contribStates[b.SenderShardID]
	
	// 更新片内交易的贡献值
	for _, tx := range b.InnerShardTxs {
		cs.UpdateContribution(tx.Sender, true)
		cs.UpdateContribution(tx.Recipient, true)
		cs.AddLocalAccount(tx.Sender)
		cs.AddLocalAccount(tx.Recipient)
	}
	
	// 统计跨片交易
	crossTxCount := len(b.Relay1Txs) + len(b.Relay2Txs)
	cs.CrossShardTxCount += crossTxCount
	
	// 对跨片交易不增加贡献值（或减少贡献值）
	for _, tx := range b.Relay1Txs {
		cs.UpdateContribution(tx.Sender, false)
	}
	for _, tx := range b.Relay2Txs {
		cs.UpdateContribution(tx.Recipient, false)
	}
}