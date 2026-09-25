package partition

import (
	"sync"
)

// ContribState 贡献值分片状态
type ContribState struct {
	// 账户贡献值映射 address -> contribution score
	ContribMap map[string]float64
	
	// 当前分片的账户集合
	LocalAccounts map[string]bool
	
	// 分片ID
	ShardID uint64
	ShardNum uint64
	
	// 贡献值衰减因子 (0-1之间，越小衰减越快)
	DecayFactor float64
	
	// 贡献值增量 (每次交易增加的贡献值)
	ContribIncrement float64
	
	// 转移阈值 (低于此值的账户可能被转移)
	TransferThreshold float64
	
	// 保留阈值 (高于此值的账户优先保留)
	RetainThreshold float64
	
	// 跨片交易数统计
	CrossShardTxCount int
	
	lock sync.RWMutex
}

// 初始化ContribState
func (cs *ContribState) Init_ContribState(shardID, shardNum uint64, decay, increment, transferThresh, retainThresh float64) {
	cs.ShardID = shardID
	cs.ShardNum = shardNum
	cs.ContribMap = make(map[string]float64)
	cs.LocalAccounts = make(map[string]bool)
	cs.DecayFactor = decay
	cs.ContribIncrement = increment
	cs.TransferThreshold = transferThresh
	cs.RetainThreshold = retainThresh
	cs.CrossShardTxCount = 0
}

// 更新账户贡献值 (交易发生时调用)
func (cs *ContribState) UpdateContribution(addr string, isLocal bool) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	
	if _, exists := cs.ContribMap[addr]; !exists {
		cs.ContribMap[addr] = 0
	}
	
	// 如果是本地交易，增加贡献值；否则不增加
	if isLocal {
		cs.ContribMap[addr] += cs.ContribIncrement
	}
}

// 衰减所有账户的贡献值 (每个epoch调用一次)
func (cs *ContribState) DecayAllContributions() {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	
	for addr := range cs.ContribMap {
		cs.ContribMap[addr] *= cs.DecayFactor
		// 清理贡献值过低的记录
		if cs.ContribMap[addr] < 0.01 {
			delete(cs.ContribMap, addr)
		}
	}
}

// 获取账户贡献值
func (cs *ContribState) GetContribution(addr string) float64 {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	
	if val, exists := cs.ContribMap[addr]; exists {
		return val
	}
	return 0
}

// 添加本地账户
func (cs *ContribState) AddLocalAccount(addr string) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cs.LocalAccounts[addr] = true
}

// 移除本地账户
func (cs *ContribState) RemoveLocalAccount(addr string) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	delete(cs.LocalAccounts, addr)
}

// 判断是否为本地账户
func (cs *ContribState) IsLocalAccount(addr string) bool {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	return cs.LocalAccounts[addr]
}

// 计算分片重新分配方案
func (cs *ContribState) ComputeReallocation() map[string]uint64 {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	
	reallocationMap := make(map[string]uint64)
	
	// 找出低贡献值账户（候选转移账户）
	lowContribAccounts := make([]string, 0)
	for addr := range cs.LocalAccounts {
		contrib := cs.ContribMap[addr]
		if contrib < cs.TransferThreshold {
			lowContribAccounts = append(lowContribAccounts, addr)
		}
	}
	
	// 将低贡献账户分配到其他分片（简单轮询策略）
	targetShard := (cs.ShardID + 1) % cs.ShardNum
	for _, addr := range lowContribAccounts {
		reallocationMap[addr] = targetShard
		targetShard = (targetShard + 1) % cs.ShardNum
		if targetShard == cs.ShardID {
			targetShard = (targetShard + 1) % cs.ShardNum
		}
	}
	
	return reallocationMap
}

// 打印贡献值统计
func (cs *ContribState) PrintContribStats() {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	
	totalAccounts := len(cs.LocalAccounts)
	highContrib := 0
	lowContrib := 0
	
	for addr := range cs.LocalAccounts {
		contrib := cs.ContribMap[addr]
		if contrib >= cs.RetainThreshold {
			highContrib++
		} else if contrib < cs.TransferThreshold {
			lowContrib++
		}
	}
	
	println("=== Contribution Statistics ===")
	println("Shard ID:", cs.ShardID)
	println("Total Accounts:", totalAccounts)
	println("High Contribution (>=", cs.RetainThreshold, "):", highContrib)
	println("Low Contribution (<", cs.TransferThreshold, "):", lowContrib)
	println("Cross-Shard Tx Count:", cs.CrossShardTxCount)
	println("===============================")
}