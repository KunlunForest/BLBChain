# BLBChain

> **BLBChain** is a cross-layer blockchain sharding framework that revisits the throughput bottleneck of sharded blockchains from a **cross-layer perspective**. Existing approaches primarily focus on transaction-layer load balancing while overlooking the fundamental bottleneck caused by data propagation along the consensus path. At the network layer, BLBChain introduces a **transaction data pre-distribution mechanism** that decouples bulk data transmission from the consensus-critical path, allowing consensus nodes to process more effective transactions in each round and thereby increasing the practical capacity of each shard. At the transaction layer, BLBChain employs a **pressure-aware account allocation algorithm**, where the ratio between workload and effective processing capacity is used as the pressure metric. Accounts are dynamically migrated to minimize the pressure variance across shards.

---

## Overview

BLBChain extends [BlockEmulator](https://github.com/HuangLab-SYSU/block-emulator) with a graph-based **dynamic account allocation mechanism**.

To validate the proposed cross-layer architecture, we modified the underlying logic of BlockEmulator and implemented:

- A network-layer transaction data pre-distribution mechanism;
- A pressure-aware account allocation algorithm;
- An improved P-Louvain graph partitioning pipeline;
- Experimental evaluation of throughput, latency, transaction-pool length, cross-shard transactions, and shard load balance.

We are also porting BLBChain to **ChainMaker**. The consensus-layer design for the ChainMaker implementation has been completed, while the transaction-layer sharding mechanism is still under development.

## Design Objectives

| Objective | Description |
|---|---|
| 🔗 **Reduce cross-shard transactions** | Place frequently interacting accounts in the same shard to reduce cross-shard coordination overhead. |
| ⚖️ **Balance shard workloads** | Maintain similar workloads across shards and avoid performance degradation caused by hotspot shards. |
| 🚀 **Improve system throughput** | Reducing cross-shard coordination overhead enables the system to achieve higher effective TPS. |

## Modified Components

| Component | Path | Description |
|---|---|---|
| P-Louvain core algorithm | `partition/PLouvain.go` | Implements the graph-based partitioning and account-movement phases. |
| P-Louvain committee module | `supervisor/committee/PLouvainCommittee.go` | Loads transactions, constructs the interaction graph, schedules partitioning, balances shard workloads, and injects transactions. |

---

## Improved P-Louvain Partitioning Algorithm

Before transaction injection, BLBChain executes a four-stage partitioning pipeline:

```text
Raw transaction data (CSV)
          |
          v
+-----------------------------------------+
| Stage 0: Transaction-frequency-based    |
|          initial partitioning           |
| Greedily assign accounts according to   |
| their transaction frequencies           |
+-------------------+---------------------+
                    |
                    v
+-----------------------------------------+
| Stage 1: Louvain community optimization |
| Iteratively optimize partitions using   |
| weighted account-interaction edges       |
+-------------------+---------------------+
                    |
                    v
+-----------------------------------------+
| Stage 2: Boundary-node optimization     |
| Move boundary accounts to reduce        |
| cross-shard edges                        |
+-------------------+---------------------+
                    |
                    v
+-----------------------------------------+
| Stage 3: Final load balancing           |
| Iteratively migrate accounts to balance |
| transaction workloads across shards     |
+-------------------+---------------------+
                    |
                    v
     Partition map (account address -> shard ID)
```

### Stage 0 — Transaction-Frequency-Based Initial Partitioning

**Implementation:**

`supervisor/committee/PLouvainCommittee.go` → `transactionBasedPartitioning()`

This stage performs the following operations:

1. Count the transaction frequency of each account address in the dataset.
2. Sort account addresses in descending order of transaction frequency.
3. Process high-frequency accounts first.
4. Greedily assign each account to the currently least-loaded shard.

This procedure provides an approximately balanced initial partition for the subsequent graph optimization stages.

### Stage 1 — Louvain Community Optimization

**Implementations:**

- `supervisor/committee/PLouvainCommittee.go` → `louvainOptimization()`
- `partition/PLouvain.go` → `phase0_AggressiveLouvain()`

The optimization follows these rules:

- The committee-level optimization executes at most **5 rounds**.
- The partition-level optimization executes at most **30 rounds**.
- For each account, the algorithm calculates the total edge weight between the account and every shard.
- An account is migrated when its connection weight to the target shard exceeds **1.2 times** its connection weight to the current shard.
- Accounts are processed in descending order of degree so that highly connected accounts are optimized first.
- Communities are sorted by their internal-edge ratios, giving priority to highly cohesive communities.

### Stage 2 — Boundary-Node Optimization

**Implementations:**

- `supervisor/committee/PLouvainCommittee.go` → `optimizeBoundaryNodes()`
- `partition/PLouvain.go` → `phase2_AccountMovement()`

A boundary node is an account with at least one neighbor assigned to another shard.

During this stage:

- The algorithm identifies all boundary nodes.
- For each boundary node, it calculates the connection weight to each neighboring shard.
- The node is migrated if the connection weight to the best neighboring shard exceeds **1.5 times** the connection weight to its current shard.
- The committee-level optimization executes at most **3 rounds**.
- The partition-level optimization executes at most **5 rounds**.
- Optimization terminates early if no account can be migrated.

This stage directly reduces the number and proportion of cross-shard edges.

### Stage 3 — Final Load Balancing

**Implementation:**

`supervisor/committee/PLouvainCommittee.go` → `finalLoadBalancing()`

The final stage balances transaction workloads across shards:

1. Use the number of intra-shard transactions as the shard-load metric.
2. Identify the most heavily loaded and least heavily loaded shards.
3. Select the account with the largest number of cross-shard edges from the most heavily loaded shard.
4. Migrate the selected account to the least heavily loaded shard.
5. Repeat the process until the maximum shard load is lower than **1.3 times** the average shard load.

Selecting accounts with many cross-shard edges increases the likelihood that migration will improve both workload balance and cross-shard communication overhead.

---

## Getting Started

### Prerequisites

- **Go 1.18 or later**
- Linux, macOS, or Windows
- The XBlock-ETH transaction dataset described in `dataset/README.md`

### 1. Clone the Repository

```bash
git clone https://gitee.com/jin-r/BuptBlockEmulator.git
cd BuptBlockEmulator
```

### 2. Download the Dataset

Follow the instructions in [`dataset/README.md`](./dataset/README.md) to download the XBlock-ETH transaction dataset.

Extract or place the transaction data under the `dataset/` directory. The complete dataset and locally sampled files are not included in this repository.

### 3. Configure the Experiment

Edit `paramsConfig.json`. An example configuration is shown below:

```json
{
  "ConsensusMethod": 4,
  "ShardNum": 4,
  "NodeNumPerShard": 4,
  "DatasetFile": "./dataset/0to999999_BlockTransaction.csv",
  "TotalDataNum": 300000,
  "BatchSize": 2000,
  "InjectSpeed": 5000,
  "BlockSize": 2000,
  "BlockInterval": 5000,
  "ExpDataRootDir": "./expTest/"
}
```

> **Important:** Set `"ConsensusMethod": 4` to enable `PLouvainCommittee`. With this option enabled, the system automatically executes the improved P-Louvain partitioning pipeline before transaction injection.

For distributed deployment, edit `ipTable.json` and configure the IP address of each node.

### Configuration Parameters

| Parameter | Description |
|---|---|
| `ConsensusMethod` | Consensus or committee mode. Set it to `4` to enable `PLouvainCommittee`. |
| `ShardNum` | Number of shards. |
| `NodeNumPerShard` | Number of consensus nodes in each shard. |
| `DatasetFile` | Path to the input transaction dataset. |
| `TotalDataNum` | Number of transactions loaded for the experiment. |
| `BatchSize` | Number of transactions in each injection batch. |
| `InjectSpeed` | Transaction injection rate. |
| `BlockSize` | Maximum number of transactions in a block. |
| `BlockInterval` | Block-generation interval. |
| `ExpDataRootDir` | Directory used to store experimental results. |

### 4. Generate the Launch Scripts

On Linux or macOS:

```bash
go run main.go -g
```

On Windows:

```powershell
.\blockEmulator_Windows_Precompile.exe -g
```

### 5. Start the Shard Nodes

On Linux or macOS:

```bash
bash ./shell/run.sh
```

On Windows:

```powershell
.\shell\run.bat
```

A node can also be started manually. For example:

```bash
go run main.go -n 0 -N 4 -s 0 -S 4 -c
```

| Argument | Description |
|---|---|
| `-n` | Node ID within a shard. |
| `-N` | Number of nodes in each shard. |
| `-s` | Shard ID. |
| `-S` | Total number of shards. |
| `-c` | Start the process as a consensus node. |

### 6. Start the Supervisor

After all shard nodes have been started, launch the supervisor:

```bash
go run main.go -S 4 -N 4 -s -1 -n 0
```

The supervisor invokes `PLouvainCommittee.MsgSendingControl()`, which executes the following workflow:

1. Load transactions from the CSV dataset.
2. Construct the weighted account-interaction graph.
3. Execute the four-stage improved P-Louvain algorithm.
4. Generate the account-to-shard mapping.
5. Dispatch transactions to the corresponding shards according to the partitioning result.

---

## Experimental Outputs

Experimental results are written to the directory specified by `ExpDataRootDir`. The default output directory is:

```text
./expTest/
```

### Standard BlockEmulator Metrics

| Output file | Metric |
|---|---|
| `TPS_Relay.csv` | System throughput in transactions per second. |
| `Latency_Relay.csv` | Transaction confirmation latency. |
| `TxPool_Relay.csv` | Transaction-pool queue length. |

### BLBChain-Specific Statistics

During the P-Louvain partitioning process, the system prints statistics similar to the following:

```text
=== P-Louvain Stats ===
Shard 0: Nodes=312, InternalTxs=18423 (24.6% of total)
Shard 1: Nodes=298, InternalTxs=19102 (25.5% of total)
Shard 2: Nodes=305, InternalTxs=18876 (25.2% of total)
Shard 3: Nodes=317, InternalTxs=18599 (24.8% of total)
Cross-Shard Transactions: 0 / 75000 (0.00%)
=======================
```

The main evaluation metrics are listed below:

| Metric | Description | Optimization objective |
|---|---|---|
| **Intra-shard transactions** | Number of transactions whose sender and receiver are assigned to the same shard. | Keep workloads approximately balanced across shards. |
| **Cross-shard transaction ratio** | Fraction of transactions involving accounts assigned to different shards. | Lower is better. |
| **Cross-shard edge ratio** | Fraction of graph edges connecting accounts in different shards. | Lower is better. |
| **Shard processing time** | Ratio between the shard workload and its effective processing capacity. | Minimize processing-time variance across shards. |
| **System throughput** | Number of committed transactions per second. | Higher is better. |
| **Confirmation latency** | Time required for an injected transaction to be confirmed. | Lower is better. |

---

## Reproducing Experiments

To reproduce an experiment:

1. Download the dataset and set `DatasetFile` to the correct CSV path.
2. Configure the shard count, node count, transaction count, injection rate, block size, and block interval in `paramsConfig.json`.
3. Set `"ConsensusMethod": 4`.
4. Generate the launch scripts.
5. Start all shard nodes.
6. Start the supervisor after the shard nodes are ready.
7. Collect the CSV result files from `ExpDataRootDir`.
8. Record the P-Louvain partitioning statistics printed by the supervisor.

For a fair comparison between different account-allocation methods, keep the following parameters unchanged across experiments:

- Input transaction dataset;
- Number of input transactions;
- Number of shards;
- Number of nodes per shard;
- Transaction injection rate;
- Block size;
- Block interval;
- Hardware and network configuration.

---

## Development Status

| Component | Status |
|---|---|
| BlockEmulator-based implementation | Available |
| Network-layer transaction pre-distribution | Implemented |
| Pressure-aware account allocation | Implemented |
| Improved P-Louvain partitioning | Implemented |
| Experimental evaluation on BlockEmulator | Completed |
| ChainMaker consensus-layer design | Completed |
| ChainMaker transaction-layer sharding | In progress |

---

## Repository Notes

- The full XBlock-ETH dataset is not included because of its size.
- Locally generated dataset samples are not included.
- Experiment output files are generated under `ExpDataRootDir`.
- The current ChainMaker port is under active development.