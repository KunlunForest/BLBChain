# BLBChain

Research code accompanying **Beyond Stress-Balanced Sharding: A Cross-Layer Perspective on Throughput Scaling in Blockchain**.

**Authors:** Hao Wu, Rui Jin, Yebo Feng, Yu Liu, Konglin Zhu, and Lin Zhang.

BLBChain studies blockchain throughput scaling through joint optimization of transaction dissemination and account allocation. The paper proposes lightweight proposals that move transaction payload dissemination outside the consensus critical path, together with stress-aware account allocation that matches workloads to shard capacity.

This repository builds on BlockEmulator. It includes graph partitioning, PBFT execution, an experimental ExClique path, and an opt-in overhead experiment. **The controlled migration experiment is not the paper's complete adaptive migration policy, and this snapshot is not a validated reproduction package for every result in the manuscript.** Implementation boundaries are described below.

## Repository structure

| Path | Purpose |
| --- | --- |
| `main.go`, `build/` | Entry point, process construction, and launch-script generation. |
| `params/`, `paramsConfig.json`, `ipTable.json` | Experiment parameters and network addresses. |
| `partition/` | P-Louvain, CLPA, and contribution-based partitioning components. |
| `supervisor/committee/` | Partitioning, workload injection, and the controlled overhead experiment. |
| `consensus_shard/pbft_all/` | PBFT, relay, account-state transfer, and opt-in lightweight proposals. |
| `consensus_shard/exclique/` | Experimental ExClique and compact-block components. |
| `core/`, `chain/`, `storage/`, `shard/` | Transaction/block structures, state storage, and node definitions. |
| `networks/`, `message/`, `utils/` | Communication, message formats, and shared helpers. |
| `broker/`, `supervisor/measure/` | Inherited components still referenced by the compiled emulator. |
| `experiment/` | Raw communication, event, transaction, and memory measurements. |
| `experiments/` | Experiment runner, validation, CSV aggregation, and plotting. |
| `dataset/download.py` | Dataset acquisition helper. |

Shared emulator components are retained where referenced by the code. Their presence does not imply that every method is selectable or evaluated by the paper's current entry point.

## Requirements

- Go **1.19 or later**, as declared in `go.mod`.
- Python **3.8 or later** for the experiment runner and CSV analysis.
- Access to dependencies pinned in `go.mod` and `go.sum` on the first build.
- Matplotlib only for plotting; the runner and CSV aggregation use the Python standard library.
- Memory and disk space for per-node databases, logs, transaction caches, and input data.

Run commands from the repository root. Use `python3` instead of `python` where appropriate on Linux/macOS. The overhead runner creates a fresh working directory, address table, database, and configuration for every trial, without overwriting the root configuration. It terminates only processes it launches.

## Quick start: functional validation

```bash
python experiments/run_overhead.py --smoke
```

This builds the executable, generates a deterministic synthetic workload, and runs four configurations with two shards, four nodes per shard, 120 transactions, and a 10 Mbps per-node upload limit. Trial order is shuffled with a fixed seed.

**Synthetic smoke results validate functionality; they are not paper evaluation data.** A nonzero exit code indicates a failed run or failed result validation.

| Configuration | Predissemination and lightweight proposals | Controlled migration |
| --- | --- | --- |
| A | Disabled | Disabled |
| B | Enabled | Disabled |
| C | Disabled | Enabled |
| D | Enabled | Enabled |

All configurations use the same PBFT/CLPA execution path, initial address-to-shard hashing, input order, and scheduled arrival rate. The overhead experiment bypasses the original static P-Louvain entry point. It must not be combined with the legacy `-e` ExClique option.

## Dataset preparation

The project uses XBlock-ETH Block Transaction data. Obtain the data through the [XBlock-ETH project](https://github.com/InPlusLab/XBlock-ETH) or the [dataset page](https://xblock.pro/#/dataset/14). The full dataset and local samples are not distributed with this repository. The optional interactive downloader is `dataset/download.py` and requires the Python `requests` package; choose its Block Transaction option if using it.

The overhead runner requires a CSV header and uses zero-based columns `2`, `3`, and `4` for sender, recipient, and transaction value by default. Check the downloaded file's actual schema. Use `--sender-column`, `--recipient-column`, and `--value-column` to select different positions.

Addresses are normalized to lowercase, 40-character hexadecimal strings; an optional `0x` prefix is removed. Values must be nonnegative decimal integers and are preserved without floating-point conversion. Invalid records, including empty contract-creation recipient addresses, are rejected rather than silently filtered. Preprocess the input as needed and retain the preprocessing procedure, selected range, and source checksum.

The selected normalized input is copied to each experiment suite's `input.csv`, and its SHA-256 is saved in the manifest. The original non-experiment entry point instead expects `DatasetFile` in `paramsConfig.json`, currently `./dataset/0to999999_BlockTransaction.csv`.

## Run experiments with transaction data

Start with a small input to check the schema and execution:

```bash
python experiments/run_overhead.py --dataset dataset/your_transactions.csv --total 2000 --rate 100 --batch 10 --bandwidth-mbps 10 --repeats 1 --migration-after 5
```

Example bandwidth sweep with repeated trials:

```bash
python experiments/run_overhead.py --dataset dataset/your_transactions.csv --total 60000 --rate 1000 --batch 100 --block-size 500 --block-ms 1000 --shards 4 --nodes 4 --bandwidth-mbps 1 5 10 20 --repeats 3 --migration-after 20 --migration-accounts 20 --timeout 1800
```

These values illustrate command usage; they are not the paper's experimental settings or tuned recommendations. Use the paper's workload, topology, and protocol settings for evaluation. Record CPU model, RAM, operating system, and concurrent host load in addition to the automatically recorded environment metadata.

Useful options:

| Option | Meaning |
| --- | --- |
| `--total`, `--rate`, `--batch` | Input transaction count, scheduled arrivals per second, and injection batch size. |
| `--block-size`, `--block-ms` | Transactions per block and block interval in milliseconds. |
| `--shards`, `--nodes` | Shards and PBFT nodes per shard; use at least four nodes per shard. |
| `--bandwidth-mbps` | Per-process shared upload limits in decimal Mbps; converted to bytes/second. |
| `--repeats`, `--seed` | Repetitions and seed for trial order and synthetic inputs. |
| `--variants B D` | Run only the selected configurations. |
| `--migration-after` | Trigger a single controlled migration after this many seconds. |
| `--migration-accounts` | Maximum number of active sender accounts selected for migration. |
| `--timeout` | Trial timeout in seconds; allow enough time for workload drain. |
| `--binary PATH` | Reuse an executable instead of compiling one. |
| `--output PATH` | Write into a new, nonexistent output directory. |
| `--skip-stream-node 1` | Omit replica 1 from streaming to deliberately exercise missing-body recovery. |

Use `python experiments/run_overhead.py --help` for all arguments. Do not interpret timeouts as valid low-throughput or zero-latency results.

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

Follow the instructions in [`dataset-preparation`](README.md#dataset-preparation) to download the XBlock-ETH transaction dataset.

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

---

## Communication accounting

In lightweight mode, a leader caches immutable transaction encodings and queues them for asynchronous dissemination to shard replicas. A proposal carries the encoded block header, content identifiers, and the original request digest. Replicas reconstruct transaction bodies from their caches; missing bodies are retrieved through actual request/response messages. The reconstructed request must match its original digest before entering the PBFT validation path.

Identifiers use SHA-256 over transaction encodings to distinguish original and relayed representations. The cache currently retains bodies until process exit; eviction is not implemented, and its memory consumption is part of this implementation's cost.

**All outgoing message classes share a single upload limiter per process.** Predissemination, consensus, recovery, cross-shard messages, and migration do not receive separate full bandwidth budgets. The existing connection pool serializes writes within each process. The supervisor uses the same configured upload limit. This emulates upload competition, not a separately constrained receive link or a full WAN topology.

| Category | Included traffic |
| --- | --- |
| `stream` | Transaction-body predissemination. |
| `consensus` | Ordinary proposals and prepare/commit messages. |
| `recovery` | Missing-body requests/responses and historical-request retrieval. |
| `cross_shard` | Relay traffic, including empty sequence-synchronization relays. |
| `migration` | Partition instructions, readiness, state/pending-transaction transfers, migration proposals and their prepare/commit votes, and application acknowledgments. |
| `injection` | Supervisor-to-node transaction injection. |
| `control` | Block reports, shutdown, and other experiment control messages. |
| `other` | Unclassified messages; inspect these before interpreting results. |

Byte counters measure actual application bytes written to sockets, including message framing and line terminators. Broadcast sends count once per recipient, and partial writes on failed attempts are counted. Received bytes are not added a second time. Counts include the emulator's localhost/self messages but exclude TCP/IP headers, kernel retransmissions, and link-layer traffic. Report these as **application-layer transmitted bytes**, not packet-capture wire totals.

Active-window counts include writes completed from the first scheduled arrival to the last final transaction confirmation. A message spanning a boundary is attributed entirely to its write-completion time; this is not per-byte time integration. Separate lifecycle counters include startup and shutdown traffic.

`protocol_bytes_per_tx` includes stream, consensus, recovery, cross-shard, and migration traffic. `total_bytes_per_tx` includes every category. Both use the number of unique finally confirmed original transactions; cross-shard execution is not counted twice.

To validate recovery explicitly:

```bash
python experiments/run_overhead.py --smoke --variants B D --skip-stream-node 1
```

This is a deliberate missing-body test, not a stochastic packet-loss model. Keep `--skip-stream-node -1` for ordinary experiments.

## Migration and performance measurements

Each migration-enabled trial performs one controlled migration. Senders are ranked by their observed injected transaction frequency, with addresses breaking ties. The top N accounts move to `(current shard + 1) mod shard count`. The selected accounts and observations are recorded in `supervisor_migration_plan.csv`. This is a cost probe, not a stress-minimizing policy, and it may improve or worsen load distribution.

The supervisor pauses routing during the existing CLPA barrier, state transfer, and PBFT confirmation. Injection resumes after all replicas acknowledge state application. **Scheduled arrivals continue advancing during this pause**, so transactions arriving during migration retain their queueing delay in the end-to-end latency measurement.

| Metric | Definition |
| --- | --- |
| `migration_ms` | Migration request to state-application acknowledgments from all replicas. |
| `shard_pause_p95_ms` | P95 across shard leaders of the interval from entering the migration barrier to local migration application. |
| `tps` | Unique final confirmations divided by time from the first scheduled arrival to the last final confirmation. |
| `latency_ms` | Scheduled arrival to final-shard confirmation, including batching, bandwidth competition, migration waiting, and cross-shard processing. |
| `max_node_go_heap_bytes` | Maximum sampled single-node Go heap allocation, sampled once per second; not process RSS or CPU utilization. |

**Shard-wide migration pause is not per-account fine-grained locking delay.** Do not label it as such in the paper. The measured interval includes migration and workload drain; it does not exclude unfavorable periods.

The runner sets view-change timeout beyond the trial timeout and tests fixed-leader, failure-free execution. Byzantine behavior, leader failures, and membership changes are outside these experiments.

## Outputs and validation

```text
experiments/runs/<timestamp>/
  manifest.json                  # Arguments, environment, source/binary/input hashes
  input.csv                      # Selected normalized workload
  summary.csv                    # Validated individual trials
  aggregate.csv                  # Means and sample standard deviations
  invalid_trials.csv             # Excluded failures and validation errors
  A_bw10_r1/
    paramsConfig.json
    ipTable.json
    status.json
    supervisor.log
    s0_n0.log
    timeseries.csv
    results/overhead/             # Raw byte/event/transaction/resource/account records
```

A valid trial requires normal process exits, unique final confirmation of every input transaction, complete nonce coverage, no failed socket writes in the measurement window, and actual migration events in migration-enabled trials. Every replica exports its owned account balances at shutdown. The summarizer independently replays CSV balance changes and checks replica balances and post-migration ownership. This validates the emulator's transfer model, not arbitrary smart-contract execution.

`timeseries.csv` provides throughput, P95 latency of transactions completed in each window, pending transactions, and migration markers. A partial final window uses its actual duration. Windows without confirmations have blank latency, not zero latency.

Investigate failed trials through `invalid_trials.csv` and process logs; do not silently drop failures from published comparisons. Recompute summaries with:

```bash
python experiments/summarize_overhead.py experiments/runs/YOUR_RUN
```

## Plot results

```bash
python -m pip install -r experiments/requirements-plot.txt
python experiments/plot_overhead.py experiments/runs/YOUR_RUN
```

The `figures/` directory contains PNG and PDF versions of communication-volume breakdowns, throughput/latency comparisons, and migration timelines. Performance error bars show sample standard deviation across repetitions, not confidence intervals. Timeline figures explicitly use repeat 1 for each bandwidth instead of averaging differently timed migration intervals. Synthetic smoke figures are marked as non-evaluation data.

## Original entry point

To build and inspect the original emulator:

```bash
go mod download
go build -o blockEmulator .
./blockEmulator --help
```

On Windows, build with `go build -o blockEmulator.exe .` and run `.\blockEmulator.exe --help`. Even `--help` reads the root configuration. The Bash scripts `start_all.sh` and `start_exclique.sh` use a fixed four-shard/four-node localhost topology. The latter selects the legacy experimental ExClique path, not the new PBFT overhead experiment.

A node command is `./blockEmulator -n 0 -N 4 -s 0 -S 4`; the supervisor command is `./blockEmulator -c -N 4 -S 4`. **`-c` means supervisor.** Topology is set with `-S` and `-N`, not additional JSON fields. Provide matching endpoints in `ipTable.json`; special shard `2147483647`, node `0`, is the supervisor.

The original entry point has these limitations:

- It passes the literal `PLouvain` to constructors. The PBFT constructor falls back to relay modules; `ConsensusMethod` is not a general working baseline selector here.
- Checked-in `ConsensusMethod=4` requests unregistered metric names. `5` selects the implemented relay metric modules, but does not repair the other legacy limitations.
- The active P-Louvain committee computes an initial partition without connecting capacity feedback to periodic migration. It keeps its partition locally and selects an injection target by Go map iteration, which is not guaranteed to select the leader.
- Its original CSV loader reads the entire input into memory and parses values through floating-point conversion; original transaction construction also requires hash/timestamp validation.
- Legacy ExClique hash reconstruction remains incomplete and lacks standard supervisor reporting. The new PBFT experiment has its own reconstruction and measurement path.

These limitations must not be confused with the separately validated controlled experiment. Neither path establishes all mechanisms or security properties described in the manuscript.

## Development checks

```bash
go test ./networks ./supervisor/committee ./consensus_shard/pbft_all ./experiment
go vet ./networks ./supervisor/committee ./consensus_shard/pbft_all ./experiment
python -m unittest discover -s experiments -p test_tools.py
```

Functional checks were completed on Windows/amd64 with Go 1.25.4 and Python 3.8. Synthetic tests covered A/B/C/D on two shards at 10 Mbps and four shards at 1 Mbps, deliberate missing-body recovery, a congested four-shard D run at 0.1 Mbps, and independent replica balance/ownership audits. These checks are not large-scale paper benchmarks. Linux/macOS runtime behavior and the paper's adaptive migration policy were not validated in those checks.

## Citation and provenance

Please cite the accompanying manuscript when using this code. Publication metadata is intentionally omitted until a final bibliographic record is available.

```bibtex
@unpublished{wu_blbchain,
  title  = {Beyond Stress-Balanced Sharding: A Cross-Layer Perspective on Throughput Scaling in Blockchain},
  author = {Wu, Hao and Jin, Rui and Feng, Yebo and Liu, Yu and Zhu, Konglin and Zhang, Lin},
  note   = {Research manuscript}
}
```

This implementation builds on BlockEmulator by HuangLab-SYSU. Please acknowledge the underlying emulator and dataset when using them. The XBlock-ETH dataset attribution is:

```bibtex
@article{zheng2020xblock,
  title   = {XBlock-ETH: Extracting and Exploring Blockchain Data from Ethereum},
  author  = {Zheng, Peilin and Zheng, Zibin and Wu, Jiajing and Dai, Hong-ning},
  journal = {IEEE Open Journal of the Computer Society},
  year    = {2020},
  volume  = {1},
  pages   = {95--106},
  doi     = {10.1109/OJCS.2020.2990458}
}
```

## License

The [MIT License](LICENSE) retains the upstream HuangLab-SYSU copyright notice. Dependencies and datasets have their own licensing terms.
