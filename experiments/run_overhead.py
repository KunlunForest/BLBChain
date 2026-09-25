"""Run isolated, opt-in PBFT overhead trials. Python 3.8+, standard library only."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
VARIANTS = {"A": (False, False), "B": (True, False), "C": (False, True), "D": (True, True)}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def synthetic(path, total, shards, seed):
    """Deterministic synthetic smoke workload, NOT paper evaluation data."""
    rng = random.Random(seed)
    accounts = [f"{i:040x}" for i in range(1, 129)]
    hot = [a for a in accounts if int(a, 16) % shards == 0][:8]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["index", "unused", "sender", "recipient", "value"])
        for i in range(total):
            sender = rng.choice(hot if rng.random() < 0.8 else accounts)
            recipient = rng.choice(accounts)
            while recipient == sender:
                recipient = rng.choice(accounts)
            w.writerow([i, "", sender, recipient, 1])


def prepare_dataset(source, target, total, sender, recipient, value):
    with open(source, newline="", encoding="utf-8-sig") as src, open(target, "w", newline="", encoding="utf-8") as dst:
        r, w = csv.reader(src), csv.writer(dst)
        next(r)
        w.writerow(["index", "unused", "sender", "recipient", "value"])
        for i in range(total):
            row = next(r, None)
            if row is None:
                raise ValueError(f"Dataset has fewer than {total} rows")
            a, b = row[sender].strip().lower(), row[recipient].strip().lower()
            a = a[2:] if a.startswith("0x") else a
            b = b[2:] if b.startswith("0x") else b
            if len(a) != 40 or len(b) != 40:
                raise ValueError(f"Invalid address at input row {i + 2}; preprocess/filter the input first")
            int(a, 16), int(b, 16)
            amount = int(row[value])
            if amount < 0:
                raise ValueError("Negative value")
            w.writerow([i, "", a, b, amount])


def address_table(shards, nodes):
    # Ask the OS for available ports. Close reservations immediately before launch.
    reservations, table = [], {}
    try:
        for s in list(range(shards)) + [2147483647]:
            table[str(s)] = {}
            for n in range(nodes if s != 2147483647 else 1):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
                table[str(s)][str(n)] = f"127.0.0.1:{sock.getsockname()[1]}"
        return table
    finally:
        for sock in reservations:
            sock.close()


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def run_trial(binary, folder, config, shards, nodes, timeout):
    folder.mkdir()
    write_json(folder / "paramsConfig.json", config)
    table = address_table(shards, nodes)
    write_json(folder / "ipTable.json", table)
    processes, logs = [], []
    deadline = time.monotonic() + timeout + 30
    try:
        def launch(name, args):
            log = open(folder / f"{name}.log", "w", encoding="utf-8")
            logs.append(log)
            flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            p = subprocess.Popen([str(binary), *args], cwd=folder, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
            processes.append(p)
            return p

        for s in range(shards):
            for n in range(nodes):
                launch(f"s{s}_n{n}", ["-s", str(s), "-n", str(n), "-S", str(shards), "-N", str(nodes)])
        # Readiness checks never start a second instance or kill unrelated processes.
        for s in range(shards):
            for n in range(nodes):
                port = int(table[str(s)][str(n)].split(":")[1])
                while True:
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError("Node exited during startup; inspect node logs")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                            break
                    except OSError:
                        if time.monotonic() > deadline:
                            raise TimeoutError("Node startup timed out")
                        time.sleep(0.1)
        supervisor = launch("supervisor", ["-c", "-S", str(shards), "-N", str(nodes)])
        while supervisor.poll() is None:
            if any(p.poll() is not None and p.returncode != 0 for p in processes):
                raise RuntimeError("Process failed; inspect logs")
            if time.monotonic() > deadline:
                raise TimeoutError("Trial timed out; partial output is not a valid result")
            time.sleep(0.2)
        if supervisor.returncode != 0:
            raise RuntimeError("Supervisor failed; inspect supervisor.log")
        for p in processes:
            p.wait(timeout=max(1, deadline - time.monotonic()))
            if p.returncode != 0:
                raise RuntimeError("Node exited with nonzero status")
        return {"status": "complete"}
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
        for p in processes:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for log in logs:
            log.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--smoke", action="store_true", help="120 synthetic transactions, two shards, one bandwidth and one repeat")
    p.add_argument("--dataset", type=Path)
    p.add_argument("--sender-column", type=int, default=2)
    p.add_argument("--recipient-column", type=int, default=3)
    p.add_argument("--value-column", type=int, default=4)
    p.add_argument("--total", type=int, default=6000)
    p.add_argument("--rate", type=int, default=200)
    p.add_argument("--batch", type=int, default=20)
    p.add_argument("--block-size", type=int, default=100)
    p.add_argument("--block-ms", type=int, default=500)
    p.add_argument("--shards", type=int, default=4)
    p.add_argument("--nodes", type=int, default=4)
    p.add_argument("--bandwidth-mbps", type=float, nargs="+", default=[1, 5, 10, 20])
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--variants", nargs="+", choices=VARIANTS, default=list(VARIANTS))
    p.add_argument("--migration-after", type=int, default=10)
    p.add_argument("--migration-accounts", type=int, default=4)
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--seed", type=int, default=12345)
    p.add_argument("--skip-stream-node", type=int, default=-1, help="Deliberately omit one replica from streaming to test recovery")
    p.add_argument("--binary", type=Path, help="Use an already built executable")
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    if args.smoke:
        args.total, args.rate, args.batch = 120, 20, 5
        args.shards, args.nodes = 2, 4
        args.block_size, args.block_ms = 20, 300
        args.bandwidth_mbps, args.repeats = [10], 1
        args.migration_after, args.migration_accounts = 2, 2
    if min(args.total,args.rate,args.batch,args.block_size,args.block_ms,args.shards,args.nodes,args.repeats,args.timeout,args.migration_accounts) <= 0 or min(args.bandwidth_mbps) <= 0:
        p.error("Counts, intervals, rates and bandwidths must be positive")
    if args.nodes < 4:
        p.error("Use at least four PBFT nodes per shard")
    if any(VARIANTS[v][1] for v in args.variants) and (args.shards < 2 or args.migration_after <= 0 or args.total / args.rate <= args.migration_after + args.batch / args.rate):
        p.error("Migration requires >=2 shards and sufficient workload after its trigger")
    if args.skip_stream_node != -1 and not 0 < args.skip_stream_node < args.nodes:
        p.error("--skip-stream-node must be a replica ID in [1,nodes), or -1")
    if not args.smoke and args.dataset is None:
        p.error("Supply --dataset for evaluation, or use --smoke for synthetic validation")
    output = (args.output or ROOT / "experiments" / "runs" / time.strftime("%Y%m%d-%H%M%S")).resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve() if args.binary else output / ("blockEmulator.exe" if os.name == "nt" else "blockEmulator")
    if not args.binary:
        subprocess.run(["go", "build", "-o", str(binary), "."], cwd=ROOT, check=True)
    data = output / "input.csv"
    if args.dataset:
        prepare_dataset(args.dataset, data, args.total, args.sender_column, args.recipient_column, args.value_column)
    else:
        synthetic(data, args.total, args.shards, args.seed)
    source_hashes = {str(f.relative_to(ROOT)): sha256(f) for f in ROOT.rglob("*.go") if "runs" not in f.parts}
    metadata = {"args": {k: str(v) if isinstance(v, Path) else v for k,v in vars(args).items()}, "platform": platform.platform(), "python": sys.version, "cpu_count": os.cpu_count(), "go_version": subprocess.check_output(["go","version"], text=True).strip(), "dataset_sha256": sha256(data), "binary_sha256": sha256(binary), "source_sha256": source_hashes, "workload": "dataset" if args.dataset else "synthetic_smoke", "migration_policy": "one controlled rotation of most frequently injected senders; global CLPA barrier"}
    write_json(output / "manifest.json", metadata)
    cases = [(bw, rep, v) for bw in args.bandwidth_mbps for rep in range(1,args.repeats+1) for v in args.variants]
    random.Random(args.seed).shuffle(cases)
    failed = False
    for index,(bw,rep,variant) in enumerate(cases,1):
        light,migrate = VARIANTS[variant]
        folder = output / f"{variant}_bw{bw:g}_r{rep}"
        config = json.loads((ROOT / "paramsConfig.json").read_text(encoding="utf-8-sig"))
        config.update(ConsensusMethod=5, ExpDataRootDir="results", DatasetFile=data.as_posix(), TotalDataSize=args.total, InjectSpeed=args.rate, TxBatchSize=args.batch, BlockSize=args.block_size, Block_Interval=args.block_ms, UseBlocksizeInBytes=0, Bandwidth=int(bw*1_000_000/8), Delay=0, JitterRange=0, RelayWithMerkleProof=0, PbftViewChangeTimeOut=(args.timeout+60)*1000)
        config["Overhead"] = dict(Enabled=True, Lightweight=light, Migration=migrate, MigrationAfterSeconds=args.migration_after, MigrationAccounts=args.migration_accounts, TimeoutSeconds=args.timeout, StreamSkipNode=args.skip_stream_node)
        print(f"[{index}/{len(cases)}] {folder.name}", flush=True)
        status = run_trial(binary,folder,config,args.shards,args.nodes,args.timeout)
        status.update(variant=variant, bandwidth_mbps=bw, repeat=rep)
        write_json(folder / "status.json",status)
        print(status, flush=True)
        if status["status"] != "complete":
            failed=True
            break
    from summarize_overhead import summarize
    valid = summarize(output)
    print(f"Results: {output}",flush=True)
    return 1 if failed or not valid else 0


if __name__ == "__main__":
    raise SystemExit(main())
