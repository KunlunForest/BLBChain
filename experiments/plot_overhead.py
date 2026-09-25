"""Plot measured overhead results (requires matplotlib). Never plots invalid trials."""
import argparse
import csv
import json
from pathlib import Path
import statistics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

LABELS = {"A":"Full proposal", "B":"Lightweight", "C":"Full + migration", "D":"Lightweight + migration"}
CATEGORIES = ["stream", "consensus", "recovery", "cross_shard", "migration", "injection", "control", "other"]
COLORS = ["#2878b5", "#9ac9db", "#f8ac8c", "#c82423", "#ffbe7a", "#c2c2c2", "#8d8d8d", "#9467bd"]


def read(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def mean_sd(values):
    return statistics.mean(values), statistics.stdev(values) if len(values)>1 else 0


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("results",type=Path)
    args=p.parse_args()
    root=args.results
    if (root/"invalid_trials.csv").exists() and read(root/"invalid_trials.csv"):
        raise SystemExit("Failed/invalid trials exist. Resolve them before generating comparison figures.")
    data=read(root/"summary.csv")
    if not data:
        raise SystemExit("No validated results")
    manifest=json.loads((root/"manifest.json").read_text())
    synthetic=manifest["workload"]=="synthetic_smoke"
    footer="Synthetic smoke test — not paper evaluation" if synthetic else "Controlled migration experiment; error bars show sample standard deviation"
    target=root/"figures";target.mkdir(exist_ok=True)
    bandwidths=sorted({float(r["bandwidth_mbps"]) for r in data})
    variants=sorted({r["variant"] for r in data})
    def group(v,bw):return [r for r in data if r["variant"]==v and float(r["bandwidth_mbps"])==bw]
    def save(fig,name):
        fig.text(.5,.01,footer,ha="center",fontsize=8)
        fig.tight_layout(rect=(0,.04,1,.97))
        fig.savefig(target/(name+".png"),dpi=200)
        fig.savefig(target/(name+".pdf"))
        plt.close(fig)
    fig,axes=plt.subplots(1,len(bandwidths),figsize=(max(5,3.4*len(bandwidths)),4.7),squeeze=False)
    for ax,bw in zip(axes[0],bandwidths):
        base=[0.0]*len(variants)
        for cat,color in zip(CATEGORIES,COLORS):
            heights=[statistics.mean(float(r[cat+"_bytes_per_tx"])/1024 for r in group(v,bw)) if group(v,bw) else 0 for v in variants]
            ax.bar(variants,heights,bottom=base,label=cat,color=color)
            base=[x+y for x,y in zip(base,heights)]
        ax.set_title(f"{bw:g} Mbps per node")
        ax.set_ylim(0,max(base)*1.12 if max(base)>0 else 1)
        ax.set_ylabel("Application bytes / confirmed tx (KiB)")
        ax.set_xlabel("Configuration")
    axes[0][-1].legend(fontsize=7,loc="upper left",bbox_to_anchor=(1,1))
    save(fig,"communication_overhead")
    fig,axes=plt.subplots(1,2,figsize=(10,4.5))
    for ax,metric,ylabel in zip(axes,["tps","p95_latency_ms"],["End-to-end throughput (tx/s)","P95 confirmation latency (ms)"]):
        for v in variants:
            xs,ys,errors=[],[],[]
            for bw in bandwidths:
                values=[float(r[metric]) for r in group(v,bw)]
                if values:
                    mean,sd=mean_sd(values);xs.append(bw);ys.append(mean);errors.append(sd)
            ax.errorbar(xs,ys,yerr=errors,marker="o",capsize=3,label=f"{v}: {LABELS[v]}")
        ax.set_xlabel("Per-node upload limit (Mbps)");ax.set_ylabel(ylabel);ax.grid(alpha=.2)
    axes[0].legend(fontsize=7)
    save(fig,"end_to_end_performance")
    # Each bandwidth has its own time series. Repeat 1 is explicitly identified;
    # this is not a pointwise average of differently timed migration intervals.
    for bw in bandwidths:
        selected=[r for r in data if float(r["bandwidth_mbps"])==bw and int(r["repeat"])==1]
        fig,axes=plt.subplots(2,1,figsize=(9,6),sharex=True)
        for r in selected:
            v=r["variant"];series=read(root/r["trial"]/"timeseries.csv")
            xs=[float(s["second"])+float(s["window_s"])/2 for s in series]
            axes[0].plot(xs,[float(s["tps"]) for s in series],label=f"{v}: {LABELS[v]}")
            axes[1].plot(xs,[float(s["p95_latency_ms"]) if s["p95_latency_ms"] else float("nan") for s in series])
            if v in ("C","D"):
                ev={e["event"]:int(e["unix_ns"]) for e in read(root/r["trial"]/"results/overhead/supervisor_events.csv")}
                left=(ev["migration_requested"]-ev["workload_start"])/1e9
                right=(ev["migration_complete"]-ev["workload_start"])/1e9
                for ax in axes:
                    ax.axvspan(left,right,alpha=.10,color="red" if v=="D" else "green")
                axes[0].text((left+right)/2,.85 if v=="D" else .94,v+" migration",transform=axes[0].get_xaxis_transform(),ha="center",fontsize=8,bbox=dict(facecolor="white",alpha=.8,edgecolor="none"))
        axes[0].set_ylabel("Throughput (tx/s)");axes[0].legend(fontsize=7,loc="upper right")
        axes[0].set_title(f"{bw:g} Mbps, repeat 1; shaded intervals are migration barriers",pad=25)
        axes[1].set_ylabel("P95 latency (ms)");axes[1].set_xlabel("Seconds from first scheduled arrival")
        for ax in axes:ax.grid(alpha=.2)
        save(fig,f"migration_timeline_bw{bw:g}")
    print(target)


if __name__=="__main__":main()
