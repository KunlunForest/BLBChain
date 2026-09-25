"""Create a portable source release; exclude results, datasets, binaries and caches."""
import hashlib
from pathlib import Path
import zipfile

ROOT=Path(__file__).resolve().parents[1]
SKIP_DIRS={".git",".agents",".codex",".tmp_readme_tools","__pycache__","releases","runs","expTest","logs"}


def main():
    out=ROOT/"releases"
    out.mkdir(exist_ok=True)
    archive=out/"BLBChain-paper-source.zip"
    if archive.exists():
        raise SystemExit("Archive already exists; choose a new release name before packaging")
    with zipfile.ZipFile(archive,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for path in sorted(ROOT.rglob("*")):
            rel=path.relative_to(ROOT)
            if not path.is_file() or path.is_symlink() or any(p in SKIP_DIRS for p in rel.parts):
                continue
            if path.name.startswith(".overhead_") or path.suffix.lower() in {".exe",".pyc",".log",".backup",".zip"}:
                continue
            if "dataset" in rel.parts and path.suffix.lower() not in {".md",".txt",".py"}:
                continue
            if path.name in {"blockEmulator","blockEmulato"}:
                continue
            z.write(path,"BLBChain/"+rel.as_posix())
    with zipfile.ZipFile(archive) as z:
        bad=z.testzip()
        if bad:raise RuntimeError("ZIP CRC failed: "+bad)
        for required in ["go.mod","go.sum","main.go","README.md","LICENSE","experiments/run_overhead.py","experiments/plot_overhead.py","supervisor/committee/committee_overhead.go","consensus_shard/pbft_all/overhead.go"]:
            assert "BLBChain/"+required in z.namelist(),required
        print("Entries:",len(z.namelist()))
    checksum=hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_suffix(".zip.sha256").write_text(checksum+"  "+archive.name+"\n",encoding="ascii")
    print(archive)
    print("SHA256:",checksum)


if __name__=="__main__":main()
