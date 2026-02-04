import os
import re
import csv
import json
import time
import uuid
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIG (edit once, then just run: python this_file.py)
# ============================================================
SOURCE = Path(r"D:\Han_demo_sar")
OUT    = Path(r"D:\Han_demo_sar\archives")

# How far to go automatically (recommended: stop before TB folders)
START_BUCKET = "<1 GB"
MAX_BUCKET   = "10 TB+"


# IMPORTANT: set to True only when you're confident
DELETE_AFTER_ARCHIVE = False     # True = delete source folder after successful archive

SKIP_EXISTING_ARCHIVES = True    # True = if OUT\FolderName.tar.zst exists, skip
RETRIES = 2                      # retry count (for transient errors)
SLEEP_BETWEEN_RETRIES_SEC = 2

# zstd settings (parallel)
ZSTD_LEVEL   = 6                 # 1..19 (higher = slower). For GeoTIFFs often 1-3 is enough.
ZSTD_THREADS = 0                 # 0 = all cores; or set e.g. 8, 16

# ============================================================
# Buckets
# ============================================================
BUCKET_ORDER = [
    "<1 GB", "1-10 GB", "10-50 GB", "50-200 GB",
    "200-500 GB", "500 GB-1 TB", "1-10 TB", "10 TB+", "Unknown"
]

def bucketize(gb: float) -> str:
    if gb is None:
        return "Unknown"
    gb = float(gb)
    if gb < 1: return "<1 GB"
    if gb < 10: return "1-10 GB"
    if gb < 50: return "10-50 GB"
    if gb < 200: return "50-200 GB"
    if gb < 500: return "200-500 GB"
    if gb < 1000: return "500 GB-1 TB"
    if gb < 10000: return "1-10 TB"
    return "10 TB+"

# ============================================================
# Helpers
# ============================================================
BYTES_RE = re.compile(r"Bytes\s*:\s*([\d,]+)", re.I)

def ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def bytes_to_gb(b: int) -> float:
    return round(b / (1024 ** 3), 2)

def is_locked(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with open(path, "ab"):
            return False
    except OSError:
        return True

def require_tools():
    tar_path = shutil.which("tar")
    zstd_path = shutil.which("zstd")
    missing = []
    if not tar_path:
        missing.append("tar")
    if not zstd_path:
        missing.append("zstd")
    if missing:
        raise SystemExit(
            "Missing required tool(s) in PATH: "
            + ", ".join(missing)
            + "\n\nFix:\n"
            + "  - Ensure tar.exe is available (Windows often includes it)\n"
            + "  - Install zstd.exe and add it to PATH\n"
        )
    return tar_path, zstd_path

def run_robocopy_total_bytes(folder: Path):
    """
    Fast folder size using robocopy /L summary (Total Bytes).
    Returns int bytes or None.
    """
    cmd = [
        "robocopy",
        str(folder),
        os.environ.get("TEMP", r"C:\Windows\Temp"),
        "/L", "/S", "/BYTES", "/NP", "/NFL", "/NDL", "/NJH"
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    out = (p.stdout or "") + "\n" + (p.stderr or "")

    bytes_line = None
    for line in out.splitlines():
        if line.strip().lower().startswith("bytes"):
            bytes_line = line.strip()
            break
    if not bytes_line:
        return None

    m = BYTES_RE.search(bytes_line)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))

def tar_zst_dir_contents(src_dir: Path, out_tar_zst: Path, level: int = 6, threads: int = 0):
    """
    Create a .tar.zst containing the CONTENTS of src_dir (not the parent folder),
    using: tar -> zstd (parallel with -T).
    """
    out_tar_zst.parent.mkdir(parents=True, exist_ok=True)

    # tar -cf - -C src_dir .
    tar_cmd = ["tar", "-cf", "-", "-C", str(src_dir), "."]

    # zstd -T{threads} -{level} -q -o out_tar_zst
    # threads=0 means all cores for most zstd builds
    zstd_cmd = ["zstd", f"-{int(level)}", f"-T{int(threads)}", "-q", "-o", str(out_tar_zst)]

    # Stream tar stdout into zstd stdin (no temp tar file)
    tar_p = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        zstd_p = subprocess.Popen(zstd_cmd, stdin=tar_p.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Important: allow tar to receive SIGPIPE if zstd exits early
        tar_p.stdout.close()

        zstd_out, zstd_err = zstd_p.communicate()
        tar_err = tar_p.stderr.read()
        tar_rc = tar_p.wait()

        if tar_rc != 0:
            raise RuntimeError(f"tar failed (rc={tar_rc}): {tar_err.decode(errors='ignore').strip()}")
        if zstd_p.returncode != 0:
            raise RuntimeError(f"zstd failed (rc={zstd_p.returncode}): {zstd_err.decode(errors='ignore').strip()}")

    finally:
        # Cleanup pipes
        try:
            if tar_p.stderr:
                tar_p.stderr.close()
        except Exception:
            pass

def total_archives_gb(out_dir: Path) -> float:
    """Sum size of all .tar.zst files in OUT (GB)."""
    total_bytes = 0
    for f in out_dir.glob("*.tar.zst"):
        try:
            total_bytes += f.stat().st_size
        except OSError:
            pass
    return round(total_bytes / (1024**3), 2)


def write_scan_outputs(rows, out_dir: Path, stamp: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    scan_csv  = out_dir / f"scan_{stamp}.csv"
    scan_json = out_dir / f"scan_{stamp}.json"
    scan_txt  = out_dir / f"scan_{stamp}.txt"

    with open(scan_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["folder", "name", "size_gb", "bucket", "status"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    with open(scan_json, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

    with open(scan_txt, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(f"{r['status']}\t{r['bucket']}\t{r['size_gb']}\t{r['folder']}\n")

    return scan_csv

def write_grouped_csv(rows, out_dir: Path, stamp: str):
    out_csv = out_dir / f"grouped_scan_{stamp}.csv"
    order_index = {b: i for i, b in enumerate(BUCKET_ORDER)}

    def sort_key(r):
        b = r.get("bucket") or "Unknown"
        gb = r.get("size_gb")
        gbv = gb if isinstance(gb, (int, float)) else -1
        return (order_index.get(b, 999), -gbv)

    grouped = sorted(rows, key=sort_key)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["bucket", "size_gb", "name", "folder", "status"])
        w.writeheader()
        for r in grouped:
            w.writerow({
                "bucket": r.get("bucket") or "Unknown",
                "size_gb": r.get("size_gb"),
                "name": r.get("name"),
                "folder": r.get("folder"),
                "status": r.get("status"),
            })

    return out_csv

def print_bucket_summary(rows):
    summary = {b: {"Folders": 0, "TotalGB": 0.0} for b in BUCKET_ORDER}
    for r in rows:
        b = r.get("bucket") or "Unknown"
        gb = r.get("size_gb")
        summary.setdefault(b, {"Folders": 0, "TotalGB": 0.0})
        summary[b]["Folders"] += 1
        if isinstance(gb, (int, float)):
            summary[b]["TotalGB"] += float(gb)

    print("\nBucket Summary:")
    print(f"{'Bucket':<14} {'Folders':>8} {'TotalGB':>12}")
    print("-" * 38)
    for b in BUCKET_ORDER:
        if summary[b]["Folders"] == 0:
            continue
        print(f"{b:<14} {summary[b]['Folders']:>8} {summary[b]['TotalGB']:>12.2f}")

def buckets_to_run():
    sb = START_BUCKET if START_BUCKET in BUCKET_ORDER else "<1 GB"
    mb = MAX_BUCKET if MAX_BUCKET in BUCKET_ORDER else "50-200 GB"

    si = BUCKET_ORDER.index(sb)
    mi = BUCKET_ORDER.index(mb)
    if mi < si:
        si, mi = mi, si
    return BUCKET_ORDER[si:mi+1]

# ============================================================
# Main
# ============================================================
def main():
    # Ensure tar + zstd exist
    require_tools()

    stamp = ts()
    OUT.mkdir(parents=True, exist_ok=True)

    if not SOURCE.exists():
        raise SystemExit(f"Source not found: {SOURCE}")

    # subfolders = sorted([p for p in SOURCE.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
    subfolders = sorted(
    [p for p in SOURCE.iterdir() if p.is_dir() and p.resolve() != OUT.resolve()],
    key=lambda p: p.name.lower()
)

    if not subfolders:
        raise SystemExit(f"No subfolders found in: {SOURCE}")

    print(f"Source: {SOURCE}")
    print(f"Out:    {OUT}")
    print(f"Folders: {len(subfolders)}")
    print(f"START_BUCKET: {START_BUCKET}  |  MAX_BUCKET: {MAX_BUCKET}")
    print(f"SKIP_EXISTING_ARCHIVES: {SKIP_EXISTING_ARCHIVES}  |  DELETE_AFTER_ARCHIVE: {DELETE_AFTER_ARCHIVE}")
    print(f"ZSTD_LEVEL: {ZSTD_LEVEL}  |  ZSTD_THREADS: {ZSTD_THREADS} (0=all cores)")
    print()

    # ----------------------------
    # Step 1: SCAN
    # ----------------------------
    scan_rows = []
    for sf in subfolders:
        b = run_robocopy_total_bytes(sf)
        if b is None:
            scan_rows.append({
                "folder": str(sf),
                "name": sf.name,
                "size_gb": None,
                "bucket": "Unknown",
                "status": "SIZE_FAILED"
            })
            print(f"SCAN SIZE_FAILED: {sf.name}")
            continue

        gb = bytes_to_gb(b)
        buck = bucketize(gb)
        scan_rows.append({
            "folder": str(sf),
            "name": sf.name,
            "size_gb": gb,
            "bucket": buck,
            "status": "OK"
        })
        print(f"SCAN OK: {sf.name} ({gb} GB)")

    scan_csv = write_scan_outputs(scan_rows, OUT, stamp)
    grouped_csv = write_grouped_csv(scan_rows, OUT, stamp)
    print_bucket_summary(scan_rows)
    print(f"\nSaved scan:    {scan_csv}")
    print(f"Saved grouped: {grouped_csv}")

        # ----------------------------
    # Total INPUT vs OUTPUT (before/after archiving)
    # ----------------------------
    total_input_gb = round(
        sum(float(r["size_gb"]) for r in scan_rows if isinstance(r.get("size_gb"), (int, float))),
        2
    )
    total_output_gb = total_archives_gb(OUT)
    ratio = round((total_output_gb / total_input_gb), 3) if total_input_gb > 0 else ""
    saved_gb = round((total_input_gb - total_output_gb), 2) if total_input_gb > 0 else ""

    print("\nTOTAL SIZE CHECK (current state):")
    print(f"  INPUT  (folders): {total_input_gb:.2f} GB")
    print(f"  OUTPUT (archives): {total_output_gb:.2f} GB")
    if ratio != "":
        print(f"  Compression ratio: {ratio}")
        print(f"  Saved: {saved_gb:.2f} GB")

    # write a tiny summary csv
    summary_csv = OUT / f"input_vs_output_{stamp}.csv"
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source", "out", "total_input_gb", "total_output_gb", "compression_ratio", "saved_gb"])
        w.writeheader()
        w.writerow({
            "source": str(SOURCE),
            "out": str(OUT),
            "total_input_gb": total_input_gb,
            "total_output_gb": total_output_gb,
            "compression_ratio": ratio,
            "saved_gb": saved_gb
        })
    print(f"Saved summary: {summary_csv}")


    # ----------------------------
    # Step 2: ARCHIVE (.tar.zst) (+ optional delete)
    # ----------------------------
    run_buckets = buckets_to_run()
    print("\nBuckets to ARCHIVE:", " -> ".join(run_buckets))
    print()

    arch_log = []
    for b in run_buckets:
        items = [r for r in scan_rows if r.get("bucket") == b and r.get("status") == "OK"]
        items.sort(key=lambda r: (r.get("size_gb") if isinstance(r.get("size_gb"), (int, float)) else 10**18))

        if not items:
            print(f"[{b}] (no folders)")
            continue

        total_gb = sum([float(r["size_gb"]) for r in items if isinstance(r.get("size_gb"), (int, float))])
        print(f"\n[{b}] folders={len(items)} total~{total_gb:.2f} GB")

        for r in items:
            folder = Path(r["folder"])
            gb = r.get("size_gb")

            if not folder.exists():
                print(f"  SKIP missing: {folder}")
                arch_log.append({"bucket": b, "folder": str(folder), "archive": "", "size_gb": gb, "status": "MISSING", "note": ""})
                continue

            final_arc = OUT / f"{folder.name}.tar.zst"

            if SKIP_EXISTING_ARCHIVES and final_arc.exists():
                print(f"  SKIP_EXISTS: {folder.name}")
                arch_log.append({"bucket": b, "folder": str(folder), "archive": str(final_arc), "size_gb": gb, "status": "SKIP_EXISTS", "note": ""})
                continue

            if is_locked(final_arc):
                print(f"  SKIP_LOCKED_ARCHIVE: {folder.name}")
                arch_log.append({"bucket": b, "folder": str(folder), "archive": str(final_arc), "size_gb": gb, "status": "SKIP_LOCKED_ARCHIVE", "note": ""})
                continue

            tmp_arc = OUT / f"{folder.name}._tmp_{uuid.uuid4().hex}.tar.zst"

            ok = False
            last_err = ""
            for attempt in range(1, RETRIES + 1):
                try:
                    if tmp_arc.exists():
                        tmp_arc.unlink()

                    print(f"  ARCHIVING: {folder.name} ({float(gb):.2f} GB) -> .tar.zst")
                    tar_zst_dir_contents(folder, tmp_arc, level=ZSTD_LEVEL, threads=ZSTD_THREADS)

                    if final_arc.exists():
                        final_arc.unlink()
                    tmp_arc.replace(final_arc)

                    ok = True
                    print(f"  ARCHIVED:  {folder.name} -> {final_arc}")
                    arch_log.append({"bucket": b, "folder": str(folder), "archive": str(final_arc), "size_gb": gb, "status": "ARCHIVED", "note": ""})
                    break
                except KeyboardInterrupt:
                    print("\nInterrupted by user. Writing partial log and exiting.")
                    ok = False
                    last_err = "KeyboardInterrupt"
                    break
                except Exception as e:
                    last_err = str(e)
                    print(f"  RETRY {attempt}/{RETRIES} failed: {last_err}")
                    time.sleep(SLEEP_BETWEEN_RETRIES_SEC)

            if not ok:
                print(f"  ARCHIVE_FAILED: {folder.name} ({last_err})")
                arch_log.append({"bucket": b, "folder": str(folder), "archive": str(final_arc), "size_gb": gb, "status": "ARCHIVE_FAILED", "note": last_err})
                if last_err == "KeyboardInterrupt":
                    break
                continue

            if DELETE_AFTER_ARCHIVE:
                try:
                    shutil.rmtree(folder)
                    print(f"  DELETED: {folder.name}")
                    arch_log[-1]["status"] = "DELETED"
                except Exception as e:
                    print(f"  DELETE_FAILED: {folder.name} ({e})")
                    arch_log[-1]["status"] = "DELETE_FAILED"
                    arch_log[-1]["note"] = str(e)

        if arch_log and arch_log[-1].get("note") == "KeyboardInterrupt":
            break

    # Save archive log
    log_path = OUT / f"archive_log_{stamp}.csv"
    with open(log_path, "w", newline="", encoding="utf-8") as f:
        fields = ["bucket", "folder", "archive", "size_gb", "status", "note"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in arch_log:
            w.writerow({k: row.get(k, "") for k in fields})

    
    
        total_output_gb = total_archives_gb(OUT)
    ratio = round((total_output_gb / total_input_gb), 3) if total_input_gb > 0 else ""
    saved_gb = round((total_input_gb - total_output_gb), 2) if total_input_gb > 0 else ""

    print("\nTOTAL SIZE CHECK (after archiving):")
    print(f"  INPUT  (folders): {total_input_gb:.2f} GB")
    print(f"  OUTPUT (archives): {total_output_gb:.2f} GB")
    if ratio != "":
        print(f"  Compression ratio: {ratio}")
        print(f"  Saved: {saved_gb:.2f} GB")

    
    print(f"\nSaved archive log: {log_path}")
    print("DONE.")

if __name__ == "__main__":
    main()
