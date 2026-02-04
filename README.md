# Compress folders with metadata (tar + zstd on Windows)

This project compresses each subfolder inside a source directory into a **`.tar.zst`** archive using **tar + zstd** (fast, strong compression), and automatically writes **metadata reports** (CSV/JSON) so you can verify what was processed before transferring large datasets (e.g., GeoTIFFs) to HPC or a datastore.

---

## What it does

For each subfolder in `SOURCE`, the script will:

- **Scan folder size** (fast, using `robocopy /L`)
- Assign a **size bucket** (e.g., `<1 GB`, `1–10 GB`, `10–50 GB`, …)
- Create an archive:  
  `OUT/<FolderName>.tar.zst`
- Write metadata logs:
  - `scan_<timestamp>.csv/.json/.txt`
  - `grouped_scan_<timestamp>.csv`
  - `archive_log_<timestamp>.csv`
  - *(optional)* `input_vs_output_<timestamp>.csv` (total input vs output sizes)

---

## Requirements

### 1) Windows tools
- **tar** (comes with Windows 10/11 or via Git for Windows)
- **zstd** (Zstandard CLI)

Check:
```powershell
tar --version
zstd --version
