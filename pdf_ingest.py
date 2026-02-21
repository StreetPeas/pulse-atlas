#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(os.path.expanduser("~/Projects/atlas"))
DB_PATH = BASE / "data" / "atlas.db"
INBOX = BASE / "inbox"
ART = BASE / "data" / "artifacts"
PAGES_DIR = ART / "pages"

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_dirs():
    INBOX.mkdir(parents=True, exist_ok=True)
    (BASE / "data").mkdir(parents=True, exist_ok=True)
    ART.mkdir(parents=True, exist_ok=True)
    PAGES_DIR.mkdir(parents=True, exist_ok=True)

def ensure_tools():
    if subprocess.call(["/usr/bin/which", "pdftoppm"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) != 0:
        raise SystemExit("ERROR: pdftoppm not found. Install: brew install poppler")

def init_db():
    ensure_dirs()
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    cur.execute("""
CREATE TABLE IF NOT EXISTS documents(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'inbox',
  url TEXT,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  pages INTEGER DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'new',
  err TEXT
);
""")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_sha ON documents(sha256);")

    cur.execute("""
CREATE TABLE IF NOT EXISTS doc_pages(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id INTEGER NOT NULL,
  page INTEGER NOT NULL,
  img_path TEXT,
  text TEXT,
  ocr_used INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(doc_id) REFERENCES documents(id)
);
""")

    cur.execute("""
CREATE VIRTUAL TABLE IF NOT EXISTS doc_pages_fts
USING fts5(doc_id, page, text, content='doc_pages', content_rowid='id');
""")
    cur.execute("""
CREATE TRIGGER IF NOT EXISTS doc_pages_ai AFTER INSERT ON doc_pages BEGIN
  INSERT INTO doc_pages_fts(rowid, doc_id, page, text) VALUES (new.id, new.doc_id, new.page, new.text);
END;
""")
    cur.execute("""
CREATE TRIGGER IF NOT EXISTS doc_pages_au AFTER UPDATE ON doc_pages BEGIN
  UPDATE doc_pages_fts SET doc_id=new.doc_id, page=new.page, text=new.text WHERE rowid=old.id;
END;
""")
    cur.execute("""
CREATE TRIGGER IF NOT EXISTS doc_pages_ad AFTER DELETE ON doc_pages BEGIN
  DELETE FROM doc_pages_fts WHERE rowid=old.id;
END;
""")

    conn.commit()
    conn.close()

def run_pdftoppm(pdf: Path, out_prefix: Path):
    subprocess.check_call(["pdftoppm", "-png", str(pdf), str(out_prefix)])

def ingest_one(pdf: Path, source: str = "inbox", url: str = "") -> int:
    ensure_dirs()
    ensure_tools()
    init_db()

    sha = sha256_file(pdf)
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    row = cur.execute("SELECT id, status, pages FROM documents WHERE sha256=?", (sha,)).fetchone()
    if row and row[1] == "done" and int(row[2] or 0) > 0:
        conn.close()
        return int(row[0])

    if row:
        doc_id = int(row[0])
        cur.execute("UPDATE documents SET ts=?, source=?, url=?, filename=?, status=?, err='' WHERE id=?",
                    (now_utc_iso(), source, url or "", pdf.name, "new", doc_id))
    else:
        cur.execute("INSERT INTO documents(ts, source, url, filename, sha256, status) VALUES(?,?,?,?,?,?)",
                    (now_utc_iso(), source, url or "", pdf.name, sha, "new"))
        doc_id = int(cur.lastrowid)

    # wipe prior pages for this doc_id
    cur.execute("DELETE FROM doc_pages WHERE doc_id=?", (doc_id,))
    conn.commit()

    out_prefix = PAGES_DIR / f"doc{doc_id}"
    try:
        cur.execute("UPDATE documents SET status=? WHERE id=?", ("rendering", doc_id))
        conn.commit()

        run_pdftoppm(pdf, out_prefix)

        imgs = sorted(PAGES_DIR.glob(f"doc{doc_id}-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
        n_pages = len(imgs)
        if n_pages == 0:
            raise RuntimeError("pdftoppm produced zero pages")

        cur.execute("UPDATE documents SET status=?, pages=? WHERE id=?", ("done", n_pages, doc_id))

        # store pages (no OCR for now — text empty)
        for img in imgs:
            page = int(img.stem.split("-")[-1])
            cur.execute("INSERT INTO doc_pages(doc_id, page, img_path, text, ocr_used) VALUES(?,?,?,?,0)",
                        (doc_id, page, str(img), ""))

        conn.commit()
        conn.close()
        return doc_id
    except Exception as e:
        cur.execute("UPDATE documents SET status=?, err=? WHERE id=?", ("error", str(e)[:500], doc_id))
        conn.commit()
        conn.close()
        raise

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", help="path to PDF")
    ap.add_argument("--source", default="inbox")
    ap.add_argument("--url", default="")
    args = ap.parse_args()

    pdf = Path(args.pdf).expanduser()
    if not pdf.exists():
        raise SystemExit(f"PDF not found: {pdf}")

    doc_id = ingest_one(pdf, source=args.source, url=args.url)
    print(f"[ok] ingested doc_id={doc_id} pdf={pdf}")

if __name__ == "__main__":
    main()
