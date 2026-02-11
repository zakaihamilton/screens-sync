import os
import subprocess
import sqlite3
import logging
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header, Depends
from pydantic import BaseModel

# --- CONFIGURATION ---
DB_PATH = "/app/data/sync.db"
API_SECRET = os.getenv("API_SECRET")

# --- SAFETY: DIRECTION CONFIGURATION ---
# 1. SOURCE (READ ONLY): We read FROM here
SOURCE_REMOTE = os.getenv("DROPBOX_SOURCE_PATH", "dropbox:sessions") 

# 2. DESTINATION (WRITE): We write TO here
DEST_REMOTE = os.getenv("WASABI_DEST_PATH", "wasabi:systemconcepts-sessions")

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("screens-sync")

app = FastAPI()

# --- DATABASE HELPERS ---
def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT,
            end_time TEXT,
            status TEXT,
            logs TEXT
        )
    ''')
    conn.commit()
    conn.close()

def log_job_start():
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO jobs (start_time, status, logs) VALUES (?, ?, ?)", 
        (datetime.now().isoformat(), 'RUNNING', f'🚀 Job Started: {SOURCE_REMOTE} -> {DEST_REMOTE}\n')
    )
    job_id = c.lastrowid
    conn.commit()
    conn.close()
    return job_id

def log_job_update(job_id, new_log_line=None, status=None):
    conn = get_db()
    c = conn.cursor()
    updates = []
    params = []
    if new_log_line:
        updates.append("logs = logs || ?")
        params.append(f"{datetime.now().strftime('%H:%M:%S')}: {new_log_line}\n")
    if status:
        updates.append("status = ?")
        params.append(status)
        if status in ['COMPLETED', 'FAILED']:
            updates.append("end_time = ?")
            params.append(datetime.now().isoformat())
    if updates:
        params.append(job_id)
        sql = f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?"
        c.execute(sql, params)
        conn.commit()
    conn.close()

# --- BACKGROUND WORKER ---
def run_rclone_sync(job_id):
    logger.info(f"Starting sync job {job_id}")
    
    # --- SAFETY CHECK ---
    # The order of arguments strictly defines the direction.
    # rclone copy <SOURCE> <DESTINATION>
    cmd = [
        "rclone", "copy", 
        SOURCE_REMOTE,  # FROM: Dropbox
        DEST_REMOTE,    # TO: Wasabi
        "--update",     # Skip files that are already newer on Wasabi
        "--transfers", "4",
        "--verbose",
        "--stats", "2s"
    ]
    
    try:
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True,
            bufsize=1
        )
        
        for line in process.stdout:
            clean_line = line.strip()
            if clean_line:
                print(clean_line)
                log_job_update(job_id, new_log_line=clean_line)
        
        process.wait()
        
        if process.returncode == 0:
            log_job_update(job_id, new_log_line="✅ Sync finished successfully.", status="COMPLETED")
        else:
            log_job_update(job_id, new_log_line=f"❌ Rclone exited with code {process.returncode}", status="FAILED")

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        log_job_update(job_id, new_log_line=f"🔥 Critical Error: {str(e)}", status="FAILED")

# --- AUTHENTICATION ---
async def verify_secret(x_api_key: str = Header(None)):
    if not API_SECRET:
        logger.warning("API_SECRET not set! Endpoint is insecure.")
        return
    if x_api_key != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API Secret")

# --- API ENDPOINTS ---
@app.on_event("startup")
def on_startup():
    init_db()

@app.get("/")
def health_check():
    return {"status": "online", "direction": "Dropbox -> Wasabi"}

@app.post("/sync", dependencies=[Depends(verify_secret)])
async def trigger_sync(background_tasks: BackgroundTasks):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM jobs WHERE status = 'RUNNING'")
    existing_job = c.fetchone()
    conn.close()

    if existing_job:
        return {"status": "ignored", "message": f"Job {existing_job['id']} is already running."}

    job_id = log_job_start()
    background_tasks.add_task(run_rclone_sync, job_id)
    
    return {"status": "started", "job_id": job_id, "message": "Sync started (Dropbox -> Wasabi)"}

@app.get("/status", dependencies=[Depends(verify_secret)])
def get_status():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1")
    job = c.fetchone()
    conn.close()

    if not job:
        return {"status": "IDLE", "logs": "No jobs run yet."}

    full_logs = job["logs"] or ""
    return {
        "job_id": job["id"],
        "status": job["status"],
        "start_time": job["start_time"],
        "logs": full_logs[-2000:] 
    }