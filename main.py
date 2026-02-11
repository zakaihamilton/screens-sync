import os
import subprocess
import sqlite3
import logging
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header, Depends, Query
from pydantic import BaseModel

# Configuration from Environment Variables
DB_PATH = "/app/data/sync.db"
API_SECRET = os.getenv("API_SECRET")
SOURCE_REMOTE = os.getenv("DROPBOX_SOURCE_PATH", "dropbox:sessions") 
DEST_REMOTE = os.getenv("WASABI_DEST_PATH", "wasabi:systemconcepts-sessions")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync-worker")

app = FastAPI()

def get_db():
    """Ensure the data directory exists and connect to SQLite."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the database schema."""
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
    """Create a new job record in the database."""
    conn = get_db()
    c = conn.cursor()
    initial_log = f"🚀 Job Started: {SOURCE_REMOTE} -> {DEST_REMOTE}\n"
    c.execute(
        "INSERT INTO jobs (start_time, status, logs) VALUES (?, ?, ?)", 
        (datetime.now().isoformat(), 'RUNNING', initial_log)
    )
    job_id = c.lastrowid
    conn.commit()
    conn.close()
    return job_id

def log_job_update(job_id, new_log_line=None, status=None):
    """Update logs or status for an existing job."""
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
        query = f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?"
        c.execute(query, params)
        conn.commit()
    conn.close()

def run_rclone_sync(job_id):
    """Execute the rclone copy command with Object Lock and Versioning flags."""
    # Note: Using flags that respect Wasabi Compliance Mode (30 days)
    cmd = [
        "rclone", "copy", SOURCE_REMOTE, DEST_REMOTE,
        "--update",
        "--transfers", "4",
        "--verbose",
        "--stats", "2s",
        "--ignore-checksum",
        "--no-update-modtime"
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
            # We log to terminal and the DB simultaneously
            clean_line = line.strip()
            print(clean_line)
            log_job_update(job_id, new_log_line=clean_line)
            
        process.wait()
        
        final_status = "COMPLETED" if process.returncode == 0 else "FAILED"
        log_job_update(job_id, new_log_line=f"Exit code: {process.returncode}", status=final_status)
        
    except Exception as e:
        logger.error(f"Execution Error: {str(e)}")
        log_job_update(job_id, new_log_line=f"CRITICAL ERROR: {str(e)}", status="FAILED")

async def verify_secret(x_api_key: str = Header(None)):
    """Simple API Key verification."""
    if x_api_key != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid Secret")

@app.on_event("startup")
def on_startup():
    init_db()

@app.get("/")
def health():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

@app.post("/sync", dependencies=[Depends(verify_secret)])
async def trigger_sync(background_tasks: BackgroundTasks):
    """Trigger a new sync job if one isn't already running."""
    conn = get_db()
    active = conn.execute("SELECT id FROM jobs WHERE status = 'RUNNING'").fetchone()
    conn.close()
    
    if active:
        return {"status": "ignored", "message": "A sync job is already in progress.", "job_id": active['id']}
    
    job_id = log_job_start()
    background_tasks.add_task(run_rclone_sync, job_id)
    return {"status": "started", "job_id": job_id}

@app.get("/status", dependencies=[Depends(verify_secret)])
def get_status(history: bool = Query(False)):
    """Retrieve the latest job status or the full job history."""
    conn = get_db()
    if history:
        # Return last 20 jobs for the History Tab
        jobs = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 20").fetchall()
        conn.close()
        return [dict(j) for j in jobs]
    
    # Return only the most recent job
    job = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    
    if not job:
        return {"status": "IDLE"}
        
    return dict(job)

if __name__ == "__main__":
    import uvicorn
    # Use PORT env var if provided by Railway
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)