import os
import subprocess
import sqlite3
import logging
import signal
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header, Depends, Query

# Configuration from Environment Variables
DB_PATH = "/app/data/sync.db"
API_SECRET = os.getenv("API_SECRET")
SOURCE_REMOTE = os.getenv("DROPBOX_SOURCE_PATH", "dropbox:sessions") 
DEST_REMOTE = os.getenv("WASABI_DEST_PATH", "wasabi:systemconcepts-sessions")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync-worker")

app = FastAPI()

# Global variable to track the active process for cancellation
active_process = None

def get_db():
    """Ensure the data directory exists and connect to SQLite."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize schema and automatically fail orphaned jobs from previous sessions."""
    conn = get_db()
    c = conn.cursor()
    
    # 1. Ensure Table exists
    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            start_time TEXT, 
            end_time TEXT, 
            status TEXT, 
            logs TEXT
        )
    ''')
    
    # 2. Force-fail any job left 'RUNNING' (happens if server crashed/restarted)
    # This prevents the "Job already in progress" error after a restart.
    c.execute('''
        UPDATE jobs 
        SET status = 'FAILED', 
            logs = logs || '\n' || ? || ': [SYSTEM] Sync was interrupted by a server restart.',
            end_time = ?
        WHERE status = 'RUNNING'
    ''', (datetime.now().strftime('%H:%M:%S'), datetime.now().isoformat()))
    
    conn.commit()
    conn.close()
    logger.info("Database initialized and orphaned jobs cleared.")

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
        if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            updates.append("end_time = ?")
            params.append(datetime.now().isoformat())
            
    if updates:
        params.append(job_id)
        query = f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?"
        c.execute(query, params)
        conn.commit()
    conn.close()

def run_rclone_sync(job_id):
    """Execute rclone with Object Lock Compliance flags."""
    global active_process
    
    cmd = [
        "rclone", "copy", SOURCE_REMOTE, DEST_REMOTE,
        "--update",
        "--transfers", "4",
        "--verbose",
        "--stats", "2s",
        "--ignore-checksum",
        "--no-update-modtime",
        "--no-traverse"
    ]
    
    try:
        active_process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True, 
            bufsize=1,
            start_new_session=True
        )
        
        for line in active_process.stdout:
            clean_line = line.strip()
            print(clean_line)
            log_job_update(job_id, new_log_line=clean_line)
            
        active_process.wait()
        
        if active_process.returncode == 0:
            final_status = "COMPLETED"
        elif active_process.returncode < 0:
            final_status = "CANCELLED"
        else:
            final_status = "FAILED"
            
        log_job_update(job_id, new_log_line=f"Exit code: {active_process.returncode}", status=final_status)
        
    except Exception as e:
        logger.error(f"Execution Error: {str(e)}")
        log_job_update(job_id, new_log_line=f"CRITICAL ERROR: {str(e)}", status="FAILED")
    finally:
        active_process = None

async def verify_secret(x_api_key: str = Header(None)):
    """API Key verification."""
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
    """Trigger a new sync job."""
    conn = get_db()
    active = conn.execute("SELECT id FROM jobs WHERE status = 'RUNNING'").fetchone()
    conn.close()
    
    if active:
        return {"status": "ignored", "message": "A sync job is already in progress.", "job_id": active['id']}
    
    job_id = log_job_start()
    background_tasks.add_task(run_rclone_sync, job_id)
    return {"status": "started", "job_id": job_id}

@app.post("/cancel", dependencies=[Depends(verify_secret)])
async def cancel_sync():
    """Terminate the running rclone process group."""
    global active_process
    if not active_process:
        return {"status": "ignored", "message": "No active sync to cancel."}
    
    try:
        os.killpg(os.getpgid(active_process.pid), signal.SIGTERM)
        return {"status": "success", "message": "Cancellation signal sent."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/status", dependencies=[Depends(verify_secret)])
def get_status(history: bool = Query(False)):
    """Latest status or full history."""
    conn = get_db()
    if history:
        jobs = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 20").fetchall()
        conn.close()
        return [dict(j) for j in jobs]
    
    job = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(job) if job else {"status": "IDLE"}

@app.post("/clear-history", dependencies=[Depends(verify_secret)])
async def clear_history():
    """Deletes all job records from the database."""
    conn = get_db()
    try:
        conn.execute("DELETE FROM jobs")
        conn.commit()
        return {"status": "success", "message": "History cleared successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()    

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)