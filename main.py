import os
import subprocess
import sqlite3
import logging
import signal
import json
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header, Depends, Query

# Configuration from Environment Variables
DB_PATH = "/app/data/sync.db"
API_SECRET = os.getenv("API_SECRET")
# Ensure these match your Railway variable names for consistency
SOURCE_REMOTE = os.getenv("DROPBOX_SOURCE_PATH", "dropbox:sessions") 
DEST_REMOTE = os.getenv("WASABI_DEST_PATH", "wasabi:systemconcepts-sessions")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync-worker")

app = FastAPI()

# Global variable to track the active process for cancellation
active_process = None

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
    # Clear orphaned jobs from previous crashes/restarts
    c.execute('''
        UPDATE jobs 
        SET status = 'FAILED', 
            logs = logs || '\n' || ? || ': [SYSTEM] Sync was interrupted by a server restart.',
            end_time = ?
        WHERE status = 'RUNNING'
    ''', (datetime.now().strftime('%H:%M:%S'), datetime.now().isoformat()))
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

def log_job_start():
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

def run_rclone_sync(job_id, dynamic_token: str = None):
    """Execute rclone using an optional short-lived token from Vercel."""
    global active_process
    
    # Clone current environment and inject the token if provided
    env = os.environ.copy()
    if dynamic_token:
        # Rclone expects the 'token' field to be a JSON blob. 
        # Even without a refresh token, this format works for short-lived access.
        token_blob = json.dumps({
            "access_token": dynamic_token,
            "token_type": "bearer",
            "expiry": "2030-01-01T00:00:00Z" # Set far future so rclone doesn't try to refresh
        })
        # Note: Remote name 'dropbox' must match the prefix in SOURCE_REMOTE (e.g., dropbox:sessions)
        env["RCLONE_CONFIG_DROPBOX_TOKEN"] = token_blob
        logger.info("Syncing with dynamic token from Vercel...")

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
            start_new_session=True,
            env=env
        )
        
        for line in active_process.stdout:
            clean_line = line.strip()
            print(clean_line)
            log_job_update(job_id, new_log_line=clean_line)
            
        active_process.wait()
        
        status_map = {0: "COMPLETED", -15: "CANCELLED"} # -15 is SIGTERM
        final_status = status_map.get(active_process.returncode, "FAILED")
            
        log_job_update(job_id, new_log_line=f"Exit code: {active_process.returncode}", status=final_status)
        
    except Exception as e:
        logger.error(f"Execution Error: {str(e)}")
        log_job_update(job_id, new_log_line=f"CRITICAL ERROR: {str(e)}", status="FAILED")
    finally:
        active_process = None

async def verify_secret(x_api_key: str = Header(None)):
    if x_api_key != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid Secret")

@app.on_event("startup")
def on_startup():
    init_db()

@app.get("/")
def health():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

@app.post("/sync", dependencies=[Depends(verify_secret)])
async def trigger_sync(background_tasks: BackgroundTasks, x_db_token: str = Header(None)):
    """Trigger a sync, optionally passing the Dropbox access token in x-db-token header."""
    conn = get_db()
    active = conn.execute("SELECT id FROM jobs WHERE status = 'RUNNING'").fetchone()
    conn.close()
    
    if active:
        return {"status": "ignored", "message": "A sync job is already in progress.", "job_id": active['id']}
    
    job_id = log_job_start()
    background_tasks.add_task(run_rclone_sync, job_id, x_db_token)
    return {"status": "started", "job_id": job_id}

@app.post("/cancel", dependencies=[Depends(verify_secret)])
async def cancel_sync():
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
    conn = get_db()
    if history:
        jobs = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 20").fetchall()
        conn.close()
        return [dict(j) for j in jobs]
    job = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(job) if job else {"status": "IDLE"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)