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
SOURCE_REMOTE = os.getenv("DROPBOX_SOURCE_PATH", "dropbox:sessions") 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync-worker")

app = FastAPI()
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
    c.execute('''
        UPDATE jobs 
        SET status = 'FAILED', 
            logs = logs || '\n' || ? || ': [SYSTEM] Sync was interrupted by a server restart.',
            end_time = ?
        WHERE status = 'RUNNING'
    ''', (datetime.now().strftime('%H:%M:%S'), datetime.now().isoformat()))
    conn.commit()
    conn.close()

def log_job_start():
    conn = get_db()
    c = conn.cursor()
    initial_log = f"🚀 Job Started: {SOURCE_REMOTE} -> Redundant Destinations\n"
    c.execute("INSERT INTO jobs (start_time, status, logs) VALUES (?, ?, ?)", 
              (datetime.now().isoformat(), 'RUNNING', initial_log))
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
    global active_process
    env = os.environ.copy()
    if dynamic_token:
        token_blob = json.dumps({
            "access_token": dynamic_token,
            "token_type": "bearer",
            "expiry": "2030-01-01T00:00:00Z" 
        })
        env["RCLONE_CONFIG_DROPBOX_TOKEN"] = token_blob

    destinations = [
        os.getenv("WASABI_DEST_PATH", "wasabi:systemconcepts-sessions"),
        os.getenv("IDRIVE_DEST_PATH", "idrive_e2:systemconcepts-sessions")
    ]
    
    try:
        success_count = 0
        for dest in destinations:
            if not dest: continue
            log_job_update(job_id, new_log_line=f"--- Starting Sync to {dest} ---")
            
            cmd = [
                "rclone", "copy", SOURCE_REMOTE, dest,
                "--update",
                "--size-only",             # Crucial: Instant comparison
                "--fast-list",             # Crucial: Fetches 1,000 files per call
                "--checkers", "128",       # Parallelize the metadata scan
                "--transfers", "4",        # Keep uploads stable to avoid bandwidth choke
                "--tpslimit", "25",        # THE CHANGE: Faster API headroom
                "--verbose",
                "--stats", "10s",
                "--no-traverse",           # Don't list the destination recursively
                "--ignore-checksum",
                "--no-update-modtime"
            ]
            
            active_process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                text=True, bufsize=1, start_new_session=True, env=env
            )
            
            for line in active_process.stdout:
                clean_line = line.strip()
                print(clean_line)
                log_job_update(job_id, new_log_line=clean_line)
                
            active_process.wait() # FIXED: Outside the loop
            
            if active_process.returncode != 0:
                log_job_update(job_id, new_log_line=f"❌ Error in sync to {dest}. Code: {active_process.returncode}")
                continue # FIXED: Try next destination
            
            success_count += 1
            log_job_update(job_id, new_log_line=f"✅ Finished Sync to {dest}")

        final_status = "COMPLETED" if success_count > 0 else "FAILED"
        log_job_update(job_id, new_log_line=f"Job sequence finished. Successes: {success_count}/{len(destinations)}", status=final_status)
        
    except Exception as e:
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
    if not active_process: return {"status": "ignored", "message": "No active sync to cancel."}
    try:
        os.killpg(os.getpgid(active_process.pid), signal.SIGTERM)
        return {"status": "success", "message": "Cancellation signal sent."}
    except Exception as e: return {"status": "error", "message": str(e)}

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
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
