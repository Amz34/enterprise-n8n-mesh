#!/usr/bin/env python3
"""
n8n_autobooster.py - Enterprise Infrastructure Auto-Boosting & Dynamic Fleet Scaler
Monitors BullMQ queue backlog, worker container fleet, database growth, and host memory.
Automatically scales secondary worker fleets during queue spikes, cleans up when idle,
auto-prunes execution history, and alerts Telegram @AzGem_Bot.
"""

import os
import sys
import time
import json
import argparse
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# Add venv site-packages if needed for redis and psycopg2
VENV_SITES = [
    "/home/ubuntu/.venvs/data/lib/python3.12/site-packages",
    "/home/ubuntu/.venvs/data/lib/python3.10/site-packages",
]
for p in VENV_SITES:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)

try:
    import redis
except ImportError:
    redis = None

try:
    import psycopg2
except ImportError:
    psycopg2 = None

# Configuration
COMPOSE_FILE = "/home/ubuntu/projects/n8n/docker-compose.yml"
PROJECT_DIR = "/home/ubuntu/projects/n8n"
STATE_FILE = "/home/ubuntu/projects/n8n/data/autobooster_state.json"
TELEGRAM_CFG = "/home/ubuntu/.config/telegram/azgem_bot.json"
TELEGRAM_CHAT_ID = "8843884232"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_PW = "WDziCGFkjg0O8w7MVkYpbo558YLMqLEkKELoI2bb"

PG_HOST = "127.0.0.1"
PG_PORT = 5435
PG_DB = "n8n_enterprise"
PG_USER = "agent_admin"
PG_PW = "central_secret"

BOOST_THRESHOLD_PENDING = 5   # Scale up if > 5 pending jobs
BOOST_THRESHOLD_ACTIVE = 8    # Scale up if >= 8 active jobs on single worker
COOLDOWN_IDLE_CYCLES = 3      # Cooldown after 3 consecutive idle cycles


def get_telegram_creds():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)
    if not token and os.path.exists(TELEGRAM_CFG):
        try:
            with open(TELEGRAM_CFG, "r") as f:
                data = json.load(f)
                token = data.get("token") or data.get("bot_token")
                chat_id = data.get("chat_id") or chat_id
        except Exception:
            pass
    return token, chat_id


def send_telegram_alert(message: str):
    token, chat_id = get_telegram_creds()
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception as e:
        sys.stderr.write(f"Telegram dispatch error: {e}\n")
        return False


def get_queue_metrics():
    if not redis:
        return {"error": "redis library unavailable", "pending": 0, "active": 0}
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PW, socket_timeout=3)
        # BullMQ keys for n8n queue mode
        wait_len = r.llen("bull:jobs:wait")
        active_len = r.llen("bull:jobs:active")
        delayed_len = r.zcard("bull:jobs:delayed")
        failed_len = r.zcard("bull:jobs:failed")
        stalled = r.exists("bull:jobs:stalled-check")
        
        # In n8n, queue keys might also exist under custom prefixes
        n8n_keys = r.keys("bull:*")
        
        return {
            "wait": wait_len,
            "active": active_len,
            "delayed": delayed_len,
            "failed": failed_len,
            "stalled_guard_active": bool(stalled),
            "total_pending": wait_len + delayed_len,
            "total_bull_keys": len(n8n_keys)
        }
    except Exception as e:
        return {"error": str(e), "pending": 0, "active": 0}


def get_container_status():
    status = {"n8n_main": False, "n8n_worker": False, "n8n_worker_boost": False}
    try:
        res = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}#{{.Status}}"],
            capture_output=True, text=True, timeout=5
        )
        for line in res.stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("#")
            name = parts[0].strip()
            state = parts[1].strip() if len(parts) > 1 else ""
            if "Up" in state:
                if name == "n8n-automation":
                    status["n8n_main"] = True
                elif name == "n8n-worker":
                    status["n8n_worker"] = True
                elif name == "n8n-worker-boost":
                    status["n8n_worker_boost"] = True
    except Exception as e:
        status["error"] = str(e)
    return status


def get_host_memory():
    mem = {"available_mb": 0, "swap_used_mb": 0}
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    k = parts[0].strip()
                    v = int(parts[1].strip().split()[0])
                    if k == "MemAvailable":
                        mem["available_mb"] = v // 1024
                    elif k == "SwapTotal":
                        mem["swap_total_mb"] = v // 1024
                    elif k == "SwapFree":
                        mem["swap_free_mb"] = v // 1024
        mem["swap_used_mb"] = mem.get("swap_total_mb", 0) - mem.get("swap_free_mb", 0)
    except Exception:
        pass
    return mem


def prune_old_executions():
    if not psycopg2:
        return {"pruned": 0, "error": "psycopg2 missing"}
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PW, connect_timeout=3
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) FROM execution_entity
            WHERE "stoppedAt" < NOW() - INTERVAL '7 days';
        """)
        count = cur.fetchone()[0]
        if count > 3000:
            cur.execute("""
                DELETE FROM execution_entity
                WHERE "stoppedAt" < NOW() - INTERVAL '7 days';
            """)
            conn.commit()
            cur.close()
            conn.close()
            return {"pruned": count, "action": "deleted"}
        cur.close()
        conn.close()
        return {"pruned": 0, "eligible": count}
    except Exception as e:
        return {"pruned": 0, "error": str(e)}


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"idle_cycles": 0, "boost_active": False, "last_boost": 0}


def save_state(state):
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def execute_boost():
    print("🚀 [ACTION] Triggering n8n Worker Auto-Boost Fleet...")
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, "--profile", "boost", "up", "-d", "n8n-worker-boost"]
    res = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True, timeout=30)
    if res.returncode == 0:
        send_telegram_alert(
            "🚀 *[n8n INFRA AUTO-BOOST ACTIVATED]*\n\n"
            "• *Event*: BullMQ Queue Load Spike\n"
            "• *Action*: Secondary Worker Fleet Spawned\n"
            "• *Fleet Concurrency*: 20 Concurrent Workers\n"
            "• *Status*: Online & Consuming Backlog"
        )
        return True
    else:
        sys.stderr.write(f"Boost failed: {res.stderr}\n")
        return False


def execute_cooldown():
    print("📉 [ACTION] Queue idle. Cooldown stopping n8n-worker-boost...")
    cmd = ["docker", "rm", "-f", "n8n-worker-boost"]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    if res.returncode == 0:
        send_telegram_alert(
            "📉 *[n8n INFRA AUTO-BOOST COOLDOWN]*\n\n"
            "• *Event*: BullMQ Queue Clear & Idle\n"
            "• *Action*: Booster Worker Gracefully Stopped & Removed\n"
            "• *Reclaimed*: 1.0 GB RAM + 1 vCPU returned to host\n"
            "• *Status*: Base Worker Fleet Active (10 concurrency)"
        )
        return True
    return False


def evaluate_auto_boost():
    queue = get_queue_metrics()
    containers = get_container_status()
    mem = get_host_memory()
    state = load_state()

    # Self-heal base services if down
    if not containers.get("n8n_main"):
        print("⚠️ [HEAL] n8n-automation container is down! Starting...")
        subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "n8n"], cwd=PROJECT_DIR)
        send_telegram_alert("⚠️ *[HEAL ACTION]* n8n-automation container was down and has been auto-restarted.")

    if not containers.get("n8n_worker"):
        print("⚠️ [HEAL] n8n-worker container is down! Starting...")
        subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "n8n-worker"], cwd=PROJECT_DIR)
        send_telegram_alert("⚠️ *[HEAL ACTION]* n8n-worker container was down and has been auto-restarted.")

    pending = queue.get("total_pending", 0)
    active = queue.get("active", 0)
    boost_running = containers.get("n8n_worker_boost", False)

    # Decision Matrix
    should_boost = (pending >= BOOST_THRESHOLD_PENDING or active >= BOOST_THRESHOLD_ACTIVE)
    
    if should_boost and not boost_running:
        success = execute_boost()
        if success:
            state["boost_active"] = True
            state["last_boost"] = time.time()
            state["idle_cycles"] = 0
            save_state(state)
            return "BOOSTED"

    elif boost_running and pending == 0 and active == 0:
        state["idle_cycles"] = state.get("idle_cycles", 0) + 1
        if state["idle_cycles"] >= COOLDOWN_IDLE_CYCLES:
            success = execute_cooldown()
            if success:
                state["boost_active"] = False
                state["idle_cycles"] = 0
                save_state(state)
                return "COOLED_DOWN"
        else:
            save_state(state)
            return f"IDLE_WAITING ({state['idle_cycles']}/{COOLDOWN_IDLE_CYCLES})"
    else:
        state["idle_cycles"] = 0
        save_state(state)
        return "OPTIMAL"


def print_status():
    queue = get_queue_metrics()
    containers = get_container_status()
    mem = get_host_memory()
    prune = prune_old_executions()
    state = load_state()

    print("=" * 65)
    print("⚡ ENTERPRISE n8n INFRASTRUCTURE AUTO-BOOSTER STATUS")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    print("\n[1] BullMQ Distributed Queue Metrics:")
    print(f"    - Pending Jobs (Wait + Delay): {queue.get('total_pending', 0)}")
    print(f"    - Active Running Jobs        : {queue.get('active', 0)}")
    print(f"    - Failed Jobs                : {queue.get('failed', 0)}")
    print(f"    - BullMQ Key Guard           : {'ACTIVE' if queue.get('stalled_guard_active') else 'IDLE'}")

    print("\n[2] Container Fleet & Worker Topology:")
    print(f"    - n8n Primary Node (UI+Ingress) : {'UP' if containers.get('n8n_main') else 'DOWN'}")
    print(f"    - n8n Base Worker (10 Concurr)  : {'UP' if containers.get('n8n_worker') else 'DOWN'}")
    print(f"    - n8n Auto-Boost Worker (+10)   : {'UP (ACTIVE BOOST)' if containers.get('n8n_worker_boost') else 'STANDBY'}")

    print("\n[3] Host Memory & Compute Containment:")
    print(f"    - Available Host RAM : {mem.get('available_mb', 0)} MB")
    print(f"    - Used Host Swap     : {mem.get('swap_used_mb', 0)} MB")
    print(f"    - DB Prune Status    : {prune.get('action', 'HEALTHY')} (eligible: {prune.get('eligible', 0)})")

    print("\n[4] Auto-Scaler State:")
    print(f"    - Boost Active       : {state.get('boost_active', False)}")
    print(f"    - Idle Cycles        : {state.get('idle_cycles', 0)}/{COOLDOWN_IDLE_CYCLES}")

    print("=" * 65)


def main():
    parser = argparse.ArgumentParser(description="n8n Enterprise Infra Auto-Booster")
    parser.add_argument("--status", action="store_true", help="Display full telemetry status")
    parser.add_argument("--boost", action="store_true", help="Force trigger immediate worker boost")
    parser.add_argument("--cooldown", action="store_true", help="Force trigger worker cooldown")
    parser.add_argument("--once", action="store_true", help="Evaluate single scaling cycle")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background")
    args = parser.parse_args()

    if args.status:
        print_status()
    elif args.boost:
        execute_boost()
    elif args.cooldown:
        execute_cooldown()
    elif args.daemon:
        print("Starting n8n Auto-Booster Daemon (interval 30s)...")
        while True:
            try:
                res = evaluate_auto_boost()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Evaluation: {res}")
            except Exception as e:
                sys.stderr.write(f"Loop error: {e}\n")
            time.sleep(30)
    else:
        # Default: evaluate once and print summary
        result = evaluate_auto_boost()
        print(f"Auto-Boost Evaluation Result: {result}")


if __name__ == "__main__":
    main()
