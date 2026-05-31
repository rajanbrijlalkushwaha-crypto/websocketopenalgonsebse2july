"""
Market Hours Scheduler
──────────────────────
Manages start/stop of the entire pipeline based on market hours.
Runs as a standalone process (or imported by any service).

Usage:
  python -m pipeline.scheduler.market_scheduler

Controls:
  - Starts tick_producer on market open
  - Stops tick_producer on market close
  - Logs state changes

Admin REST API at port 5901:
  GET  /scheduler/status   → { running, next_start, next_stop, schedule }
  POST /scheduler/start    → force start now
  POST /scheduler/stop     → force stop now
  POST /scheduler/schedule → update schedule (JSON body)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import json
import time
import logging
import subprocess
import signal
import threading
from datetime import datetime, date, timedelta
from pathlib import Path

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, jsonify, request

from pipeline.config.settings import SCHEDULE

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [scheduler] %(message)s'
)
log = logging.getLogger(__name__)

IST   = pytz.timezone('Asia/Kolkata')
ROOT  = Path(__file__).parent.parent.parent
CFG_FILE = ROOT / 'pipeline' / 'config' / 'schedule_override.json'

_producer_proc:  subprocess.Popen | None = None
_consumer_procs: list[subprocess.Popen] = []
_sched_lock = threading.Lock()


def _now_ist() -> datetime:
    return datetime.now(IST)


def _load_schedule() -> dict:
    if CFG_FILE.exists():
        try:
            return json.loads(CFG_FILE.read_text())
        except Exception:
            pass
    return SCHEDULE


def _save_schedule(cfg: dict):
    CFG_FILE.write_text(json.dumps(cfg, indent=2))


def _is_market_open(schedule: dict | None = None) -> bool:
    cfg = schedule or _load_schedule()
    now = _now_ist()
    dow = now.weekday()
    t   = now.hour * 60 + now.minute

    for seg, s in cfg.items():
        if dow in s.get('days', []):
            sh, sm = map(int, s['start'].split(':'))
            eh, em = map(int, s['stop'].split(':'))
            if (sh * 60 + sm) <= t <= (eh * 60 + em):
                return True
    return False


def _launch_process(module: str) -> subprocess.Popen:
    cmd = [sys.executable, '-m', module]
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    log.info("Started %s (pid=%d)", module, proc.pid)
    return proc


def start_pipeline():
    global _producer_proc, _consumer_procs

    with _sched_lock:
        if _producer_proc and _producer_proc.poll() is None:
            log.info("Pipeline already running (pid=%d)", _producer_proc.pid)
            return

        log.info("=== STARTING PIPELINE ===")
        _producer_proc = _launch_process('pipeline.producer.tick_producer')

        time.sleep(2)   # let producer connect before consumers start

        _consumer_procs = [
            _launch_process('pipeline.consumers.dragonfly_consumer'),
            _launch_process('pipeline.consumers.influx_consumer'),
        ]
        log.info("Pipeline started: 1 producer + %d consumers", len(_consumer_procs))


def stop_pipeline():
    global _producer_proc, _consumer_procs

    with _sched_lock:
        log.info("=== STOPPING PIPELINE ===")

        for proc in [_producer_proc] + _consumer_procs:
            if proc and proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired: proc.kill()
                log.info("Stopped pid=%d", proc.pid)

        _producer_proc  = None
        _consumer_procs = []
        log.info("Pipeline stopped")


def _check_schedule():
    """Called every minute by APScheduler."""
    if _is_market_open():
        if not _producer_proc or _producer_proc.poll() is not None:
            log.info("Market hours — starting pipeline")
            start_pipeline()
    else:
        if _producer_proc and _producer_proc.poll() is None:
            log.info("Outside market hours — stopping pipeline")
            stop_pipeline()


# ── Admin Flask API ──────────────────────────────────────────────────
admin_app = Flask(__name__)

@admin_app.route('/scheduler/status')
def status():
    cfg = _load_schedule()
    running = bool(_producer_proc and _producer_proc.poll() is None)
    return jsonify({
        'running':     running,
        'market_open': _is_market_open(cfg),
        'schedule':    cfg,
        'time_ist':    _now_ist().strftime('%Y-%m-%d %H:%M:%S'),
        'producer_pid': _producer_proc.pid if running else None,
        'consumers':    len(_consumer_procs),
    })

@admin_app.route('/scheduler/start', methods=['POST'])
def force_start():
    start_pipeline()
    return jsonify({'success': True, 'action': 'started'})

@admin_app.route('/scheduler/stop', methods=['POST'])
def force_stop():
    stop_pipeline()
    return jsonify({'success': True, 'action': 'stopped'})

@admin_app.route('/scheduler/schedule', methods=['GET', 'POST'])
def manage_schedule():
    if request.method == 'GET':
        return jsonify(_load_schedule())
    cfg = request.get_json()
    if not cfg:
        return jsonify({'error': 'No JSON body'}), 400
    _save_schedule(cfg)
    return jsonify({'success': True, 'schedule': cfg})


def main():
    # Start APScheduler
    scheduler = BackgroundScheduler(timezone=IST)
    scheduler.add_job(_check_schedule, 'interval', minutes=1, id='market_check')
    scheduler.start()
    log.info("Market scheduler started (checking every minute)")

    # Run initial check
    _check_schedule()

    # Start admin API in background thread
    threading.Thread(
        target=lambda: admin_app.run(host='0.0.0.0', port=5901, use_reloader=False),
        daemon=True,
    ).start()
    log.info("Scheduler admin API on port 5901")

    # Block main thread
    def _sig(sig, frame):
        log.info("Signal — shutting down")
        stop_pipeline()
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    while True:
        time.sleep(30)


if __name__ == '__main__':
    main()
