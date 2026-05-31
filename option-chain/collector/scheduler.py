"""
Market Schedule Manager
Handles auto start/stop based on configurable exchange schedules.
"""
import json
import os
import logging
import threading
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

SCHEDULE_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'schedule.json')

DEFAULT_SCHEDULE = {
    "auto_schedule": True,
    "timezone": "Asia/Kolkata",
    "schedules": [
        {
            "name": "NSE / BSE",
            "exchanges": ["NSE", "BSE", "NFO", "BFO", "NSE_INDEX", "BSE_INDEX"],
            "start": "09:15",
            "stop":  "15:35",
            "days":  ["Mon", "Tue", "Wed", "Thu", "Fri"]
        },
        {
            "name": "MCX",
            "exchanges": ["MCX"],
            "start": "09:00",
            "stop":  "23:30",
            "days":  ["Mon", "Tue", "Wed", "Thu", "Fri"]
        }
    ]
}

DAY_MAP = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


class ScheduleManager:
    def __init__(self, on_start=None, on_stop=None):
        self._on_start  = on_start
        self._on_stop   = on_stop
        self._config    = self._load()
        self._thread    = None
        self._running   = False
        self._last_state = None   # 'started' | 'stopped'

    def _load(self):
        os.makedirs(os.path.dirname(SCHEDULE_FILE), exist_ok=True)
        if os.path.exists(SCHEDULE_FILE):
            try:
                return json.load(open(SCHEDULE_FILE))
            except Exception:
                pass
        self._save(DEFAULT_SCHEDULE)
        return DEFAULT_SCHEDULE

    def _save(self, cfg):
        os.makedirs(os.path.dirname(SCHEDULE_FILE), exist_ok=True)
        with open(SCHEDULE_FILE, 'w') as f:
            json.dump(cfg, f, indent=2)

    def get_config(self):
        return self._config

    def get_schedule(self):
        """Return schedule in frontend format {enabled, days, start_time, stop_time}."""
        cfg = self._config
        # Convert internal NSE start/stop to simple format
        nse = cfg.get('NSE', cfg.get('nse', {}))
        return {
            'enabled':    cfg.get('auto_schedule', True),
            'days':       nse.get('days', [1,2,3,4,5]),
            'start_time': nse.get('start', '09:15'),
            'stop_time':  nse.get('stop',  '15:35'),
        }

    def apply_schedule(self, ui_cfg: dict):
        """Update schedule from frontend format."""
        days  = ui_cfg.get('days', [1,2,3,4,5])
        start = ui_cfg.get('start_time', '09:15')
        stop  = ui_cfg.get('stop_time',  '15:35')
        self._config['auto_schedule'] = ui_cfg.get('enabled', True)
        for seg in ('NSE', 'BSE'):
            if seg not in self._config:
                self._config[seg] = {}
            self._config[seg].update({'days': days, 'start': start, 'stop': stop})
        self._save(self._config)
        logger.info("Schedule updated via UI: %s-%s days=%s", start, stop, days)

    def update_config(self, new_cfg):
        self._config = new_cfg
        self._save(new_cfg)
        logger.info("Schedule config updated")

    def start_watcher(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._watch_loop, daemon=True, name='scheduler')
        self._thread.start()
        logger.info("Schedule watcher started")

    def stop_watcher(self):
        self._running = False

    def _watch_loop(self):
        while self._running:
            try:
                if self._config.get('auto_schedule'):
                    should_run = self._should_be_running()
                    if should_run and self._last_state != 'started':
                        logger.info("Schedule: auto-starting data collection")
                        self._last_state = 'started'
                        if self._on_start:
                            self._on_start()
                    elif not should_run and self._last_state != 'stopped':
                        logger.info("Schedule: auto-stopping data collection")
                        self._last_state = 'stopped'
                        if self._on_stop:
                            self._on_stop()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            import time; import time as _t; _t.sleep(30)  # check every 30 sec

    def _should_be_running(self):
        tz  = pytz.timezone(self._config.get('timezone', 'Asia/Kolkata'))
        now = datetime.now(tz)
        day_name = now.strftime('%a')  # Mon, Tue ...

        for sched in self._config.get('schedules', []):
            if day_name not in sched.get('days', []):
                continue
            start = self._parse_time(sched['start'], now)
            stop  = self._parse_time(sched['stop'],  now)
            if start <= now.replace(tzinfo=None).time() <= stop:
                return True
        return False

    def _parse_time(self, t_str, now):
        h, m = map(int, t_str.split(':'))
        from datetime import time as dtime
        return dtime(h, m)

    def is_market_open(self):
        return self._should_be_running()
