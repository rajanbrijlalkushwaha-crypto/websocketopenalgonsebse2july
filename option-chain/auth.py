"""
MongoDB Auth Module
───────────────────
Handles user auth with MongoDB Atlas.
Provides Flask routes:
  POST /api/auth/signin           → login with email+password → JWT cookie
  POST /api/auth/signup           → register new user
  POST /api/auth/logout           → clear session
  GET  /api/auth/check-session    → verify current session
  GET  /api/auth/bootstrap        → full user+subscription data
  POST /api/admin/login           → admin system token (for Schedule/System tabs)
  GET  /api/admin/schedule        → get market schedule
  POST /api/admin/schedule        → update market schedule
  GET  /api/admin/status          → data collection status
  GET  /api/auth/admin/users      → list all users (admin only)
  PATCH /api/auth/admin/users/<id>/role    → change user role
  PATCH /api/auth/admin/users/<id>/suspend → toggle suspend
"""

import os
import jwt
import bcrypt
import logging
from datetime import datetime, timedelta, timezone
from functools import wraps

from flask import request, jsonify, make_response
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson import ObjectId

log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
MONGO_URI   = os.getenv('MONGO_URI', 'mongodb+srv://soc2025:soc2025@soc.idlpa2e.mongodb.net/?appName=SOC')
JWT_SECRET  = os.getenv('JWT_SECRET', 'openalgo-jwt-secret-2024-change-me')
JWT_EXPIRY  = int(os.getenv('JWT_EXPIRY_DAYS', 7))
ADMIN_SECRET= os.getenv('ADMIN_SECRET', 'admin-system-secret-2024')

# ── MongoDB connection ────────────────────────────────────────────────────────
_client = None
_db     = None

def get_db():
    global _client, _db
    if _db is None:
        try:
            _client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                tls=True,
                tlsAllowInvalidCertificates=True,  # needed for some Mac/Linux SSL configs
            )
            _client.admin.command('ping')
            _db = _client['socupstock']
            # Ensure indexes
            _db.users.create_index('email', unique=True)
            log.info("MongoDB connected: %s", MONGO_URI.split('@')[-1])
        except Exception as e:
            log.error("MongoDB connection failed: %s", e)
            raise
    return _db


# ── JWT helpers ───────────────────────────────────────────────────────────────
def _make_token(user_id: str, role: str) -> str:
    payload = {
        'sub':  str(user_id),
        'role': role,
        'exp':  datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRY),
        'iat':  datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')


def _verify_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def _get_token_from_request() -> str | None:
    # 1. Authorization: Bearer <token>
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        return auth[7:]
    # 2. Cookie
    return request.cookies.get('token')


def _user_to_dict(u: dict) -> dict:
    return {
        'id':        str(u['_id']),
        'name':      u.get('name', ''),
        'email':     u.get('email', ''),
        'role':      u.get('role', 'user'),
        'plan':      u.get('plan', 'free'),
        'suspended': u.get('suspended', False),
    }


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = _get_token_from_request()
        if not token:
            return jsonify({'success': False, 'error': 'Not authenticated'}), 401
        payload = _verify_token(token)
        if not payload:
            return jsonify({'success': False, 'error': 'Token expired or invalid'}), 401
        request.user_id = payload['sub']
        request.user_role = payload.get('role', 'user')
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = _get_token_from_request()
        if not token:
            return jsonify({'success': False, 'error': 'Not authenticated'}), 401
        payload = _verify_token(token)
        if not payload or payload.get('role') != 'admin':
            return jsonify({'success': False, 'error': 'Admin access required'}), 403
        request.user_id = payload['sub']
        request.user_role = 'admin'
        return f(*args, **kwargs)
    return decorated


def _set_cookie(response, token: str):
    response.set_cookie(
        'token', token,
        httponly=True, samesite='Lax',
        max_age=JWT_EXPIRY * 86400,
        secure=False,   # set True in production with HTTPS
    )


# ── Route registration ────────────────────────────────────────────────────────
def register_auth_routes(app, scheduler_ref=None):
    """Call this from app.py to mount all auth + schedule routes."""

    # ── Sign in ───────────────────────────────────────────────────────────────
    @app.route('/api/auth/signin', methods=['POST'])
    def signin():
        data  = request.get_json() or {}
        email = data.get('email', '').strip().lower()
        pwd   = data.get('password', '')
        if not email or not pwd:
            return jsonify({'success': False, 'message': 'Email and password required'}), 400
        try:
            db   = get_db()
            user = db.users.find_one({'email': email})
            if not user:
                return jsonify({'success': False, 'message': 'Invalid email or password'}), 401
            if user.get('suspended'):
                return jsonify({'success': False, 'message': 'Account suspended'}), 403
            stored_pwd = user['password']
            if isinstance(stored_pwd, str):
                stored_pwd = stored_pwd.encode()
            if not bcrypt.checkpw(pwd.encode(), stored_pwd):
                return jsonify({'success': False, 'message': 'Invalid email or password'}), 401

            token = _make_token(user['_id'], user.get('role', 'user'))
            resp  = make_response(jsonify({
                'success': True,
                'user':    _user_to_dict(user),
                'token':   token,
            }))
            _set_cookie(resp, token)
            return resp
        except Exception as e:
            log.error("signin error: %s", e)
            return jsonify({'success': False, 'message': 'Server error'}), 500

    # ── Sign up ───────────────────────────────────────────────────────────────
    @app.route('/api/auth/signup', methods=['POST'])
    def signup():
        data  = request.get_json() or {}
        name  = data.get('name', '').strip()
        email = data.get('email', '').strip().lower()
        pwd   = data.get('password', '')
        if not email or not pwd or not name:
            return jsonify({'success': False, 'message': 'Name, email and password required'}), 400
        if len(pwd) < 6:
            return jsonify({'success': False, 'message': 'Password must be at least 6 characters'}), 400
        try:
            db   = get_db()
            hashed = bcrypt.hashpw(pwd.encode(), bcrypt.gensalt())
            # First user becomes admin
            is_first = db.users.count_documents({}) == 0
            role = 'admin' if is_first else 'user'
            doc = {
                'name':          name,
                'email':         email,
                'password': hashed,
                'role':          role,
                'plan':          'pro' if is_first else 'free',
                'suspended':     False,
                'created_at':    datetime.now(timezone.utc),
            }
            result = db.users.insert_one(doc)
            token  = _make_token(result.inserted_id, role)
            resp   = make_response(jsonify({
                'success': True,
                'user':    _user_to_dict({**doc, '_id': result.inserted_id}),
                'token':   token,
                'message': 'Account created',
            }))
            _set_cookie(resp, token)
            return resp
        except DuplicateKeyError:
            return jsonify({'success': False, 'message': 'Email already registered'}), 409
        except Exception as e:
            log.error("signup error: %s", e)
            return jsonify({'success': False, 'message': 'Server error'}), 500

    # ── Logout ────────────────────────────────────────────────────────────────
    @app.route('/api/auth/logout', methods=['POST'])
    def logout():
        resp = make_response(jsonify({'success': True}))
        resp.delete_cookie('token')
        return resp

    # ── Check session ─────────────────────────────────────────────────────────
    @app.route('/api/auth/check-session')
    def check_session():
        token = _get_token_from_request()
        if not token:
            return jsonify({'authenticated': False})
        payload = _verify_token(token)
        if not payload:
            return jsonify({'authenticated': False})
        try:
            db   = get_db()
            user = db.users.find_one({'_id': ObjectId(payload['sub'])})
            if not user or user.get('suspended'):
                return jsonify({'authenticated': False})
            return jsonify({'authenticated': True, 'user': _user_to_dict(user)})
        except Exception:
            return jsonify({'authenticated': False})

    # ── Bootstrap (replaces the hardcoded one in app.py) ─────────────────────
    @app.route('/api/auth/bootstrap')
    def bootstrap():
        token = _get_token_from_request()
        # No token → return unauthenticated so React shows login page
        if not token:
            return jsonify({'authenticated': False, 'subscription': None,
                            'settings': {}, 'popup': [], 'unread': 0})
        payload = _verify_token(token)
        if not payload:
            return jsonify({'authenticated': False, 'subscription': None,
                            'settings': {}, 'popup': [], 'unread': 0})
        try:
            db   = get_db()
            user = db.users.find_one({'_id': ObjectId(payload['sub'])})
            if not user or user.get('suspended'):
                return jsonify({'authenticated': False, 'subscription': None,
                                'settings': {}, 'popup': [], 'unread': 0})
            role = user.get('role', 'user')
            plan = user.get('plan', 'free')
            return jsonify({
                'authenticated': True,
                'user':         _user_to_dict(user),
                'subscription': {'plan': plan, 'active': True},
                'settings':     {},
                'popup':        [],
                'unread':       0,
            })
        except Exception as e:
            log.error("bootstrap error: %s", e)
            return jsonify({'authenticated': False, 'subscription': None,
                            'settings': {}, 'popup': [], 'unread': 0})

    # ── Forgot / reset password (stub — extend with email later) ─────────────
    @app.route('/api/auth/forgot-password', methods=['POST'])
    def forgot_password():
        return jsonify({'success': True, 'message': 'Password reset email sent (if account exists)'})

    @app.route('/api/auth/reset-password', methods=['POST'])
    def reset_password():
        return jsonify({'success': False, 'message': 'Use admin panel to reset passwords'})

    @app.route('/api/auth/verify-otp', methods=['POST'])
    def verify_otp():
        return jsonify({'success': True, 'message': 'Verified'})

    @app.route('/api/auth/resend-verification', methods=['POST'])
    def resend_verification():
        return jsonify({'success': True})

    # ── Admin user management ─────────────────────────────────────────────────
    @app.route('/api/auth/admin/users')
    @require_admin
    def admin_list_users():
        try:
            db    = get_db()
            users = list(db.users.find({}, {'password': 0}))
            return jsonify({'success': True, 'users': [_user_to_dict(u) for u in users]})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/api/auth/admin/users/<user_id>/role', methods=['PATCH'])
    @require_admin
    def admin_change_role(user_id):
        role = (request.get_json() or {}).get('role', 'user')
        if role not in ('admin', 'member', 'user'):
            return jsonify({'success': False, 'error': 'Invalid role'}), 400
        try:
            db = get_db()
            db.users.update_one({'_id': ObjectId(user_id)}, {'$set': {'role': role}})
            return jsonify({'success': True, 'message': f'Role updated to {role}'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/api/auth/admin/users/<user_id>/suspend', methods=['PATCH'])
    @require_admin
    def admin_toggle_suspend(user_id):
        try:
            db   = get_db()
            user = db.users.find_one({'_id': ObjectId(user_id)})
            if not user:
                return jsonify({'success': False, 'error': 'User not found'}), 404
            new_state = not user.get('suspended', False)
            db.users.update_one({'_id': ObjectId(user_id)}, {'$set': {'suspended': new_state}})
            return jsonify({'success': True, 'suspended': new_state})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    # ── Admin system login (for Schedule/System/Logs tabs) ───────────────────
    @app.route('/api/admin/login', methods=['POST'])
    def admin_login():
        data = request.get_json() or {}
        # username:password — use email:password of any admin user
        username = data.get('username', '').strip().lower()
        password = data.get('password', '')
        try:
            db   = get_db()
            user = db.users.find_one({'email': username, 'role': 'admin'})
            stored_pwd = (user or {}).get('password', b'')
            if isinstance(stored_pwd, str): stored_pwd = stored_pwd.encode()
            if not user or not bcrypt.checkpw(password.encode(), stored_pwd):
                return jsonify({'success': False, 'message': 'Invalid admin credentials'}), 401
            token = _make_token(user['_id'], 'admin')
            return jsonify({'success': True, 'token': token})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # ── Admin status ──────────────────────────────────────────────────────────
    @app.route('/api/admin/status')
    @require_admin
    def api_admin_status():
        return jsonify({
            'success': True,
            'status':  'running',
            'version': '1.0.0',
            'uptime':  'active',
        })

    # ── Market schedule (connected to option-chain ScheduleManager) ───────────
    @app.route('/api/admin/schedule', methods=['GET', 'POST'])
    @require_admin
    def admin_schedule():
        if request.method == 'GET':
            if scheduler_ref and hasattr(scheduler_ref, 'get_schedule'):
                cfg = scheduler_ref.get_schedule()
            else:
                cfg = _load_schedule_file()
            return jsonify({'success': True, 'schedule': cfg, 'enabled': cfg.get('enabled', True)})

        # POST — save new schedule
        data    = request.get_json() or {}
        sched   = data.get('schedule', {})
        enabled = data.get('enabled', True)
        cfg = {
            'enabled':    enabled,
            'days':       sched.get('days', [1,2,3,4,5]),
            'start_time': sched.get('start_time', '09:15'),
            'stop_time':  sched.get('stop_time', '15:35'),
        }
        _save_schedule_file(cfg)
        if scheduler_ref and hasattr(scheduler_ref, 'apply_schedule'):
            scheduler_ref.apply_schedule(cfg)
        return jsonify({'success': True, 'message': 'Schedule saved', 'schedule': cfg})

    # ── Admin start/stop data collection ─────────────────────────────────────
    @app.route('/api/admin/start', methods=['POST'])
    @require_admin
    def admin_start_collection():
        # Calls the option-chain collector start
        try:
            from flask import current_app
            current_app.extensions.get('start_collector', lambda: None)()
        except Exception:
            pass
        return jsonify({'success': True, 'message': 'Collection started'})

    @app.route('/api/admin/stop', methods=['POST'])
    @require_admin
    def admin_stop_collection():
        try:
            from flask import current_app
            current_app.extensions.get('stop_collector', lambda: None)()
        except Exception:
            pass
        return jsonify({'success': True, 'message': 'Collection stopped'})

    log.info("Auth + schedule routes registered")


# ── Schedule file helpers ─────────────────────────────────────────────────────
import json
from pathlib import Path

_SCHED_FILE = Path(__file__).parent / 'data' / 'schedule_ui.json'

def _load_schedule_file() -> dict:
    try:
        if _SCHED_FILE.exists():
            return json.loads(_SCHED_FILE.read_text())
    except Exception:
        pass
    return {
        'enabled': True,
        'days': [1,2,3,4,5],
        'start_time': '09:15',
        'stop_time':  '15:35',
    }

def _save_schedule_file(cfg: dict):
    _SCHED_FILE.parent.mkdir(exist_ok=True)
    _SCHED_FILE.write_text(json.dumps(cfg, indent=2))
