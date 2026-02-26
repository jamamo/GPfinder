import os
import secrets
import sqlite3
from functools import wraps

from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
from werkzeug.security import generate_password_hash, check_password_hash

load_dotenv()

app = Flask(__name__)

# ── Security configuration ─────────────────────────────────────────────────────

_secret = os.environ.get('SECRET_KEY')
if not _secret:
    _secret = secrets.token_hex(32)
    print('WARNING: SECRET_KEY not set in .env — using a random key. '
          'Sessions will reset on every restart.')

app.secret_key = _secret

_is_production = os.environ.get('FORCE_HTTPS', '').lower() in ('1', 'true', 'yes')

app.config.update(
    TEMPLATES_AUTO_RELOAD=True,
    SESSION_COOKIE_HTTPONLY=True,       # JS cannot read session cookie
    SESSION_COOKIE_SAMESITE='Lax',      # CSRF mitigation for cross-site requests
    SESSION_COOKIE_SECURE=_is_production,  # HTTPS-only cookies in production
    PERMANENT_SESSION_LIFETIME=3600,    # 1-hour session timeout
    WTF_CSRF_TIME_LIMIT=3600,
)

csrf = CSRFProtect(app)

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=[],
    storage_uri='memory://',
)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gp.db')


# ── HTTPS redirect ─────────────────────────────────────────────────────────────

@app.before_request
def enforce_https():
    if _is_production:
        proto = request.headers.get('X-Forwarded-Proto', 'http')
        if proto != 'https':
            return redirect(request.url.replace('http://', 'https://'), 301)


# ── Database ───────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS gps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            practice_code TEXT,
            practice_name TEXT NOT NULL,
            partnership_name TEXT,
            neighbourhood TEXT,
            area TEXT,
            address_line1 TEXT,
            address_line2 TEXT,
            address_line3 TEXT,
            postcode TEXT,
            telephone TEXT,
            email TEXT,
            region TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        );
    ''')

    admin_password = os.environ.get('ADMIN_PASSWORD', 'admin123')
    existing = conn.execute('SELECT id FROM admins LIMIT 1').fetchone()

    if not existing:
        conn.execute(
            'INSERT INTO admins (username, password_hash) VALUES (?, ?)',
            ('admin', generate_password_hash(admin_password))
        )
        conn.commit()
    elif os.environ.get('ADMIN_PASSWORD'):
        # Sync password if explicitly provided via env var
        conn.execute(
            'UPDATE admins SET password_hash = ? WHERE username = ?',
            (generate_password_hash(admin_password), 'admin')
        )
        conn.commit()

    if admin_password == 'admin123':
        print('WARNING: Using default admin password "admin123". '
              'Set ADMIN_PASSWORD in your .env file.')

    conn.close()


# ── Auth helper ────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'admin_id' not in session:
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated


# ── Public routes ──────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search')
def search():
    q = request.args.get('q', '').strip()
    if not q:
        return redirect(url_for('index'))
    conn = get_db()
    results = conn.execute('''
        SELECT * FROM gps
        WHERE practice_name LIKE ?1
           OR postcode       LIKE ?1
           OR practice_code  LIKE ?1
           OR address_line1  LIKE ?1
           OR neighbourhood  LIKE ?1
           OR area           LIKE ?1
        ORDER BY practice_name
        LIMIT 100
    ''', (f'%{q}%',)).fetchall()
    conn.close()
    return render_template('results.html', results=results, query=q)


@app.route('/gp/<int:gp_id>')
def gp_detail(gp_id):
    conn = get_db()
    gp = conn.execute('SELECT * FROM gps WHERE id = ?', (gp_id,)).fetchone()
    conn.close()
    if not gp:
        return redirect(url_for('index'))
    return render_template('gp_detail.html', gp=gp)


# ── Admin routes ───────────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
@limiter.limit('10 per minute')
def admin_login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        conn = get_db()
        row = conn.execute('SELECT * FROM admins WHERE username = ?', (username,)).fetchone()
        conn.close()
        if row and check_password_hash(row['password_hash'], password):
            session.clear()
            session['admin_id'] = row['id']
            session.permanent = True
            return redirect(url_for('admin_dashboard'))
        flash('Invalid username or password.')
    return render_template('admin_login.html')


@app.route('/admin/logout')
def admin_logout():
    session.clear()
    return redirect(url_for('index'))


@app.route('/admin')
@login_required
def admin_dashboard():
    q = request.args.get('q', '').strip()
    conn = get_db()
    if q:
        gps = conn.execute('''
            SELECT * FROM gps
            WHERE practice_name LIKE ?1 OR postcode LIKE ?1 OR practice_code LIKE ?1
            ORDER BY practice_name LIMIT 200
        ''', (f'%{q}%',)).fetchall()
    else:
        gps = conn.execute('SELECT * FROM gps ORDER BY practice_name LIMIT 200').fetchall()
    total = conn.execute('SELECT COUNT(*) FROM gps').fetchone()[0]
    conn.close()
    return render_template('admin.html', gps=gps, total=total, query=q)


@app.route('/admin/add', methods=['GET', 'POST'])
@login_required
def admin_add():
    if request.method == 'POST':
        conn = get_db()
        conn.execute('''
            INSERT INTO gps
              (practice_code, practice_name, partnership_name, neighbourhood, area,
               address_line1, address_line2, address_line3, postcode, telephone, email, region)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            request.form.get('practice_code') or None,
            request.form.get('practice_name'),
            request.form.get('partnership_name') or None,
            request.form.get('neighbourhood') or None,
            request.form.get('area') or None,
            request.form.get('address_line1') or None,
            request.form.get('address_line2') or None,
            request.form.get('address_line3') or None,
            request.form.get('postcode') or None,
            request.form.get('telephone') or None,
            request.form.get('email') or None,
            request.form.get('region') or None,
        ))
        conn.commit()
        conn.close()
        flash('GP practice added.')
        return redirect(url_for('admin_dashboard'))
    return render_template('admin_form.html', gp=None, action='Add')


@app.route('/admin/edit/<int:gp_id>', methods=['GET', 'POST'])
@login_required
def admin_edit(gp_id):
    conn = get_db()
    gp = conn.execute('SELECT * FROM gps WHERE id = ?', (gp_id,)).fetchone()
    if not gp:
        conn.close()
        return redirect(url_for('admin_dashboard'))
    if request.method == 'POST':
        conn.execute('''
            UPDATE gps SET
              practice_code=?, practice_name=?, partnership_name=?, neighbourhood=?, area=?,
              address_line1=?, address_line2=?, address_line3=?, postcode=?, telephone=?, email=?, region=?
            WHERE id=?
        ''', (
            request.form.get('practice_code') or None,
            request.form.get('practice_name'),
            request.form.get('partnership_name') or None,
            request.form.get('neighbourhood') or None,
            request.form.get('area') or None,
            request.form.get('address_line1') or None,
            request.form.get('address_line2') or None,
            request.form.get('address_line3') or None,
            request.form.get('postcode') or None,
            request.form.get('telephone') or None,
            request.form.get('email') or None,
            request.form.get('region') or None,
            gp_id,
        ))
        conn.commit()
        conn.close()
        flash('GP practice updated.')
        return redirect(url_for('admin_dashboard'))
    conn.close()
    return render_template('admin_form.html', gp=gp, action='Edit')


@app.route('/admin/delete/<int:gp_id>', methods=['POST'])
@login_required
def admin_delete(gp_id):
    conn = get_db()
    conn.execute('DELETE FROM gps WHERE id = ?', (gp_id,))
    conn.commit()
    conn.close()
    flash('GP practice removed.')
    return redirect(url_for('admin_dashboard'))


init_db()

if __name__ == '__main__':
    print('\n  GP Finder running at http://127.0.0.1:5000')
    print('  Admin panel:        http://127.0.0.1:5000/admin\n')
    app.run(debug=False, port=5000)
