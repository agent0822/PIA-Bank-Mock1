
import hashlib
import html
import math
import mimetypes
import os
import secrets
import sqlite3
from flask import Flask
app = Flask(__name__)
from datetime import datetime, timedelta
from http.cookies import SimpleCookie
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "camp_cards.db")
STARTING_WEEKLY_BALANCE = 147000.0
PORT = 8123
ADMIN_USERNAME = "johhny"
ADMIN_PASSWORD = "admin"
MARKET_REFRESH_HOURS = 12
DEFAULT_MARKET_PRICE = 1200.0
LIVE_MARKET_BUCKET_MINUTES = 15
LIVE_MARKET_MAX_SWING_PCT = 2.8


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def money(value):
    return f"${value:,.2f}"


def number(value):
    return f"{value:,.2f}"


def hash_password(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def get_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}


def init_db():
    conn = get_db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS campers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            card_number TEXT NOT NULL UNIQUE,
            balance REAL NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            camper_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            amount REAL NOT NULL,
            note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            actor_username TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (camper_id) REFERENCES campers(id)
        );

        CREATE TABLE IF NOT EXISTS staff_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS auth_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_user_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (staff_user_id) REFERENCES staff_users(id)
        );

        CREATE TABLE IF NOT EXISTS action_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_user_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            details TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (staff_user_id) REFERENCES staff_users(id)
        );

        CREATE TABLE IF NOT EXISTS market_assets (
            symbol TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            sector TEXT NOT NULL,
            current_price REAL NOT NULL,
            previous_price REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            last_reason TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS market_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            camper_id INTEGER NOT NULL,
            asset_symbol TEXT NOT NULL,
            shares REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (camper_id, asset_symbol),
            FOREIGN KEY (camper_id) REFERENCES campers(id),
            FOREIGN KEY (asset_symbol) REFERENCES market_assets(symbol)
        );

        CREATE TABLE IF NOT EXISTS market_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            summary TEXT NOT NULL,
            energy_level INTEGER NOT NULL DEFAULT 50,
            spirit_level INTEGER NOT NULL DEFAULT 50,
            weather_score INTEGER NOT NULL DEFAULT 50,
            competition_score INTEGER NOT NULL DEFAULT 50,
            submitted_by TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_symbol TEXT NOT NULL,
            price REAL NOT NULL,
            reason TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'rule',
            created_at TEXT NOT NULL,
            FOREIGN KEY (asset_symbol) REFERENCES market_assets(symbol)
        );
        """
    )

    if "actor_username" not in get_columns(conn, "transactions"):
        conn.execute("ALTER TABLE transactions ADD COLUMN actor_username TEXT NOT NULL DEFAULT ''")

    for symbol, name, sector in [
        ("PIA", "Camp Spirit Index", "SPIRIT"),
        ("OIL", "Fuel & Logistics", "ENERGY"),
        ("GOLD", "Awards & Prestige", "VALUE"),
        ("TECH", "Innovation Lab", "TECH"),
    ]:
        existing_asset = conn.execute(
            "SELECT symbol FROM market_assets WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        if not existing_asset:
            conn.execute(
                """
                INSERT INTO market_assets (symbol, name, sector, current_price, previous_price, updated_at, last_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (symbol, name, sector, DEFAULT_MARKET_PRICE, DEFAULT_MARKET_PRICE, now(), "Opening market price"),
            )
            conn.execute(
                """
                INSERT INTO market_snapshots (asset_symbol, price, reason, source, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (symbol, DEFAULT_MARKET_PRICE, "Opening market price", "seed", now()),
            )

    admin_user = conn.execute(
        "SELECT id FROM staff_users WHERE username = ?",
        (ADMIN_USERNAME,),
    ).fetchone()
    if not admin_user:
        conn.execute(
            """
            INSERT INTO staff_users (username, password_hash, role, active, created_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (ADMIN_USERNAME, hash_password(ADMIN_PASSWORD), "ADMIN", now()),
        )

    conn.commit()
    conn.close()


def get_cookie_value(environ, key):
    cookie_header = environ.get("HTTP_COOKIE", "")
    if not cookie_header:
        return ""
    cookie = SimpleCookie()
    cookie.load(cookie_header)
    morsel = cookie.get(key)
    return morsel.value if morsel else ""


def build_session_cookie(token):
    return f"camp_wallet_session={token}; Path=/; HttpOnly; SameSite=Lax"


def clear_session_cookie():
    return "camp_wallet_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"


def get_current_user(conn, environ):
    token = get_cookie_value(environ, "camp_wallet_session")
    if not token:
        return None
    return conn.execute(
        """
        SELECT staff_users.*
        FROM auth_sessions
        JOIN staff_users ON staff_users.id = auth_sessions.staff_user_id
        WHERE auth_sessions.token = ? AND staff_users.active = 1
        """,
        (token,),
    ).fetchone()


def get_post_data(environ):
    size = int(environ.get("CONTENT_LENGTH") or 0)
    raw = environ["wsgi.input"].read(size).decode("utf-8")
    parsed = parse_qs(raw)
    return {key: values[0].strip() for key, values in parsed.items()}


def log_action(conn, user, action_type, details):
    if not user:
        return
    conn.execute(
        """
        INSERT INTO action_log (staff_user_id, action_type, details, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (user["id"], action_type, details, now()),
    )


def archived_card_number(card_number, camper_id):
    safe_card = "".join(ch if ch.isalnum() else "_" for ch in card_number)
    return f"archived_{safe_card}_{camper_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"


def page_template(content, user=None, message="", error="", action=""):
    message_html = f'<div class="notice success">{html.escape(message)}</div>' if message else ""
    error_html = f'<div class="notice error">{html.escape(error)}</div>' if error else ""
    action_titles = {
        "create": "Card Created",
        "charge": "Charge Recorded",
        "add_funds": "Funds Added",
        "transfer": "Transfer Complete",
        "weekly_reset": "New Week Ready",
        "remove": "Camper Removed",
        "replace_card": "Card Swapped",
        "staff_created": "Staff Added",
        "market_refresh": "Market Refreshed",
        "market_buy": "Stock Purchased",
        "market_sell": "Stock Sold",
        "market_event": "Hype Saved",
    }
    action_title = action_titles.get(action, "Action Complete")
    action_html = (
        f"""
        <div class="action-banner action-{html.escape(action)}" aria-live="polite">
          <div class="action-burst"></div>
          <div class="action-copy">
            <span class="action-kicker">Camp Wallet Update</span>
            <strong>{html.escape(action_title)}</strong>
            <span>{html.escape(message)}</span>
          </div>
        </div>
        """
        if action and message
        else ""
    )
    topbar = ""
    if user:
        topbar = f"""
        <div class="topbar">
          <div>
            <strong>{html.escape(user["username"])}</strong>
            <span class="topbar-role">{html.escape(user["role"])}</span>
          </div>
          <form method="post" action="/logout" class="logout-form">
            <button type="submit">Log Out</button>
          </form>
        </div>
        """
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Camp Card System</title>
  <style>
    :root {{
      --bg: #09162d;
      --panel: rgba(255, 255, 255, 0.96);
      --ink: #102446;
      --accent: #c72d2d;
      --accent-dark: #961f1f;
      --line: rgba(16, 36, 70, 0.14);
      --gold: #f1c24b;
      --good: #0f5132;
      --good-bg: #d1fae5;
      --bad: #991b1b;
      --bad-bg: #fee2e2;
      --hero-ink: #f8fbff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Trebuchet MS", "Gill Sans", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(199, 45, 45, 0.35), transparent 24%),
        radial-gradient(circle at top right, rgba(241, 194, 75, 0.16), transparent 22%),
        linear-gradient(180deg, #0a1a34, #102446 38%, #ecf3ff 38%, #f5f8ff 100%);
      min-height: 100vh;
    }}
    .wrap {{
      width: min(1140px, calc(100% - 32px));
      margin: 24px auto 48px;
    }}
    .topbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 16px;
      padding: 14px 18px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.92);
      box-shadow: 0 12px 24px rgba(16, 36, 70, 0.08);
    }}
    .topbar-role {{
      margin-left: 8px;
      padding: 4px 9px;
      border-radius: 999px;
      background: #e8eef9;
      font-size: 0.78rem;
      font-weight: bold;
    }}
    .logout-form {{
      margin: 0;
    }}
    .logout-form button {{
      margin: 0;
      width: auto;
      padding: 10px 14px;
    }}
    .hero {{
      position: relative;
      overflow: hidden;
      padding: 34px;
      border: 1px solid rgba(255, 255, 255, 0.18);
      border-radius: 28px;
      background: linear-gradient(135deg, rgba(199, 45, 45, 0.95), rgba(16, 36, 70, 0.98) 62%);
      box-shadow: 0 26px 48px rgba(7, 18, 39, 0.35);
      color: var(--hero-ink);
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -8% -30% auto;
      width: 280px;
      height: 280px;
      border-radius: 50%;
      background: rgba(255, 255, 255, 0.08);
      filter: blur(4px);
    }}
    h1, h2, h3 {{ margin-top: 0; }}
    h1 {{
      font-family: Georgia, "Times New Roman", serif;
      font-size: clamp(2.2rem, 4vw, 3.5rem);
      margin-bottom: 12px;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }}
    h2 {{
      font-family: Georgia, "Times New Roman", serif;
      font-size: 1.35rem;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }}
    .sub {{
      max-width: 780px;
      line-height: 1.5;
      font-size: 1.03rem;
      color: rgba(248, 251, 255, 0.92);
    }}
    .hero-top {{
      position: relative;
      z-index: 1;
      display: grid;
      grid-template-columns: minmax(120px, 190px) 1fr;
      gap: 24px;
      align-items: center;
    }}
    .logo-shell {{
      padding: 14px;
      border-radius: 24px;
      background: rgba(255, 255, 255, 0.08);
      border: 1px solid rgba(255, 255, 255, 0.16);
    }}
    .logo-shell img {{
      width: 100%;
      display: block;
      border-radius: 18px;
      background: #0b1730;
    }}
    .pill {{
      display: inline-block;
      padding: 6px 11px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.14);
      color: #fff;
      font-size: 0.84rem;
      font-weight: bold;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .hero-tagline {{
      display: inline-block;
      margin-top: 4px;
      color: rgba(255, 255, 255, 0.85);
      font-weight: bold;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      font-size: 0.82rem;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      margin-top: 22px;
      align-items: stretch;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-top: 18px;
    }}
    .stat, .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: 0 14px 32px rgba(16, 36, 70, 0.08);
    }}
    .stat {{
      padding: 16px;
      position: relative;
      overflow: hidden;
    }}
    .stat::before {{
      content: "";
      position: absolute;
      left: 0;
      top: 0;
      bottom: 0;
      width: 6px;
      background: linear-gradient(180deg, #c72d2d, #102446);
    }}
    .card {{
      padding: 20px;
      height: 100%;
      display: flex;
      flex-direction: column;
    }}
    .card h2 {{
      color: #102446;
      border-bottom: 2px solid #edf2fb;
      padding-bottom: 10px;
    }}
    .card form {{
      display: flex;
      flex-direction: column;
      flex: 1;
    }}
    label {{
      display: block;
      font-weight: bold;
      margin-bottom: 6px;
    }}
    input, select, button, textarea {{
      width: 100%;
      padding: 11px 12px;
      border-radius: 12px;
      border: 1px solid rgba(16, 36, 70, 0.18);
      font: inherit;
      margin-bottom: 12px;
      background: #fff;
    }}
    input:focus, select:focus, textarea:focus {{
      outline: 3px solid rgba(199, 45, 45, 0.16);
      border-color: rgba(199, 45, 45, 0.55);
    }}
    button {{
      background: linear-gradient(180deg, #d93737, #ad2525);
      color: #fff;
      border: 0;
      font-weight: bold;
      cursor: pointer;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }}
    button:hover {{ background: var(--accent-dark); }}
    .danger-button {{ background: linear-gradient(180deg, #7f1d1d, #5f1616); }}
    .danger-button:hover {{ background: #4a1010; }}
    .tiny {{
      font-size: 0.9rem;
      opacity: 0.82;
      margin-top: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      font-size: 0.97rem;
      background: #fff;
      border-radius: 16px;
      overflow: hidden;
    }}
    th, td {{
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid #e3eaf7;
      vertical-align: top;
    }}
    th {{
      background: #e8eef9;
      color: #102446;
    }}
    .notice {{
      padding: 12px 14px;
      border-radius: 14px;
      margin: 18px 0;
      font-weight: bold;
    }}
    .success {{ background: var(--good-bg); color: var(--good); }}
    .error {{ background: var(--bad-bg); color: var(--bad); }}
    .compact-form {{ display: inline; }}
    .compact-form button {{
      width: auto;
      margin: 0;
      padding: 8px 12px;
      border-radius: 10px;
      font-size: 0.82rem;
    }}
    .action-cell {{ white-space: nowrap; }}
    .action-banner {{
      position: fixed;
      top: 18px;
      right: 18px;
      width: min(360px, calc(100% - 36px));
      padding: 18px;
      border-radius: 22px;
      color: #fff;
      overflow: hidden;
      z-index: 999;
      box-shadow: 0 24px 48px rgba(7, 18, 39, 0.28);
      animation: banner-in 0.45s ease, banner-out 0.45s ease 3.7s forwards;
    }}
    .action-copy {{
      position: relative;
      z-index: 2;
      display: grid;
      gap: 4px;
    }}
    .action-kicker {{
      font-size: 0.75rem;
      font-weight: bold;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      opacity: 0.88;
    }}
    .action-copy strong {{
      font-size: 1.25rem;
      font-family: Georgia, "Times New Roman", serif;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }}
    .action-burst {{
      position: absolute;
      right: -30px;
      top: -30px;
      width: 150px;
      height: 150px;
      border-radius: 50%;
      background: rgba(255, 255, 255, 0.16);
      animation: burst-spin 3.2s linear infinite;
    }}
    .action-create {{ background: linear-gradient(135deg, #1d4ed8, #102446); }}
    .action-charge {{ background: linear-gradient(135deg, #c72d2d, #7f1d1d); }}
    .action-add_funds {{ background: linear-gradient(135deg, #0f766e, #14532d); }}
    .action-transfer {{ background: linear-gradient(135deg, #7c3aed, #102446); }}
    .action-weekly_reset {{ background: linear-gradient(135deg, #ea580c, #b45309); }}
    .action-remove {{ background: linear-gradient(135deg, #475569, #1e293b); }}
    .action-replace_card {{ background: linear-gradient(135deg, #2563eb, #0f172a); }}
    .action-staff_created {{ background: linear-gradient(135deg, #0891b2, #0f172a); }}
    .action-market_refresh {{ background: linear-gradient(135deg, #0f766e, #0b3b2e); }}
    .action-market_buy {{ background: linear-gradient(135deg, #1d4ed8, #0f172a); }}
    .action-market_sell {{ background: linear-gradient(135deg, #7c2d12, #431407); }}
    .action-market_event {{ background: linear-gradient(135deg, #ca8a04, #713f12); }}
    .login-shell {{
      max-width: 480px;
      margin: 80px auto;
    }}
    .admin-grid {{
      display: grid;
      grid-template-columns: minmax(300px, 1fr) minmax(320px, 1.2fr);
      gap: 18px;
      margin-top: 22px;
    }}
    .login-note {{
      font-size: 0.92rem;
      opacity: 0.86;
    }}
    .tabbar {{
      display: flex;
      gap: 12px;
      margin-top: 22px;
      flex-wrap: wrap;
    }}
    .tablink {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 140px;
      padding: 12px 16px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.72);
      color: #102446;
      text-decoration: none;
      font-weight: bold;
      box-shadow: 0 10px 22px rgba(16, 36, 70, 0.08);
    }}
    .tablink.active {{
      background: linear-gradient(180deg, #d93737, #ad2525);
      color: #fff;
    }}
    .market-board {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin-top: 22px;
    }}
    .ticker {{
      padding: 18px;
      border-radius: 20px;
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid var(--line);
      box-shadow: 0 14px 32px rgba(16, 36, 70, 0.08);
    }}
    .ticker h3 {{
      margin-bottom: 8px;
      font-size: 1.1rem;
    }}
    .ticker-meta {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin: 8px 0;
    }}
    .trend-up {{ color: #166534; font-weight: bold; }}
    .trend-down {{ color: #991b1b; font-weight: bold; }}
    .trend-flat {{ color: #334155; font-weight: bold; }}
    .mini {{
      font-size: 0.84rem;
      opacity: 0.82;
      line-height: 1.45;
    }}
    .chart-box {{
      height: 84px;
      margin: 10px 0 6px;
    }}
    .chart-box svg {{
      width: 100%;
      height: 100%;
      display: block;
    }}
    @keyframes banner-in {{
      from {{ opacity: 0; transform: translateY(-16px) scale(0.96); }}
      to {{ opacity: 1; transform: translateY(0) scale(1); }}
    }}
    @keyframes banner-out {{
      to {{ opacity: 0; transform: translateY(-10px) scale(0.98); }}
    }}
    @keyframes burst-spin {{
      from {{ transform: rotate(0deg) scale(1); }}
      50% {{ transform: rotate(180deg) scale(1.08); }}
      to {{ transform: rotate(360deg) scale(1); }}
    }}
    @media (max-width: 860px) {{
      .admin-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 720px) {{
      .wrap {{ width: min(100% - 24px, 1140px); }}
      .hero {{ padding: 24px; }}
      .hero-top {{ grid-template-columns: 1fr; }}
      .logo-shell {{ max-width: 180px; }}
      .topbar {{ flex-direction: column; align-items: flex-start; gap: 10px; }}
    }}
  </style>
</head>
<body>
  {action_html}
  <div class="wrap">
    {topbar}
    {content}
  </div>
</body>
</html>"""


def render_login(message="", error=""):
    content = f"""
    <div class="login-shell">
      <section class="hero">
        <div class="hero-top">
          <div class="logo-shell">
            <img src="/assets/pia-logo.jpeg" alt="Camp logo">
          </div>
          <div>
            <span class="pill">Staff Access</span>
            <span class="hero-tagline">Camp Wallet Login</span>
            <h1>Camp Card System</h1>
            <p class="sub">Leaders and under leaders must sign in before using the wallet tools.</p>
          </div>
        </div>
      </section>
      {f'<div class="notice success">{html.escape(message)}</div>' if message else ''}
      {f'<div class="notice error">{html.escape(error)}</div>' if error else ''}
      <section class="card" style="margin-top: 22px;">
        <h2>Staff Login</h2>
        <form method="post" action="/login">
          <label for="username">Username</label>
          <input id="username" name="username" required>
          <label for="password">Password</label>
          <input id="password" name="password" type="password" required>
          <button type="submit">Log In</button>
        </form>
        <p class="login-note">Main admin account: <strong>{ADMIN_USERNAME}</strong> with password <strong>{ADMIN_PASSWORD}</strong>.</p>
      </section>
    </div>
    """
    return page_template(content, message=message, error=error)


def render_home(user, message="", error="", action="", tab=""):
    active_tab = tab if tab in {"bank", "stocks"} else ("stocks" if action.startswith("market_") else "bank")
    conn = get_db()
    camper_count = conn.execute(
        "SELECT COUNT(*) AS count FROM campers WHERE active = 1"
    ).fetchone()["count"]
    total_balance = conn.execute(
        "SELECT COALESCE(SUM(balance), 0) AS total FROM campers WHERE active = 1"
    ).fetchone()["total"]
    transaction_count = conn.execute("SELECT COUNT(*) AS count FROM transactions").fetchone()["count"]
    campers = conn.execute(
        """
        SELECT c.*,
               (
                   SELECT created_at
                   FROM transactions t
                   WHERE t.camper_id = c.id
                   ORDER BY t.id DESC
                   LIMIT 1
               ) AS last_activity
        FROM campers c
        WHERE c.active = 1
        ORDER BY c.name COLLATE NOCASE
        """
    ).fetchall()
    recent_transactions = conn.execute(
        """
        SELECT t.*, c.name, c.card_number
        FROM transactions t
        JOIN campers c ON c.id = t.camper_id
        ORDER BY t.id DESC
        LIMIT 15
        """
    ).fetchall()
    staff_users = conn.execute(
        "SELECT username, role, active, created_at FROM staff_users ORDER BY username COLLATE NOCASE"
    ).fetchall()
    action_logs = conn.execute(
        """
        SELECT action_log.*, staff_users.username
        FROM action_log
        JOIN staff_users ON staff_users.id = action_log.staff_user_id
        ORDER BY action_log.id DESC
        LIMIT 15
        """
    ).fetchall()
    market_assets = conn.execute(
        """
        SELECT symbol, name, sector, current_price, previous_price, updated_at, last_reason
        FROM market_assets
        ORDER BY symbol
        """
    ).fetchall()
    latest_event = conn.execute(
        """
        SELECT *
        FROM market_events
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_market_updates = conn.execute(
        """
        SELECT asset_symbol, price, reason, source, created_at
        FROM market_snapshots
        ORDER BY id DESC
        LIMIT 8
        """
    ).fetchall()
    top_positions = conn.execute(
        """
        SELECT campers.name, market_positions.asset_symbol, market_positions.shares
        FROM market_positions
        JOIN campers ON campers.id = market_positions.camper_id
        WHERE campers.active = 1 AND market_positions.shares > 0
        ORDER BY market_positions.shares DESC, campers.name COLLATE NOCASE
        LIMIT 10
        """
    ).fetchall()
    live_assets = []
    history_by_symbol = {}
    for row in market_assets:
        live_price = live_market_price(row)
        history_points = [{"price": item["price"], "created_at": item["created_at"]} for item in get_market_history(conn, row["symbol"], limit=11)]
        history_points.append({"price": live_price, "created_at": now()})
        history_by_symbol[row["symbol"]] = history_points[-12:]
        live_assets.append({**dict(row), "live_price": live_price})
    conn.close()

    camper_rows = "".join(
        f"""
        <tr>
          <td>{html.escape(row["name"])}</td>
          <td>{row["age"]}</td>
          <td>{html.escape(row["card_number"])}</td>
          <td>{money(row["balance"])}</td>
          <td>{html.escape(row["last_activity"] or "No activity yet")}</td>
          <td class="action-cell">
            <form method="post" action="/campers/remove" class="compact-form">
              <input type="hidden" name="card_number" value="{html.escape(row["card_number"])}">
              <button type="submit" class="danger-button">Remove</button>
            </form>
          </td>
        </tr>
        """
        for row in campers
    ) or '<tr><td colspan="6">No campers added yet.</td></tr>'

    transaction_rows = "".join(
        f"""
        <tr>
          <td>{html.escape(row["created_at"])}</td>
          <td>{html.escape(row["actor_username"] or "Unknown")}</td>
          <td>{html.escape(row["name"])}</td>
          <td>{html.escape(row["card_number"])}</td>
          <td>{html.escape(row["kind"])}</td>
          <td>{money(row["amount"])}</td>
          <td>{html.escape(row["note"])}</td>
        </tr>
        """
        for row in recent_transactions
    ) or '<tr><td colspan="7">No transactions recorded yet.</td></tr>'

    ticker_cards = "".join(
        f"""
        <div class="ticker">
          <span class="pill" style="background:#102446;">{html.escape(row["symbol"])}</span>
          <h3>{html.escape(row["name"])}</h3>
          <div class="ticker-meta">
            <strong>{money(row["live_price"])}</strong>
            <span class="{
                'trend-up' if row['live_price'] > row['previous_price']
                else 'trend-down' if row['live_price'] < row['previous_price']
                else 'trend-flat'
            }">
              {
                '+' if row['live_price'] > row['previous_price'] else ''
              }{number(row["live_price"] - row["previous_price"])}
            </span>
          </div>
          <div class="chart-box">{market_chart_svg(history_by_symbol[row["symbol"]], "#c72d2d" if row["live_price"] >= row["previous_price"] else "#1d4ed8")}</div>
          <div class="mini"><strong>Sector:</strong> {html.escape(row["sector"])}</div>
          <div class="mini"><strong>Live drift:</strong> small moves every {LIVE_MARKET_BUCKET_MINUTES} minutes</div>
          <div class="mini"><strong>Big swing anchor:</strong> {html.escape(row["updated_at"])}</div>
          <div class="mini">{html.escape(row["last_reason"] or "No hype note yet.")}</div>
        </div>
        """
        for row in live_assets
    )
    market_update_rows = "".join(
        f"""
        <tr>
          <td>{html.escape(row["created_at"])}</td>
          <td>{html.escape(row["asset_symbol"])}</td>
          <td>{money(row["price"])}</td>
          <td>{html.escape(row["source"])}</td>
          <td>{html.escape(row["reason"])}</td>
        </tr>
        """
        for row in latest_market_updates
    ) or '<tr><td colspan="5">No market updates yet.</td></tr>'
    top_position_rows = "".join(
        f"""
        <tr>
          <td>{html.escape(row["name"])}</td>
          <td>{html.escape(row["asset_symbol"])}</td>
          <td>{number(row["shares"])}</td>
        </tr>
        """
        for row in top_positions
    ) or '<tr><td colspan="3">No student holdings yet.</td></tr>'

    admin_markup = ""
    if user["role"] == "ADMIN":
        staff_rows = "".join(
            f"""
            <tr>
              <td>{html.escape(row["username"])}</td>
              <td>{html.escape(row["role"])}</td>
              <td>{"Active" if row["active"] else "Disabled"}</td>
              <td>{html.escape(row["created_at"])}</td>
            </tr>
            """
            for row in staff_users
        ) or '<tr><td colspan="4">No staff accounts yet.</td></tr>'
        log_rows = "".join(
            f"""
            <tr>
              <td>{html.escape(row["created_at"])}</td>
              <td>{html.escape(row["username"])}</td>
              <td>{html.escape(row["action_type"])}</td>
              <td>{html.escape(row["details"])}</td>
            </tr>
            """
            for row in action_logs
        ) or '<tr><td colspan="4">No staff activity yet.</td></tr>'
        admin_markup = f"""
        <section class="admin-grid">
          <div class="card">
            <h2>Create Staff Login</h2>
            <form method="post" action="/staff/create">
              <label for="staff_username">Username</label>
              <input id="staff_username" name="username" required>
              <label for="staff_password">Password</label>
              <input id="staff_password" name="password" type="password" required>
              <label for="staff_role">Role</label>
              <select id="staff_role" name="role" required>
                <option value="LEADER">Leader</option>
                <option value="UNDER LEADER">Under Leader</option>
              </select>
              <button type="submit">Create Staff Account</button>
            </form>
            <p class="tiny">Only the main admin account can create new leader and under leader logins.</p>
          </div>

          <div class="card">
            <h2>Staff Accounts</h2>
            <table>
              <thead>
                <tr>
                  <th>Username</th>
                  <th>Role</th>
                  <th>Status</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>{staff_rows}</tbody>
            </table>
          </div>
        </section>

        <section class="card" style="margin-top: 22px;">
          <h2>Leader Activity Log</h2>
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Staff</th>
                <th>Action</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>{log_rows}</tbody>
          </table>
        </section>
        """

    bank_content = f"""
    <section class="grid">
      <div class="card">
        <h2>Add Camper Card</h2>
        <form method="post" action="/campers/add">
          <label for="name">Camper Name</label>
          <input id="name" name="name" required>
          <label for="age">Age</label>
          <input id="age" name="age" type="number" min="1" max="18" required>
          <label for="card_number">RFID Card Number</label>
          <input id="card_number" name="card_number" required>
          <label for="starting_balance">Starting Balance</label>
          <input id="starting_balance" name="starting_balance" type="number" min="0" step="0.01" value="{STARTING_WEEKLY_BALANCE:.2f}" required>
          <button type="submit">Create Camper</button>
        </form>
      </div>

      <div class="card">
        <h2>Charge Card</h2>
        <form method="post" action="/transactions/charge">
          <label for="charge_card_number">RFID Card Number</label>
          <input id="charge_card_number" name="card_number" required>
          <label for="charge_amount">Charge Amount</label>
          <input id="charge_amount" name="amount" type="number" min="0.01" step="0.01" required>
          <label for="charge_note">What Was Purchased?</label>
          <input id="charge_note" name="note" placeholder="Snack, soda, craft item..." required>
          <button type="submit">Charge Camper</button>
        </form>
      </div>

      <div class="card">
        <h2>Add Money</h2>
        <form method="post" action="/transactions/add-funds">
          <label for="fund_card_number">RFID Card Number</label>
          <input id="fund_card_number" name="card_number" required>
          <label for="fund_amount">Amount to Add</label>
          <input id="fund_amount" name="amount" type="number" min="0.01" step="0.01" required>
          <label for="fund_note">Reason</label>
          <input id="fund_note" name="note" value="Manual top-up" required>
          <button type="submit">Add Funds</button>
        </form>
      </div>

      <div class="card">
        <h2>Remove Camper Card</h2>
        <form method="post" action="/campers/remove">
          <label for="remove_card_number">RFID Card Number</label>
          <input id="remove_card_number" name="card_number" required>
          <button type="submit" class="danger-button">Remove Camper</button>
        </form>
        <p class="tiny">Removing a camper hides the account from active use but keeps old transactions for records.</p>
      </div>

      <div class="card">
        <h2>Transfer Between Cards</h2>
        <form method="post" action="/transactions/transfer">
          <label for="from_card_number">From RFID Card</label>
          <input id="from_card_number" name="from_card_number" required>
          <label for="to_card_number">To RFID Card</label>
          <input id="to_card_number" name="to_card_number" required>
          <label for="transfer_amount">Transfer Amount</label>
          <input id="transfer_amount" name="amount" type="number" min="0.01" step="0.01" required>
          <label for="transfer_note">Reason</label>
          <input id="transfer_note" name="note" value="Camper to camper transfer" required>
          <button type="submit">Transfer Money</button>
        </form>
        <p class="tiny">Use this when two campers are together and one wants to move part of their balance to the other.</p>
      </div>

      <div class="card">
        <h2>Replace Lost Card</h2>
        <form method="post" action="/campers/replace-card">
          <label for="replace_name">Camper Name</label>
          <input id="replace_name" name="name" required>
          <label for="replace_card_number">New RFID Card Number</label>
          <input id="replace_card_number" name="new_card_number" required>
          <button type="submit">Assign New Card</button>
        </form>
        <p class="tiny">Use this if a camper loses a card. Find them by name and scan or type the new card number.</p>
      </div>

      <div class="card">
        <h2>Find Camper By Card</h2>
        <form method="get" action="/lookup">
          <label for="lookup_card_number">RFID Card Number</label>
          <input id="lookup_card_number" name="card_number" required>
          <button type="submit">Lookup Card</button>
        </form>
        <p class="tiny">This is useful when a staff member scans or types a card number and wants to confirm the right camper before charging.</p>
      </div>

      <div class="card">
        <h2>Start New Week</h2>
        <form method="post" action="/weekly-reset">
          <label for="weekly_amount">Reset Every Camper To</label>
          <input id="weekly_amount" name="weekly_amount" type="number" min="0" step="0.01" value="{STARTING_WEEKLY_BALANCE:.2f}" required>
          <button type="submit">Reset All Active Campers</button>
        </form>
        <p class="tiny">Use this at the start of each camp week to give each camper the same fresh balance.</p>
      </div>
    </section>

    {admin_markup}

    <section class="card" style="margin-top: 22px;">
      <h2>Campers</h2>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Age</th>
            <th>Card Number</th>
            <th>Balance</th>
            <th>Last Activity</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>{camper_rows}</tbody>
      </table>
    </section>

    <section class="card" style="margin-top: 22px;">
      <h2>Recent Transactions</h2>
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Staff</th>
            <th>Camper</th>
            <th>Card</th>
            <th>Type</th>
            <th>Amount</th>
            <th>Note</th>
          </tr>
        </thead>
        <tbody>{transaction_rows}</tbody>
      </table>
    </section>
    """

    stocks_content = f"""
    <section class="market-board">
      {ticker_cards}
    </section>

    <section class="grid">
      <div class="card">
        <h2>Buy Stock By Card</h2>
        <form method="post" action="/market/buy">
          <label for="buy_card_number">RFID Card Number</label>
          <input id="buy_card_number" name="card_number" required>
          <label for="buy_symbol">Market</label>
          <select id="buy_symbol" name="symbol" required>
            <option value="PIA">PIA</option>
            <option value="OIL">OIL</option>
            <option value="GOLD">GOLD</option>
            <option value="TECH">TECH</option>
          </select>
          <label for="buy_shares">Shares</label>
          <input id="buy_shares" name="shares" type="number" min="0.01" step="0.01" required>
          <button type="submit">Buy Shares</button>
        </form>
        <p class="tiny">Students buy shares using the same wallet balance already tied to their RFID card.</p>
      </div>

      <div class="card">
        <h2>Sell Stock By Card</h2>
        <form method="post" action="/market/sell">
          <label for="sell_card_number">RFID Card Number</label>
          <input id="sell_card_number" name="card_number" required>
          <label for="sell_symbol">Market</label>
          <select id="sell_symbol" name="symbol" required>
            <option value="PIA">PIA</option>
            <option value="OIL">OIL</option>
            <option value="GOLD">GOLD</option>
            <option value="TECH">TECH</option>
          </select>
          <label for="sell_shares">Shares</label>
          <input id="sell_shares" name="shares" type="number" min="0.01" step="0.01" required>
          <button type="submit">Sell Shares</button>
        </form>
        <p class="tiny">Selling returns the live market value back to the camper's balance instantly.</p>
      </div>
    </section>

    {admin_markup}

    <section class="admin-grid">
      <div class="card">
        <h2>Camp Market Pulse</h2>
        <p><strong>Latest hype note:</strong> {html.escape(latest_event["summary"] if latest_event else "No hype update has been submitted yet.")}</p>
        <p class="mini">Leaders can save the vibe of the day, and the market uses those answers to create a big game-style swing every 12 hours. Between those swings, prices drift a little on their own to keep the board feeling alive.</p>
        <form method="post" action="/market/event">
          <label for="market_summary">Hype Of The Day</label>
          <textarea id="market_summary" name="summary" rows="4" placeholder="Huge color war comeback, wild dining hall buzz, everybody is talking..." required></textarea>
          <label for="energy_level">Camp Buzz (0-100)</label>
          <input id="energy_level" name="energy_level" type="number" min="0" max="100" value="50" required>
          <label for="spirit_level">Overall Hype (0-100)</label>
          <input id="spirit_level" name="spirit_level" type="number" min="0" max="100" value="50" required>
          <label for="weather_score">Weather Chaos (0-100)</label>
          <input id="weather_score" name="weather_score" type="number" min="0" max="100" value="50" required>
          <label for="competition_score">Rivalry Pressure (0-100)</label>
          <input id="competition_score" name="competition_score" type="number" min="0" max="100" value="50" required>
          <button type="submit">Save Hype Inputs</button>
        </form>
        <form method="post" action="/market/refresh" style="margin-top: 12px;">
          <button type="submit">Trigger Big 12-Hour Swing</button>
        </form>
        <p class="tiny">The market is designed to swing big every 12 hours. If refreshed early, the app keeps the current prices and tells staff when the next swing window opens.</p>
      </div>

      <div class="card">
        <h2>Top Student Holdings</h2>
        <table>
          <thead>
            <tr>
              <th>Camper</th>
              <th>Market</th>
              <th>Shares</th>
            </tr>
          </thead>
          <tbody>{top_position_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="card" style="margin-top: 22px;">
      <h2>Market Update History</h2>
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Market</th>
            <th>Price</th>
            <th>Source</th>
            <th>Reason</th>
          </tr>
        </thead>
        <tbody>{market_update_rows}</tbody>
      </table>
    </section>
    """
    content = f"""
    <section class="hero">
      <div class="hero-top">
        <div class="logo-shell">
          <img src="/assets/pia-logo.jpeg" alt="Camp logo">
        </div>
        <div>
          <span class="pill">RFID Camp Wallet</span>
          <span class="hero-tagline">Weekly Spending Dashboard</span>
          <h1>Camp Card System</h1>
          <p class="sub">Use the tabs below to keep the camper bank tools separate from the camp stock market. Campers start with {money(STARTING_WEEKLY_BALANCE)} each, and the market swings hard every 12 hours with small live moves in between.</p>
        </div>
      </div>
    </section>

    <div class="tabbar">
      <a class="tablink {'active' if active_tab == 'bank' else ''}" href="/?tab=bank">Bank</a>
      <a class="tablink {'active' if active_tab == 'stocks' else ''}" href="/?tab=stocks">Stocks</a>
    </div>

    <section class="stats">
      <div class="stat"><strong>Active Campers</strong><br>{camper_count}</div>
      <div class="stat"><strong>Total Stored Balance</strong><br>{money(total_balance)}</div>
      <div class="stat"><strong>Total Transactions</strong><br>{transaction_count}</div>
      <div class="stat"><strong>Default Weekly Amount</strong><br>{money(STARTING_WEEKLY_BALANCE)}</div>
    </section>

    {bank_content if active_tab == 'bank' else stocks_content}
    """
    return page_template(content, user=user, message=message, error=error, action=action)


def render_lookup(user, card_number):
    conn = get_db()
    camper = conn.execute(
        "SELECT * FROM campers WHERE card_number = ? AND active = 1",
        (card_number,),
    ).fetchone()
    transactions = []
    if camper:
        transactions = conn.execute(
            """
            SELECT *
            FROM transactions
            WHERE camper_id = ?
            ORDER BY id DESC
            LIMIT 10
            """,
            (camper["id"],),
        ).fetchall()
    conn.close()

    if not camper:
        return page_template(
            f"""
            <section class="card">
              <h2>Card Lookup</h2>
              <p>No active camper found for card number <strong>{html.escape(card_number)}</strong>.</p>
              <p><a href="/">Back to dashboard</a></p>
            </section>
            """,
            user=user,
            error="Card number not found.",
        )

    transaction_rows = "".join(
        f"""
        <tr>
          <td>{html.escape(row["created_at"])}</td>
          <td>{html.escape(row["actor_username"] or "Unknown")}</td>
          <td>{html.escape(row["kind"])}</td>
          <td>{money(row["amount"])}</td>
          <td>{html.escape(row["note"])}</td>
        </tr>
        """
        for row in transactions
    ) or '<tr><td colspan="5">No transactions yet.</td></tr>'

    content = f"""
    <section class="card">
      <h2>Camper Found</h2>
      <p><strong>Name:</strong> {html.escape(camper["name"])}</p>
      <p><strong>Age:</strong> {camper["age"]}</p>
      <p><strong>RFID Card:</strong> {html.escape(camper["card_number"])}</p>
      <p><strong>Current Balance:</strong> {money(camper["balance"])}</p>
      <p><a href="/">Back to dashboard</a></p>
    </section>
    <section class="card" style="margin-top: 22px;">
      <h2>Recent Activity</h2>
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Staff</th>
            <th>Type</th>
            <th>Amount</th>
            <th>Note</th>
          </tr>
        </thead>
        <tbody>{transaction_rows}</tbody>
      </table>
    </section>
    """
    return page_template(content, user=user, message="Card lookup successful.")


def get_camper_by_card(conn, card_number):
    return conn.execute(
        "SELECT * FROM campers WHERE card_number = ? AND active = 1",
        (card_number,),
    ).fetchone()


def get_camper_by_name(conn, name):
    return conn.execute(
        """
        SELECT *
        FROM campers
        WHERE LOWER(name) = LOWER(?) AND active = 1
        ORDER BY id DESC
        LIMIT 1
        """,
        (name,),
    ).fetchone()


def make_card_available(conn, card_number):
    existing = conn.execute(
        "SELECT * FROM campers WHERE card_number = ? ORDER BY id DESC LIMIT 1",
        (card_number,),
    ).fetchone()
    if not existing:
        return True, ""
    if existing["active"]:
        return False, "That RFID card number is already assigned to another active camper."
    conn.execute(
        "UPDATE campers SET card_number = ? WHERE id = ?",
        (archived_card_number(card_number, existing["id"]), existing["id"]),
    )
    return True, ""


def insert_transaction(conn, camper_id, kind, amount, note, actor_username):
    conn.execute(
        """
        INSERT INTO transactions (camper_id, kind, amount, note, created_at, actor_username)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (camper_id, kind, amount, note, now(), actor_username),
    )


def parse_score(value, label):
    try:
        number_value = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a whole number between 0 and 100.")
    if number_value < 0 or number_value > 100:
        raise ValueError(f"{label} must be between 0 and 100.")
    return number_value


def get_market_asset(conn, symbol):
    return conn.execute(
        "SELECT * FROM market_assets WHERE symbol = ?",
        (symbol.upper(),),
    ).fetchone()


def get_position(conn, camper_id, symbol):
    return conn.execute(
        """
        SELECT *
        FROM market_positions
        WHERE camper_id = ? AND asset_symbol = ?
        """,
        (camper_id, symbol.upper()),
    ).fetchone()


def upsert_position(conn, camper_id, symbol, shares):
    existing = get_position(conn, camper_id, symbol)
    timestamp = now()
    if existing:
        conn.execute(
            "UPDATE market_positions SET shares = ?, updated_at = ? WHERE id = ?",
            (shares, timestamp, existing["id"]),
        )
    else:
        conn.execute(
            """
            INSERT INTO market_positions (camper_id, asset_symbol, shares, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (camper_id, symbol.upper(), shares, timestamp, timestamp),
        )


def get_latest_market_event(conn):
    return conn.execute(
        "SELECT * FROM market_events ORDER BY id DESC LIMIT 1"
    ).fetchone()


def snapshot_reason(event):
    if not event:
        return "No hype submitted yet."
    return event["summary"][:140]


def stable_wave(symbol, anchor_text, bucket):
    digest = hashlib.sha256(f"{symbol}|{anchor_text}|{bucket}".encode("utf-8")).hexdigest()
    n1 = int(digest[:8], 16) / 0xFFFFFFFF
    n2 = int(digest[8:16], 16) / 0xFFFFFFFF
    return (n1 * 2.0 - 1.0) * 0.65 + (n2 * 2.0 - 1.0) * 0.35


def live_market_price(asset, current_time=None):
    current_time = current_time or datetime.now()
    anchor_time = datetime.strptime(asset["updated_at"], "%Y-%m-%d %H:%M:%S")
    elapsed_minutes = max(0, (current_time - anchor_time).total_seconds() / 60.0)
    bucket = int(elapsed_minutes // LIVE_MARKET_BUCKET_MINUTES)
    if bucket <= 0:
        return round(asset["current_price"], 2)
    phase = min(1.0, elapsed_minutes / (MARKET_REFRESH_HOURS * 60.0))
    wave = stable_wave(asset["symbol"], asset["updated_at"], bucket)
    drift_pct = max(
        -LIVE_MARKET_MAX_SWING_PCT,
        min(LIVE_MARKET_MAX_SWING_PCT, wave * LIVE_MARKET_MAX_SWING_PCT * (0.45 + phase * 0.55)),
    )
    return round(max(5.0, asset["current_price"] * (1 + drift_pct / 100.0)), 2)


def get_market_history(conn, symbol, limit=10):
    rows = conn.execute(
        """
        SELECT price, created_at, source
        FROM market_snapshots
        WHERE asset_symbol = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (symbol, limit),
    ).fetchall()
    return list(reversed(rows))


def market_chart_svg(points, stroke):
    if len(points) < 2:
        return ""
    prices = [point["price"] for point in points]
    min_price = min(prices)
    max_price = max(prices)
    spread = max(max_price - min_price, 1)
    width = 220
    height = 84
    coords = []
    for index, point in enumerate(points):
        x = 8 + (index / max(len(points) - 1, 1)) * (width - 16)
        normalized = (point["price"] - min_price) / spread
        y = height - 10 - normalized * (height - 20)
        coords.append((round(x, 2), round(y, 2)))
    line = " ".join(f"{x},{y}" for x, y in coords)
    area = f"8,{height - 8} " + " ".join(f"{x},{y}" for x, y in coords) + f" {coords[-1][0]},{height - 8}"
    return f"""
    <svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" aria-hidden="true">
      <polygon points="{area}" fill="{stroke}22"></polygon>
      <polyline points="{line}" fill="none" stroke="{stroke}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
    </svg>
    """


def next_refresh_time(conn):
    last_snapshot = conn.execute(
        """
        SELECT created_at
        FROM market_snapshots
        WHERE source != 'seed'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not last_snapshot:
        return None
    last_time = datetime.strptime(last_snapshot["created_at"], "%Y-%m-%d %H:%M:%S")
    return last_time + timedelta(hours=MARKET_REFRESH_HOURS)


def build_rule_based_market(event, assets):
    event = event or {
        "summary": "Quiet camp day",
        "energy_level": 50,
        "spirit_level": 50,
        "weather_score": 50,
        "competition_score": 50,
    }
    buzz = event["energy_level"] - 50
    hype = event["spirit_level"] - 50
    weather = event["weather_score"] - 50
    rivalry = event["competition_score"] - 50
    summary = event["summary"]
    summary_boost = ((sum(ord(ch) for ch in summary) % 19) - 9) / 10.0
    influences = {
        "PIA": 0.50 * hype + 0.36 * rivalry + 0.14 * buzz + summary_boost,
        "OIL": 0.62 * buzz - 0.18 * weather + 0.20 * rivalry + summary_boost * 1.3,
        "GOLD": -0.25 * buzz + 0.54 * weather + 0.28 * hype - 0.08 * rivalry,
        "TECH": 0.70 * buzz + 0.22 * hype - 0.14 * weather + summary_boost * 1.1,
    }
    updates = []
    for asset in assets:
        swing_seed = stable_wave(asset["symbol"], summary, len(summary))
        delta_pct = influences.get(asset["symbol"], 0.0) + swing_seed * 8.5
        delta_pct = max(-32.0, min(34.0, delta_pct))
        new_price = round(max(5.0, asset["current_price"] * (1 + (delta_pct / 100.0))), 2)
        direction = "jumped" if delta_pct > 0 else "slid"
        reason = f"{asset['symbol']} {direction} after '{summary[:90]}' with a {delta_pct:+.1f}% swing."
        updates.append(
            {
                "symbol": asset["symbol"],
                "price": new_price,
                "reason": reason,
                "source": "major",
            }
        )
    return updates


def apply_market_updates(conn, updates):
    timestamp = now()
    for update in updates:
        current = get_market_asset(conn, update["symbol"])
        conn.execute(
            """
            UPDATE market_assets
            SET previous_price = ?, current_price = ?, updated_at = ?, last_reason = ?
            WHERE symbol = ?
            """,
            (current["current_price"], update["price"], timestamp, update["reason"], update["symbol"]),
        )
        conn.execute(
            """
            INSERT INTO market_snapshots (asset_symbol, price, reason, source, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (update["symbol"], update["price"], update["reason"], update["source"], timestamp),
        )


def handle_market_event(user, data):
    summary = data.get("summary", "")
    if not summary:
        return render_home(user, error="A hype of the day summary is required before the market can react.", tab="stocks")
    try:
        energy_level = parse_score(data.get("energy_level"), "Camp energy")
        spirit_level = parse_score(data.get("spirit_level"), "Camp spirit")
        weather_score = parse_score(data.get("weather_score"), "Weather score")
        competition_score = parse_score(data.get("competition_score"), "Competition pressure")
    except ValueError as exc:
        return render_home(user, error=str(exc), tab="stocks")
    conn = get_db()
    conn.execute(
        """
        INSERT INTO market_events (summary, energy_level, spirit_level, weather_score, competition_score, submitted_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (summary, energy_level, spirit_level, weather_score, competition_score, user["username"], now()),
    )
    log_action(conn, user, "market_event", f"Saved hype inputs: {summary[:80]}")
    conn.commit()
    conn.close()
    return render_home(user, message="Saved today's hype inputs.", action="market_event", tab="stocks")


def handle_market_refresh(user):
    conn = get_db()
    next_time = next_refresh_time(conn)
    if next_time and datetime.now() < next_time:
        wait_text = next_time.strftime("%Y-%m-%d %H:%M:%S")
        conn.close()
        return render_home(user, error=f"The next big market swing window opens at {wait_text}.", tab="stocks")
    assets = conn.execute("SELECT * FROM market_assets ORDER BY symbol").fetchall()
    event = get_latest_market_event(conn)
    updates = build_rule_based_market(event, assets)
    apply_market_updates(conn, updates)
    log_action(conn, user, "market_refresh", f"Triggered a major market swing using the hype inputs")
    conn.commit()
    conn.close()
    return render_home(user, message="Market prices exploded into a new 12-hour swing.", action="market_refresh", tab="stocks")


def handle_trade(user, data, side):
    card_number = data.get("card_number", "")
    symbol = data.get("symbol", "").upper()
    shares_raw = data.get("shares", "")
    if not card_number or not symbol or not shares_raw:
        return render_home(user, error="Card number, market symbol, and shares are all required.", tab="stocks")
    try:
        shares = float(shares_raw)
    except ValueError:
        return render_home(user, error="Shares must be a valid number.", tab="stocks")
    if shares <= 0:
        return render_home(user, error="Shares must be greater than zero.", tab="stocks")
    conn = get_db()
    camper = get_camper_by_card(conn, card_number)
    asset = get_market_asset(conn, symbol)
    if not camper:
        conn.close()
        return render_home(user, error="No active camper was found for that card number.", tab="stocks")
    if not asset:
        conn.close()
        return render_home(user, error="That market symbol does not exist.", tab="stocks")
    live_price = live_market_price(asset)
    cost = round(live_price * shares, 2)
    position = get_position(conn, camper["id"], symbol)
    held_shares = position["shares"] if position else 0
    if side == "buy":
        if camper["balance"] < cost:
            conn.close()
            return render_home(user, error=f"{camper['name']} does not have enough balance to buy {number(shares)} shares of {symbol}.", tab="stocks")
        conn.execute("UPDATE campers SET balance = ? WHERE id = ?", (camper["balance"] - cost, camper["id"]))
        upsert_position(conn, camper["id"], symbol, held_shares + shares)
        insert_transaction(conn, camper["id"], "market_buy", cost, f"Bought {number(shares)} shares of {symbol} at {money(live_price)}", user["username"])
        log_action(conn, user, "market_buy", f"{camper['name']} bought {number(shares)} shares of {symbol}")
        conn.commit()
        conn.close()
        return render_home(user, message=f"{camper['name']} bought {number(shares)} shares of {symbol}.", action="market_buy", tab="stocks")
    if held_shares < shares:
        conn.close()
        return render_home(user, error=f"{camper['name']} does not own enough {symbol} shares to sell.", tab="stocks")
    conn.execute("UPDATE campers SET balance = ? WHERE id = ?", (camper["balance"] + cost, camper["id"]))
    upsert_position(conn, camper["id"], symbol, held_shares - shares)
    insert_transaction(conn, camper["id"], "market_sell", cost, f"Sold {number(shares)} shares of {symbol} at {money(live_price)}", user["username"])
    log_action(conn, user, "market_sell", f"{camper['name']} sold {number(shares)} shares of {symbol}")
    conn.commit()
    conn.close()
    return render_home(user, message=f"{camper['name']} sold {number(shares)} shares of {symbol}.", action="market_sell", tab="stocks")


def handle_login(environ, start_response):
    data = get_post_data(environ)
    username = data.get("username", "")
    password = data.get("password", "")
    conn = get_db()
    user = conn.execute(
        """
        SELECT *
        FROM staff_users
        WHERE username = ? AND password_hash = ? AND active = 1
        """,
        (username, hash_password(password)),
    ).fetchone()
    if not user:
        conn.close()
        body = render_login(error="Invalid username or password.").encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]
    token = secrets.token_hex(24)
    conn.execute(
        "INSERT INTO auth_sessions (staff_user_id, token, created_at) VALUES (?, ?, ?)",
        (user["id"], token, now()),
    )
    log_action(conn, user, "login", f"{user['username']} logged in")
    conn.commit()
    conn.close()
    headers = [("Content-Type", "text/html; charset=utf-8"), ("Set-Cookie", build_session_cookie(token))]
    body = render_home(user, message=f"Welcome, {user['username']}.").encode("utf-8")
    start_response("200 OK", headers)
    return [body]


def handle_logout(environ, start_response):
    conn = get_db()
    user = get_current_user(conn, environ)
    token = get_cookie_value(environ, "camp_wallet_session")
    if token:
        conn.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
    if user:
        log_action(conn, user, "logout", f"{user['username']} logged out")
    conn.commit()
    conn.close()
    headers = [("Content-Type", "text/html; charset=utf-8"), ("Set-Cookie", clear_session_cookie())]
    body = render_login(message="You have been logged out.").encode("utf-8")
    start_response("200 OK", headers)
    return [body]


def handle_add_camper(user, data):
    name = data.get("name", "")
    age_raw = data.get("age", "")
    card_number = data.get("card_number", "")
    starting_balance_raw = data.get("starting_balance", "")
    if not name or not age_raw or not card_number or not starting_balance_raw:
        return render_home(user, error="All camper fields are required.")
    try:
        age = int(age_raw)
        starting_balance = float(starting_balance_raw)
    except ValueError:
        return render_home(user, error="Age and starting balance must be valid numbers.")

    conn = get_db()
    available, error = make_card_available(conn, card_number)
    if not available:
        conn.close()
        return render_home(user, error=error)
    try:
        cursor = conn.execute(
            """
            INSERT INTO campers (name, age, card_number, balance, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, age, card_number, starting_balance, now()),
        )
        insert_transaction(conn, cursor.lastrowid, "starting_balance", starting_balance, "Camper created", user["username"])
        log_action(conn, user, "create_camper", f"Created {name} with card {card_number}")
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return render_home(user, error="That RFID card number is already assigned to another active camper.")
    conn.close()
    return render_home(user, message=f"Created camper {name} with card {card_number}.", action="create")


def handle_remove_camper(user, data):
    card_number = data.get("card_number", "")
    if not card_number:
        return render_home(user, error="RFID card number is required to remove a camper.")
    conn = get_db()
    camper = get_camper_by_card(conn, card_number)
    if not camper:
        conn.close()
        return render_home(user, error="No active camper was found for that card number.")
    conn.execute(
        "UPDATE campers SET active = 0, card_number = ? WHERE id = ?",
        (archived_card_number(camper["card_number"], camper["id"]), camper["id"]),
    )
    insert_transaction(conn, camper["id"], "removed", 0, "Camper account removed from active use", user["username"])
    log_action(conn, user, "remove_camper", f"Removed {camper['name']}")
    conn.commit()
    conn.close()
    return render_home(user, message=f"Removed {camper['name']} from the active camper list.", action="remove")


def handle_balance_change(user, data, change_type):
    card_number = data.get("card_number", "")
    amount_raw = data.get("amount", "")
    note = data.get("note", "")
    if not card_number or not amount_raw or not note:
        return render_home(user, error="Card number, amount, and note are required.")
    try:
        amount = float(amount_raw)
    except ValueError:
        return render_home(user, error="Amount must be a valid number.")
    if amount <= 0:
        return render_home(user, error="Amount must be greater than zero.")
    conn = get_db()
    camper = get_camper_by_card(conn, card_number)
    if not camper:
        conn.close()
        return render_home(user, error="No active camper was found for that card number.")
    signed_amount = amount if change_type == "add_funds" else -amount
    new_balance = camper["balance"] + signed_amount
    if new_balance < 0:
        conn.close()
        return render_home(user, error=f"{camper['name']} does not have enough money for that charge.")
    conn.execute("UPDATE campers SET balance = ? WHERE id = ?", (new_balance, camper["id"]))
    insert_transaction(conn, camper["id"], change_type, amount, note, user["username"])
    log_action(conn, user, change_type, f"{change_type} {money(amount)} for {camper['name']}")
    conn.commit()
    conn.close()
    if change_type == "add_funds":
        return render_home(user, message=f"Added {money(amount)} to {camper['name']}.", action="add_funds")
    return render_home(user, message=f"Charged {camper['name']} {money(amount)} for {note}.", action="charge")


def handle_transfer(user, data):
    from_card_number = data.get("from_card_number", "")
    to_card_number = data.get("to_card_number", "")
    amount_raw = data.get("amount", "")
    note = data.get("note", "")
    if not from_card_number or not to_card_number or not amount_raw or not note:
        return render_home(user, error="Both card numbers, amount, and reason are required for a transfer.")
    if from_card_number == to_card_number:
        return render_home(user, error="Transfer source and destination cards must be different.")
    try:
        amount = float(amount_raw)
    except ValueError:
        return render_home(user, error="Transfer amount must be a valid number.")
    if amount <= 0:
        return render_home(user, error="Transfer amount must be greater than zero.")
    conn = get_db()
    from_camper = get_camper_by_card(conn, from_card_number)
    to_camper = get_camper_by_card(conn, to_card_number)
    if not from_camper or not to_camper:
        conn.close()
        return render_home(user, error="Both campers must have active card numbers before you can transfer money.")
    if from_camper["balance"] < amount:
        conn.close()
        return render_home(user, error=f"{from_camper['name']} does not have enough money for that transfer.")
    conn.execute("UPDATE campers SET balance = ? WHERE id = ?", (from_camper["balance"] - amount, from_camper["id"]))
    conn.execute("UPDATE campers SET balance = ? WHERE id = ?", (to_camper["balance"] + amount, to_camper["id"]))
    insert_transaction(conn, from_camper["id"], "transfer_out", amount, f"To {to_camper['name']} ({to_camper['card_number']}): {note}", user["username"])
    insert_transaction(conn, to_camper["id"], "transfer_in", amount, f"From {from_camper['name']} ({from_camper['card_number']}): {note}", user["username"])
    log_action(conn, user, "transfer", f"Transferred {money(amount)} from {from_camper['name']} to {to_camper['name']}")
    conn.commit()
    conn.close()
    return render_home(user, message=f"Transferred {money(amount)} from {from_camper['name']} to {to_camper['name']}.", action="transfer")


def handle_weekly_reset(user, data):
    weekly_amount_raw = data.get("weekly_amount", "")
    if not weekly_amount_raw:
        return render_home(user, error="Weekly reset amount is required.")
    try:
        weekly_amount = float(weekly_amount_raw)
    except ValueError:
        return render_home(user, error="Weekly amount must be a valid number.")
    if weekly_amount < 0:
        return render_home(user, error="Weekly amount cannot be negative.")
    conn = get_db()
    campers = conn.execute("SELECT * FROM campers WHERE active = 1").fetchall()
    for camper in campers:
        conn.execute("UPDATE campers SET balance = ? WHERE id = ?", (weekly_amount, camper["id"]))
        insert_transaction(conn, camper["id"], "weekly_reset", weekly_amount, f"Weekly reset to {money(weekly_amount)}", user["username"])
    conn.execute("DELETE FROM market_positions")
    reset_time = now()
    conn.execute(
        """
        UPDATE market_assets
        SET previous_price = ?, current_price = ?, updated_at = ?, last_reason = ?
        """,
        (DEFAULT_MARKET_PRICE, DEFAULT_MARKET_PRICE, reset_time, "Fresh week reset"),
    )
    conn.execute("DELETE FROM market_snapshots")
    for symbol in ["PIA", "OIL", "GOLD", "TECH"]:
        conn.execute(
            """
            INSERT INTO market_snapshots (asset_symbol, price, reason, source, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (symbol, DEFAULT_MARKET_PRICE, "Fresh week reset", "reset", reset_time),
        )
    log_action(conn, user, "weekly_reset", f"Reset {len(campers)} campers to {money(weekly_amount)} and restarted the market")
    conn.commit()
    conn.close()
    return render_home(
        user,
        message=f"Reset {len(campers)} campers to {money(weekly_amount)} and restarted the market for a fresh week.",
        action="weekly_reset",
    )


def handle_replace_card(user, data):
    camper_name = data.get("name", "")
    new_card_number = data.get("new_card_number", "")
    if not camper_name or not new_card_number:
        return render_home(user, error="Camper name and new RFID card number are required.")
    conn = get_db()
    camper = get_camper_by_name(conn, camper_name)
    if not camper:
        conn.close()
        return render_home(user, error="No active camper was found with that name.")
    available, error = make_card_available(conn, new_card_number)
    if not available:
        conn.close()
        return render_home(user, error=error)
    old_card_number = camper["card_number"]
    conn.execute(
        "UPDATE campers SET card_number = ? WHERE id = ?",
        (new_card_number, camper["id"]),
    )
    insert_transaction(
        conn,
        camper["id"],
        "replace_card",
        0,
        f"Replaced lost card {old_card_number} with new card {new_card_number}",
        user["username"],
    )
    log_action(conn, user, "replace_card", f"Moved {camper['name']} from {old_card_number} to {new_card_number}")
    conn.commit()
    conn.close()
    return render_home(user, message=f"Assigned a new card to {camper['name']}.", action="replace_card")


def handle_create_staff(user, data):
    if user["role"] != "ADMIN":
        return render_home(user, error="Only the main admin account can create staff logins.")
    username = data.get("username", "")
    password = data.get("password", "")
    role = data.get("role", "")
    if not username or not password or role not in {"LEADER", "UNDER LEADER"}:
        return render_home(user, error="Username, password, and a valid role are required.")
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO staff_users (username, password_hash, role, active, created_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (username, hash_password(password), role, now()),
        )
        log_action(conn, user, "create_staff", f"Created {role} account for {username}")
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return render_home(user, error="That username already exists.")
    conn.close()
    return render_home(user, message=f"Created staff login for {username}.", action="staff_created")


def application(environ, start_response):
    init_db()
    method = environ["REQUEST_METHOD"]
    path = environ.get("PATH_INFO", "/")
    query = parse_qs(environ.get("QUERY_STRING", ""))

    if method == "GET" and path.startswith("/assets/"):
        asset_path = os.path.join(BASE_DIR, path.lstrip("/"))
        if os.path.isfile(asset_path):
            content_type = mimetypes.guess_type(asset_path)[0] or "application/octet-stream"
            with open(asset_path, "rb") as asset_file:
                data = asset_file.read()
            start_response("200 OK", [("Content-Type", content_type)])
            return [data]
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Asset not found"]

    if method == "GET" and path == "/login":
        body = render_login().encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/login":
        return handle_login(environ, start_response)

    conn = get_db()
    user = get_current_user(conn, environ)
    conn.close()
    if not user:
        body = render_login(error="Please log in to continue.").encode("utf-8")
        headers = [("Content-Type", "text/html; charset=utf-8"), ("Set-Cookie", clear_session_cookie())]
        start_response("200 OK", headers)
        return [body]

    if method == "POST" and path == "/logout":
        return handle_logout(environ, start_response)

    if method == "GET" and path == "/":
        selected_tab = (query.get("tab", ["bank"])[0] or "bank").strip().lower()
        body = render_home(user, tab=selected_tab).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "GET" and path == "/lookup":
        card_number = (query.get("card_number", [""])[0]).strip()
        body = render_lookup(user, card_number).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/campers/add":
        body = handle_add_camper(user, get_post_data(environ)).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/campers/remove":
        body = handle_remove_camper(user, get_post_data(environ)).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/campers/replace-card":
        body = handle_replace_card(user, get_post_data(environ)).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/transactions/charge":
        body = handle_balance_change(user, get_post_data(environ), "charge").encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/transactions/add-funds":
        body = handle_balance_change(user, get_post_data(environ), "add_funds").encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/transactions/transfer":
        body = handle_transfer(user, get_post_data(environ)).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/weekly-reset":
        body = handle_weekly_reset(user, get_post_data(environ)).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/market/event":
        body = handle_market_event(user, get_post_data(environ)).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/market/refresh":
        body = handle_market_refresh(user).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/market/buy":
        body = handle_trade(user, get_post_data(environ), "buy").encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/market/sell":
        body = handle_trade(user, get_post_data(environ), "sell").encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if method == "POST" and path == "/staff/create":
        body = handle_create_staff(user, get_post_data(environ)).encode("utf-8")
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [body]

    if path == "/favicon.ico":
        start_response("204 No Content", [])
        return [b""]

    start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
    return [b"Not found"]


if __name__ == "__main__":
    init_db()
    print(f"Camp Card System running at http://127.0.0.1:{PORT}")
    with make_server("127.0.0.1", PORT, application) as server:
        server.serve_forever()
