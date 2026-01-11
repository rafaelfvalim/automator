import os
import threading
import pymysql
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)
_db_initialized = False
_db_lock = threading.Lock()

# Mesma ideia do ThingSpeak
WRITE_KEY = os.environ.get("WRITE_KEY", "YOUR_WRITE_KEY")

# MariaDB (via PyMySQL)
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USER = os.environ.get("DB_USER", "mariadb")
DB_PASS = os.environ.get("DB_PASS", "")
DB_NAME = os.environ.get("DB_NAME", "telemetry")

def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )

def init_db():
    ddl = """
    CREATE TABLE IF NOT EXISTS entries (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      created_at DATETIME(6) NOT NULL,
      api_key VARCHAR(64) NULL,
      status VARCHAR(255) NULL,
      field1 VARCHAR(255) NULL,
      field2 VARCHAR(255) NULL,
      field3 VARCHAR(255) NULL,
      field4 VARCHAR(255) NULL,
      field5 VARCHAR(255) NULL,
      field6 VARCHAR(255) NULL,
      field7 VARCHAR(255) NULL,
      field8 VARCHAR(255) NULL,
      raw_payload TEXT NULL,

      INDEX idx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(ddl)

@app.before_request
def _ensure_db_initialized():
    # Garante que a tabela exista mesmo rodando via gunicorn
    global _db_initialized
    if not _db_initialized:
        with _db_lock:
            if not _db_initialized:
                init_db()
                _db_initialized = True

@app.route("/update", methods=["GET", "POST"])
def update():
    # Aceita querystring, x-www-form-urlencoded e JSON
    data = {}
    data.update(request.args.to_dict(flat=True))
    if request.form:
        data.update(request.form.to_dict(flat=True))
    if request.is_json:
        js = request.get_json(silent=True) or {}
        if isinstance(js, dict):
            data.update(js)

    api_key = (data.get("api_key") or data.get("apikey") or "").strip()
    if not api_key or api_key != WRITE_KEY:
        # ThingSpeak retorna "0" quando falha
        return ("0", 200)

    status = data.get("status")
    fields = [data.get(f"field{i}") for i in range(1, 9)]

    created_at = datetime.now(timezone.utc).replace(tzinfo=None)  # grava como UTC "naive"

    raw_payload = None
    try:
        raw_payload = (request.get_data(as_text=True) or "")[:4000]
    except Exception:
        raw_payload = None

    sql = """
      INSERT INTO entries (
        created_at, api_key, status,
        field1, field2, field3, field4, field5, field6, field7, field8,
        raw_payload
      ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(sql, (
                created_at, api_key, status,
                fields[0], fields[1], fields[2], fields[3],
                fields[4], fields[5], fields[6], fields[7],
                raw_payload
            ))
            entry_id = cur.lastrowid

    return (str(entry_id), 200)

@app.route("/latest", methods=["GET"])
def latest():
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT * FROM entries ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()

    if not row:
        return jsonify({"ok": False, "message": "no data"}), 404
    return jsonify({"ok": True, "data": row}), 200

if __name__ == "__main__":
    # Em dev local
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
