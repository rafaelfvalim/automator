import os
import threading
import pymysql
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
# Configuração explícita do CORS para permitir todas as origens, métodos e headers
CORS(app, 
     resources={r"/*": {
         "origins": "*",
         "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
         "allow_headers": ["Content-Type", "Authorization"]
     }})
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


def _parse_dt(s: str):
    """
    Aceita:
      - 2026-01-11T18:21:33
      - 2026-01-11T18:21:33.123456
      - 2026-01-11 18:21:33
      - 2026-01-11T18:21:33Z  (tratado como UTC)
    Retorna datetime (naive) ou None.
    """
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1]

    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _to_float(v):
    try:
        return float(v) if v is not None and v != "" else None
    except (ValueError, TypeError):
        return None


def _validate_api_key():
    """
    Valida a api_key da requisição.
    Aceita api_key via querystring, form ou JSON.
    Retorna (True, api_key) se válida, (False, None) caso contrário.
    """
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
        return False, None
    return True, api_key


@app.route("/update", methods=["GET", "POST"])
def update():
    # Valida api_key
    is_valid, api_key = _validate_api_key()
    if not is_valid:
        # ThingSpeak retorna "0" quando falha
        return ("0", 200)
    
    # Aceita querystring, x-www-form-urlencoded e JSON
    data = {}
    data.update(request.args.to_dict(flat=True))
    if request.form:
        data.update(request.form.to_dict(flat=True))
    if request.is_json:
        js = request.get_json(silent=True) or {}
        if isinstance(js, dict):
            data.update(js)

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
            cur.execute(
                sql,
                (
                    created_at,
                    api_key,
                    status,
                    fields[0],
                    fields[1],
                    fields[2],
                    fields[3],
                    fields[4],
                    fields[5],
                    fields[6],
                    fields[7],
                    raw_payload,
                ),
            )
            entry_id = cur.lastrowid

    return (str(entry_id), 200)


@app.route("/latest", methods=["GET"])
def latest():
    # Valida api_key
    is_valid, _ = _validate_api_key()
    if not is_valid:
        return jsonify({"ok": False, "error": "api_key inválida ou ausente"}), 401
    
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT * FROM entries ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()

    if not row:
        return jsonify({"ok": False, "message": "no data"}), 404
    return jsonify({"ok": True, "data": row}), 200


@app.route("/chart", methods=["GET"])
def chart():
    """
    Retorno pronto para gráfico (Chart.js/Plotly):

      /chart?api_key=XXX&last_minutes=60&limit=2000
      /chart?api_key=XXX&start=2026-01-11T18:00:00Z&end=2026-01-11T19:00:00Z&limit=2000

    Mapeamento:
      field1 -> PM1.0
      field2 -> PM2.5
      field3 -> PM10
    """
    # Valida api_key
    is_valid, _ = _validate_api_key()
    if not is_valid:
        return jsonify({"ok": False, "error": "api_key inválida ou ausente"}), 401

    # limite
    limit_raw = request.args.get("limit", "2000")
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 2000
    limit = max(1, min(limit, 2000))

    # filtros
    start_raw = request.args.get("start")
    end_raw = request.args.get("end")
    last_minutes_raw = request.args.get("last_minutes")

    start_dt = _parse_dt(start_raw) if start_raw else None
    end_dt = _parse_dt(end_raw) if end_raw else None

    params = []
    where = ""

    # last_minutes tem precedência se informado
    if last_minutes_raw:
        try:
            last_minutes = int(last_minutes_raw)
        except ValueError:
            return jsonify({"ok": False, "error": "last_minutes inválido"}), 400

        # até 31 dias
        last_minutes = max(1, min(last_minutes, 60 * 24 * 31))

        # created_at é UTC naive; usamos UTC_TIMESTAMP para não depender do timezone do servidor
        where = "WHERE created_at >= (UTC_TIMESTAMP(6) - INTERVAL %s MINUTE)"
        params.append(last_minutes)
    else:
        clauses = []

        if start_raw and not start_dt:
            return jsonify({"ok": False, "error": "start inválido"}), 400
        if end_raw and not end_dt:
            return jsonify({"ok": False, "error": "end inválido"}), 400
        if start_dt and end_dt and start_dt > end_dt:
            return jsonify({"ok": False, "error": "start maior que end"}), 400

        if start_dt:
            clauses.append("created_at >= %s")
            params.append(start_dt)
        if end_dt:
            clauses.append("created_at <= %s")
            params.append(end_dt)

        if clauses:
            where = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT id, created_at, field1, field2, field3
        FROM entries
        {where}
        ORDER BY id DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    # ordem cronológica para plot
    # Converte para lista se necessário (fetchall pode retornar tupla em algumas versões)
    if not isinstance(rows, list):
        rows = list(rows)
    rows.reverse()

    labels = []
    pm1 = []
    pm25 = []
    pm10 = []

    for r in rows:
        ts = r.get("created_at")
        if hasattr(ts, "isoformat"):
            ts_str = ts.isoformat() + "Z"
        else:
            ts_str = str(ts) + "Z"

        labels.append(ts_str)
        pm1.append(_to_float(r.get("field1")))
        pm25.append(_to_float(r.get("field2")))
        pm10.append(_to_float(r.get("field3")))

    return jsonify(
        {
            "ok": True,
            "meta": {
                "tz": "UTC",
                "start": start_raw,
                "end": end_raw,
                "last_minutes": last_minutes_raw,
                "limit": limit,
                "points": len(labels),
            },
            "labels": labels,
            "series": {
                "pm1": pm1,
                "pm25": pm25,
                "pm10": pm10,
            },
        }
    ), 200


if __name__ == "__main__":
    # Em dev local
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
