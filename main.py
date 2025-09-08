import random, string, duckdb, os
from datetime import datetime
from flask import Flask, request, jsonify, redirect, render_template

app = Flask(__name__)

DB_NAME = "urls.duckdb"

def init_db():
    conn = duckdb.connect(DB_NAME)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            codigo TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            created_at TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            id BIGINT PRIMARY KEY,
            link TEXT,
            status TEXT,
            data TIMESTAMP
        )
    """)
    conn.close()

init_db()

@app.route("/")
def home():
    return render_template("form.html")

def gerar_codigo(tamanho=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=tamanho))

@app.route('/encurtar', methods=['POST'])
def encurtar():
    url = request.form.get("url")
    if not url:
        return "URL inválida", 400
    conn = duckdb.connect(DB_NAME)
    codigo = gerar_codigo()
    exists = conn.execute("SELECT 1 FROM urls WHERE codigo = ?", (codigo,)).fetchone()
    while exists:
        codigo = gerar_codigo()
        exists = conn.execute("SELECT 1 FROM urls WHERE codigo = ?", (codigo,)).fetchone()

    conn.execute(
        "INSERT INTO urls (codigo, url, created_at) VALUES (?, ?, ?)",
        (codigo, url, datetime.now())
    )
    conn.close()

    url_encurtada = f"{request.host_url}{codigo}"
    return render_template("result.html", url_encurtada=url_encurtada)

@app.route('/<codigo>')
def redirecionar(codigo):
    conn = duckdb.connect(DB_NAME)
    result = conn.execute("SELECT url FROM urls WHERE codigo = ?", (codigo,)).fetchone()
    conn.close()

    if result:
        return redirect(result[0])
    return jsonify({"erro": "URL não encontrada"}), 404

@app.route("/lista")
def listar():
    conn = duckdb.connect(DB_NAME)
    rows = conn.execute("SELECT codigo, url, created_at FROM urls ORDER BY created_at DESC").fetchall()
    conn.close()

    lista = []
    for r in rows:
        codigo = r[0]
        url = r[1]
        created_at = r[2]

        if hasattr(created_at, "strftime"):
            data_str = created_at.strftime("%d/%m/%Y %H:%M")
        else:
            data_str = str(created_at) if created_at is not None else ""
        lista.append({
            "codigo": codigo,
            "url": url,
            "data": data_str
        })

    return render_template("list.html", lista=lista)

if __name__ == "__main__":
    app.run(debug=True)