import os
import time
from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)

def get_db_connection():
    conn = None
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=os.environ.get("DB_HOST"),
                database=os.environ.get("POSTGRES_DB"),
                user=os.environ.get("POSTGRES_USER"),
                password=os.environ.get("POSTGRES_PASSWORD"))
            return conn
        except psycopg2.OperationalError:
            retries -= 1
            time.sleep(5)
    raise Exception("Could not connect to database")

@app.route('/init_db')
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id SERIAL PRIMARY KEY,
            text VARCHAR(255) NOT NULL,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()
    return "Database initialized"

@app.route('/item', methods=['POST', 'GET'])
def add_item():
    text = None
    if request.method == 'POST':
        data = request.get_json()
        if not data or 'text' not in data:
            return jsonify({"error": "Missing 'text' in request body"}), 400
        text = data['text']
    elif request.method == 'GET':
        text = request.args.get('text')
        if not text:
            return jsonify({"error": "Missing 'text' parameter in URL"}), 400
    
    if text is None:
        return jsonify({"error": "Could not determine text to add"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO items (text) VALUES (%s)", (text,))
    conn.commit()
    cur.close()
    conn.close()
    
    return jsonify({"message": "Item added successfully"}), 201

@app.route('/items', methods=['GET'])
def get_items():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, text, update_time FROM items")
    items = cur.fetchall()
    cur.close()
    conn.close()
    
    items_list = []
    for item in items:
        items_list.append({
            "id": item[0],
            "text": item[1],
            "update_time": item[2].strftime("%Y-%m-%d %H:%M:%S")
        })
        
    return jsonify(items_list)

@app.route('/query', methods=['POST'])
def query_db():
    data = request.get_json()
    if not data or 'sql' not in data:
        return jsonify({"error": "Missing 'sql' in request body"}), 400
    
    sql_query = data['sql']
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(sql_query)
        if sql_query.strip().upper().startswith('SELECT'):
            colnames = [desc[0] for desc in cur.description]
            items = [dict(zip(colnames, row)) for row in cur.fetchall()]
            for item in items:
                for key, value in item.items():
                    if hasattr(value, 'strftime'):
                        item[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            result = jsonify(items)
        else:
            conn.commit()
            result = jsonify({"message": "Query executed successfully", "rows_affected": cur.rowcount})

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()
    
    return result

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000) 