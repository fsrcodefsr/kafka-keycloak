from kafka import KafkaProducer
import json
import time
import psycopg2

# Настройки PostgreSQL
db_config = {
    "dbname": "testdb",
    "user": "test",
    "password": "test",
    "host": "sourcedb",
    "port": 5432
}

# Настройки Kafka
kafka_topic = 'user_topic'
kafka_bootstrap_servers = ['broker:19092']

def fetch_users():
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, email, password FROM users WHERE imported = FALSE;")
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users

def mark_user_imported(user_id):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET imported = TRUE WHERE id = %s;", (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    while True:
        users = fetch_users()
        for user in users:
            user_id, username, email, password = user
            user_data = {
                "id": user_id,
                "username": username,
                "email": email,
                "password": password
            }
            producer.send(kafka_topic, user_data)
            mark_user_imported(user_id)
            print(f"Sent user data to Kafka: {user_data}")
        time.sleep(60)

if __name__ == "__main__":
    main()
