from kafka import KafkaConsumer
import json
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
import logging

# Настройки Keycloak
keycloak_server_url = "http://keycloak:8080/"
keycloak_username = "admin"
keycloak_password = "admin"
keycloak_realm_name = "master"
keycloak_client_id = "admin-cli"
keycloak_client_secret_key = "FQ2khlbqR2AejG2BaW3wfArLlJHotvWQ"

# Настройки Kafka
kafka_topic = 'user_topic'
kafka_bootstrap_servers = ['broker:19092']

# Подключение к Keycloak
keycloak_connection = KeycloakOpenIDConnection(server_url=keycloak_server_url, 
                                               username=keycloak_username, 
                                               password=keycloak_password, 
                                               realm_name=keycloak_realm_name,
                                               client_id=keycloak_client_id,
                                               client_secret_key=keycloak_client_secret_key,
                                               verify=True)
keycloak_admin = KeycloakAdmin(connection=keycloak_connection)

def main():
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        user_data = message.value
        try:
            keycloak_admin.create_user({
                "username": user_data['username'],
                "email": user_data['email'],
                "enabled": True,
                "credentials": [{"value": user_data['password'], "type": "password", "temporary": True}]
            })
            print(f"User created in Keycloak: {user_data['username']}")
        except Exception as e:
            logging.error(f"Error creating user {user_data['username']}: {e}")

if __name__ == "__main__":
    main()
