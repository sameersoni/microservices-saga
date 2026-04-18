import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/order.db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SERVICE_NAME = "order-service"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FAILURE_SIMULATION_URL = os.getenv("FAILURE_SIMULATION", "")  # unused in order
