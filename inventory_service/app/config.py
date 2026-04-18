import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/inventory.db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SERVICE_NAME = "inventory-service"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
INVENTORY_FAILURE_RATE = float(os.getenv("INVENTORY_FAILURE_RATE", "0.0"))
INITIAL_STOCK = int(os.getenv("INITIAL_STOCK", "1000"))
