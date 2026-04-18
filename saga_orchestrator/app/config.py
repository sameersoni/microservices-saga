import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/orchestrator.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
SERVICE_NAME = "saga-orchestrator"

ORDER_URL = os.getenv("ORDER_SERVICE_URL", "http://localhost:8000")
PAYMENT_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8001")
INVENTORY_URL = os.getenv("INVENTORY_SERVICE_URL", "http://localhost:8002")
