import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/payment.db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SERVICE_NAME = "payment-service"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# 0.0–1.0 probability to fail charge/refund (demo)
FAILURE_RATE = float(os.getenv("PAYMENT_FAILURE_RATE", "0.0"))
