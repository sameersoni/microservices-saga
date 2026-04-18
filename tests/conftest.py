from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = ROOT / "shared"
SERVICE_DIRS = {
    str(ROOT / "order_service"),
    str(ROOT / "payment_service"),
    str(ROOT / "inventory_service"),
    str(ROOT / "saga_orchestrator"),
}

if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))


def load_service_module(service_dir_name: str, module_name: str):
    """Import a module from one service while isolating `app.*` collisions."""
    service_dir = str(ROOT / service_dir_name)
    sys.path[:] = [p for p in sys.path if p not in SERVICE_DIRS]
    sys.path.insert(0, service_dir)
    if str(SHARED_DIR) not in sys.path:
        sys.path.insert(0, str(SHARED_DIR))

    # Each service uses package name `app`; clear previous imports first.
    for key in list(sys.modules.keys()):
        if key == "app" or key.startswith("app."):
            del sys.modules[key]

    return importlib.import_module(module_name)
