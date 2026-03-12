# migrations/__init__.py
import importlib
import os
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent

def run_migrations(db, direction='up'):
    """
    Run all migration files in order.
    direction: 'up' or 'down'
    """
    migration_files = sorted(
        f for f in os.listdir(MIGRATIONS_DIR)
        if f.endswith('.py') and f != '__init__.py'
    )

    for filename in migration_files:
        module_name = filename[:-3]  # remove .py
        module_path = f"migrations.{module_name}"
        module = importlib.import_module(module_path)

        if hasattr(module, direction):
            print(f"Running {direction} migration: {module_name}")
            getattr(module, direction)(db)
        else:
            print(f"Warning: {module_name} has no '{direction}' function.")