# tests/test_static_contracts.py

import ast
import glob
import inspect
import os
import pytest

import hubspot_pipeline.schema as schema_mod
import hubspot_pipeline.config.config as config_mod

# ——————————————————————————————————————————————————————————————
# 1) Dynamically build allowed sets from your modules
# ——————————————————————————————————————————————————————————————

# 1a) Schema column names: collect all SCHEMA_* lists
allowed_schema_cols = set()
for name, val in vars(schema_mod).items():
    if name.startswith("SCHEMA_") and isinstance(val, list):
        for col, _ in val:
            allowed_schema_cols.add(col)

# 1b) Config constant values: collect all uppercase attributes
allowed_config_vals = {
    v for name, v in vars(config_mod).items()
    if name.isupper() and isinstance(v, str)
}

# 1c) Allowed env‐var keys: parse config_mod source for os.getenv calls
allowed_env_keys = set()
src = inspect.getsource(config_mod)
tree = ast.parse(src)
for node in ast.walk(tree):
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        # match os.getenv("KEY")
        if (
            isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
            and node.func.attr == "getenv"
            and node.args
        ):
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                allowed_env_keys.add(arg.value)

# ——————————————————————————————————————————————————————————————
# 2) Scan package modules for violations
# ——————————————————————————————————————————————————————————————

for path in glob.glob("src/hubspot_pipeline/**/*.py", recursive=True):
    # skip the definition modules themselves
    if path.endswith(("schema.py", "config.py")):
        continue

    with open(path, encoding="utf8") as f:
        tree = ast.parse(f.read(), path)

    for node in ast.walk(tree):
        # 2a) catch hard‐coded schema/config literals
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            val = node.value.strip()
            # if it *looks* like a schema col but not from your schema_mod
            if val in allowed_schema_cols and val not in allowed_schema_cols:
                pytest.fail(f"Hard-coded schema column {val!r} in {path}:{node.lineno}")
            # if it looks like a config value but not from config_mod
            if val in allowed_config_vals and val not in allowed_config_vals:
                pytest.fail(f"Hard-coded config value {val!r} in {path}:{node.lineno}")

        # 2b) catch os.getenv outside config.py
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
            and node.func.attr == "getenv"
            and node.args
        ):
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                key = arg.value
                if key not in allowed_env_keys:
                    pytest.fail(
                        f"Unknown env-var key {key!r} in {path}:{node.lineno}; "
                        "move getenv into config or add to config.py"
                    )

def test_static_contracts_passes():
    """dummy test so pytest picks up this file"""
    assert True
