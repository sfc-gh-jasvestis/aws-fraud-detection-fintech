"""
DEPRECATED — use scripts/generate_synthetic_data.py instead.

This shim re-invokes the canonical generator with default args so any
existing bookmarks or CI references keep working.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
script = ROOT / "scripts" / "generate_synthetic_data.py"

print(
    "demo/seed_data.py is deprecated.\n"
    f"Delegating to {script} — pass args there directly for full control.\n"
)
result = subprocess.run(
    [sys.executable, str(script)] + sys.argv[1:],
    cwd=str(ROOT),
)
sys.exit(result.returncode)
