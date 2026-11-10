"""
Extract the generator code from the notebook-formatted dataset_generator.py
and scale the dataset size.

Usage (from project root):
    python data_generation/prepare_generator.py dataset_generator.py 200000 50000
Produces data_generation/generate_data.py with NUM_SESSIONS / NUM_TRANSACTIONS patched.
Then run:
    cd data && python ../data_generation/generate_data.py
"""
import json
import re
import sys

src = sys.argv[1]
n_sessions = int(sys.argv[2]) if len(sys.argv) > 2 else 200_000
n_txn = int(sys.argv[3]) if len(sys.argv) > 3 else 50_000

with open(src) as f:
    content = f.read()

try:  # the provided file is a Jupyter notebook saved with a .py extension
    nb = json.loads(content)
    code = "".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
except json.JSONDecodeError:  # already plain python
    code = content

code = re.sub(r"NUM_SESSIONS\s*=\s*[\d_]+", f"NUM_SESSIONS = {n_sessions}", code)
code = re.sub(r"NUM_TRANSACTIONS\s*=\s*[\d_]+", f"NUM_TRANSACTIONS = {n_txn}", code)

with open("data_generation/generate_data.py", "w") as f:
    f.write(code)
print(f"Wrote data_generation/generate_data.py (sessions={n_sessions:,}, transactions={n_txn:,})")
