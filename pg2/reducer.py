#reducer
import sys
from collections import defaultdict

intermediate = defaultdict(list)

for line in sys.stdin:
    line = line.strip()
    key, value = line.split("\t")
    i, j = map(int, key.split())
    parts = value.split()

    intermediate[(i, j)].append((parts[0], int(parts[1]), int(parts[2])))

for (i, j), values in intermediate.items():
    a_values = {k: v for m, k, v in values if m == "A"}
    b_values = {k: v for m, k, v in values if m == "B"}

    result = sum(a_values[k] * b_values[k] for k in a_values if k in b_values)

    if result:
        print(f"{i} {j} {result}")
