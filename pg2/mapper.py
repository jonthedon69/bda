#mapper
#!/usr/bin/env python3
import sys

# Define matrix dimensions
A_ROWS = 2  # Number of rows in A
B_COLS = 2  # Number of columns in B

for line in sys.stdin:
    line = line.strip()
    matrix, row, col, value = line.split()
    row, col, value = int(row), int(col), int(value)

    if matrix == "A":
        for k in range(B_COLS):
            print(f"{row} {k}\tA {col} {value}")
    elif matrix == "B":
        for i in range(A_ROWS):
            print(f"{i} {col}\tB {row} {value}")
#to run pg2 Get-Content matrix.txt | python mapper.py | sort | python reducer.py
