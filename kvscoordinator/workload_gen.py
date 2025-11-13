#!/usr/bin/env python3
import random
import sys

def main():
    if len(sys.argv) != 5:
        print("Usage: python generate_transactions.py <output_file> <num_transactions> <ops_per_transaction> <get_proportion>")
        print("Example: python generate_transactions.py txns.txt 10 3 0.7")
        sys.exit(1)

    output_file = sys.argv[1]
    num_transactions = int(sys.argv[2])
    ops_per_transaction = int(sys.argv[3])
    get_proportion = float(sys.argv[4])  # e.g., 0.7 means 70% gets, 30% puts

    keys = ["apple", "banana", "cherry", "date", "hello", "world"]
    values = list(range(1, 101))

    with open(output_file, "w") as f:
        for _ in range(num_transactions):
            f.write("begin ")
            ops = []
            for _ in range(ops_per_transaction):
                if random.random() < get_proportion:
                    key = random.choice(keys)
                    ops.append(f"get({key})")
                else:
                    key = random.choice(keys)
                    value = random.choice(values)
                    ops.append(f"put({key}, {value})")
            f.write(" ".join(ops))
            f.write(" end\n")

    print(f"Wrote {num_transactions} transactions to {output_file}")

if __name__ == "__main__":
    main()
