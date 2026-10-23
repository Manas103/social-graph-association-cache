#!/usr/bin/env python3
"""Generates the Zipfian key-access sequence the C++ throughput benchmark replays.

Usage: python3 gen_zipf_workload.py --num-keys 20000 --num-ops 2000000 --s 0.99 --out workload.txt

This is the one piece of the project that is genuinely easier in Python than C++:
numpy's Zipf sampler is a single call, and the output is a flat file of key indices,
one per line, that bench/run_throughput_bench.cpp reads with no Python dependency at
run time. The C++ side never calls back into Python; this is an offline data-prep step.
"""
import argparse
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--num-keys", type=int, default=20000)
    ap.add_argument("--num-ops", type=int, default=2_000_000)
    ap.add_argument("--s", type=float, default=0.99)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    try:
        import numpy as np
    except ImportError:
        print("numpy is required: pip install numpy", file=sys.stderr)
        sys.exit(1)

    rng = np.random.default_rng(args.seed)
    # numpy's zipf is unbounded above num_keys; sample with rejection so every
    # emitted key index is in range, which keeps the workload file directly
    # usable as a 0-based array index in C++ with no modulo bias.
    out = np.empty(args.num_ops, dtype=np.int64)
    filled = 0
    while filled < args.num_ops:
        batch = rng.zipf(1.0 + args.s, size=(args.num_ops - filled) * 2)
        batch = batch[batch <= args.num_keys] - 1
        take = min(len(batch), args.num_ops - filled)
        out[filled:filled + take] = batch[:take]
        filled += take

    with open(args.out, "w") as f:
        f.write(f"{args.num_keys} {args.num_ops}\n")
        for v in out:
            f.write(f"{v}\n")

    # Quick sanity report: fraction of ops landing on the single hottest key.
    counts = np.bincount(out, minlength=args.num_keys)
    hottest_frac = counts.max() / args.num_ops
    print(f"wrote {args.num_ops} ops over {args.num_keys} keys to {args.out}; "
          f"hottest key gets {hottest_frac:.4%} of traffic (s={args.s})")


if __name__ == "__main__":
    main()
