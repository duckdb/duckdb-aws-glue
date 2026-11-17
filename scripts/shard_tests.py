#!/usr/bin/env python3
"""Splits the sqllogictests of the cloud Glue run into shards that can run at the same time.

Every shard uses a Glue database of its own (GLUE_TEST_DB), so the tests of different shards cannot see each other's
tables. What a database of its own does not separate is the account: a test that creates a Glue database with a fixed
name would collide with another test that creates the same name, and a test that lists the databases of the account
sees the other shards'. So

  - tests that create the same database name (CREATE SCHEMA / CREATE DATABASE <name>) go to the same shard, and
  - tests listed in a serial file (test/configs/cloud_serial_tests.txt) run on their own, after the shards.

The tests are balanced by their time in an earlier run (--timings, a JSON object of test path -> seconds, written by
--write-timings from a run's log); a test without a time counts as the median.

  shard_tests.py --shards 4 --index 1          the tests of shard 1 of 4, one path per line
  shard_tests.py --serial                      the tests that run after the shards
  shard_tests.py --write-timings < run.log     test times from the "[n/m] (p%): <test> took <s>s" lines of a log
"""

import argparse
import collections
import glob
import json
import os
import re
import statistics
import sys

DEFAULT_CONFIG = "test/configs/cloud_glue.json"
DEFAULT_SERIAL = "test/configs/cloud_serial_tests.txt"
CREATED_DATABASE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:SCHEMA|DATABASE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w\"]+\.)?\"?(\w+)\"?",
    re.IGNORECASE | re.MULTILINE,
)
TIME_LINE = re.compile(r"\]\s*\(\d+%\): (\S+\.test) took ([\d.]+)s")


def all_tests(root="test/sql"):
    return sorted(glob.glob(os.path.join(root, "**", "*.test"), recursive=True))


def skipped_tests(config_path):
    with open(config_path) as config:
        skipped = json.load(config).get("skip_tests", [])
    return {path for group in skipped for path in group["paths"]}


def serial_tests(path):
    if not os.path.exists(path):
        return set()
    with open(path) as serial:
        return {line.strip() for line in serial if line.strip() and not line.startswith("#")}


def groups(tests):
    """The tests that must share a shard: those that create a Glue database of the same name."""
    parent = {test: test for test in tests}

    def find(test):
        while parent[test] != test:
            parent[test] = parent[parent[test]]
            test = parent[test]
        return test

    by_name = collections.defaultdict(list)
    for test in tests:
        with open(test) as source:
            for name in set(CREATED_DATABASE.findall(source.read())):
                by_name[name.lower()].append(test)
    for members in by_name.values():
        for other in members[1:]:
            parent[find(other)] = find(members[0])
    grouped = collections.defaultdict(list)
    for test in tests:
        grouped[find(test)].append(test)
    return list(grouped.values())


def split(tests, shards, timings):
    known = [seconds for seconds in timings.values() if seconds > 0]
    fallback = statistics.median(known) if known else 1.0

    def cost(test):
        return timings.get(test, fallback)

    units = sorted(groups(tests), key=lambda unit: -sum(map(cost, unit)))
    result = [([], 0.0) for _ in range(shards)]
    for unit in units:
        index = min(range(shards), key=lambda i: result[i][1])
        result[index] = (result[index][0] + unit, result[index][1] + sum(map(cost, unit)))
    return [sorted(tests) for tests, _ in result], [seconds for _, seconds in result]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--index", type=int, help="the shard to print, from 1")
    parser.add_argument("--serial", action="store_true", help="print the tests that run after the shards")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--serial-file", default=DEFAULT_SERIAL)
    parser.add_argument("--timings", help="JSON object of test path -> seconds")
    parser.add_argument("--write-timings", action="store_true", help="read a run's log on stdin, print the timings")
    parser.add_argument("--summary", action="store_true", help="print the expected time of each shard")
    args = parser.parse_args()

    if args.write_timings:
        timings = {}
        for line in sys.stdin:
            match = TIME_LINE.search(line)
            if match:
                timings[match.group(1)] = float(match.group(2))
        json.dump(timings, sys.stdout, indent=1, sort_keys=True)
        return

    timings = {}
    if args.timings and os.path.exists(args.timings):
        with open(args.timings) as source:
            timings = json.load(source)
    skipped = skipped_tests(args.config)
    serial = serial_tests(args.serial_file)
    tests = [test for test in all_tests() if test not in skipped]
    if args.serial:
        print("\n".join(sorted(test for test in tests if test in serial)))
        return
    parallel = [test for test in tests if test not in serial]
    shards, seconds = split(parallel, args.shards, timings)
    if args.summary:
        for i, (members, total) in enumerate(zip(shards, seconds), 1):
            print(f"shard {i}: {len(members)} tests, about {total / 60:.1f} min", file=sys.stderr)
    if args.index is not None:
        if not 1 <= args.index <= args.shards:
            parser.error("--index must be between 1 and --shards")
        print("\n".join(shards[args.index - 1]))


if __name__ == "__main__":
    main()
