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
  shard_tests.py --check                       fail on a test that breaks the rules above, naming the test and the fix
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
# Every CREATE SCHEMA / CREATE DATABASE and the name it creates, up to the end of the name
CREATED_DATABASE = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:SCHEMA|DATABASE)\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:"[^"]*"|[^\s;"(]+)+)',
    re.IGNORECASE | re.MULTILINE,
)
NAME_PART = re.compile(r'"([^"]*)"|([^."]+)')
# The variables a test's names can hold: foreach <var> <values...>, (concurrent)loop <var> <from> <to>
FOREACH = re.compile(r"^\s*foreach\s+(\w+)\s+(.+)$", re.MULTILINE)
LOOP = re.compile(r"^\s*(?:concurrent)?loop\s+(\w+)\s+(\d+)\s+(\d+)\s*$", re.MULTILINE)
VARIABLE = re.compile(r"\{(\w+)\}")
TIME_LINE = re.compile(r"\]\s*\(\d+%\): (\S+\.test(?:_slow)?) took ([\d.]+)s")
# A table or a Glue call in the shared default database instead of the shard's
DEFAULT_DATABASE = re.compile(
    r"""\."?default"?\.|"DatabaseName"\s*:\s*"default"|DEFAULT_SCHEMA\s+'default'|\(\s*'default'""",
    re.IGNORECASE,
)
# A listing of what the whole account holds: it sees the databases the other shards create
ACCOUNT_LISTING = re.compile(
    r"duckdb_schemas\s*\(|information_schema\.schemata|SHOW\s+ALL\s+TABLES|SHOW\s+(?:SCHEMAS|DATABASES)"
    r"|duckdb_(?:tables|columns|views)\s*\(\s*\)|information_schema\.(?:tables|columns|views)",
    re.IGNORECASE,
)
# What narrows such a listing to the shard's own database
SHARD_FILTER = re.compile(r"\{GLUE_TEST_DB(?:_SQL)?\}|\bschema_name\b|\btable_schema\b", re.IGNORECASE)


def created_databases(source):
    """The Glue databases a test creates, lowercased, and the CREATE statements whose name it cannot read.

    The name is the target without its catalog (my_datalake.x and "x" are x; a nested exn.a.b is a.b). A {variable}
    of a foreach or a (concurrent)loop stands for each of its values; {GLUE_TEST_DB} is the shard's own database,
    which no other shard creates."""
    values = {name: rest.split() for name, rest in FOREACH.findall(source)}
    values.update({name: [str(i) for i in range(int(low), int(high))] for name, low, high in LOOP.findall(source)})
    names, unreadable = set(), []
    for target in CREATED_DATABASE.findall(source):
        parts = [quoted if quoted else bare for quoted, bare in NAME_PART.findall(target)]
        name = ".".join(parts[1:] if len(parts) > 1 else parts)
        if "{GLUE_TEST_DB" in name:
            continue
        expanded = [name]
        for variable in VARIABLE.findall(name):
            if variable not in values:
                expanded = None
                break
            expanded = [e.replace("{" + variable + "}", value) for e in expanded for value in values[variable]]
        if expanded is None or not name:
            unreadable.append(target)
        else:
            names.update(e.lower() for e in expanded)
    return names, unreadable


def all_tests(root="test/sql"):
    tests = []
    for pattern in ("*.test", "*.test_slow"):
        tests += glob.glob(os.path.join(root, "**", pattern), recursive=True)
    return sorted(tests)


def statements(source):
    """The statements and queries of a test, as (first line number, SQL): its lines up to a blank line or a query's
    result, without the sqllogictest header (statement ok, query I, ...) and without comments."""
    block, first = [], None
    for number, line in enumerate(source.splitlines(), 1):
        stripped = line.strip()
        if not stripped or stripped == "----":
            if block:
                yield first, "\n".join(block)
            block, first = [], None
            continue
        if stripped.startswith("#") or (not block and stripped.split()[0] in ("statement", "query")):
            continue
        if not block:
            first = number
        block.append(line)
    if block:
        yield first, "\n".join(block)


RULES = {
    "default": "uses the shared Glue database 'default': use {GLUE_TEST_DB} (or {GLUE_TEST_DB_SQL} where it must be "
    "quoted), so that the test runs on its shard's own database",
    "listing": "lists what the whole account holds: filter the listing to {GLUE_TEST_DB}, or add the test to "
    + DEFAULT_SERIAL
    + " so that it runs after the shards",
    "name": "creates a Glue database whose name the sharding cannot read: name it with a literal, so that the tests "
    "that create it share a shard, or add the test to " + DEFAULT_SERIAL,
}


def problems(source, serial):
    """How a test breaks the rules that keep the shards apart: rule -> [(line number, SQL)]."""
    found = collections.defaultdict(list)
    for number, statement in statements(source):
        sql = " ".join(statement.split())
        if not serial and DEFAULT_DATABASE.search(statement):
            found["default"].append((number, sql))
        if not serial and ACCOUNT_LISTING.search(statement) and not SHARD_FILTER.search(statement):
            found["listing"].append((number, sql))
    if not serial:
        _, unreadable = created_databases(source)
        for target in unreadable:
            found["name"].append((source[: source.find(target)].count("\n") + 1, target))
    return found


def check(tests, serial):
    failed = 0
    for test in tests:
        with open(test) as source:
            found = problems(source.read(), test in serial)
        for rule, where in found.items():
            lines = ", ".join(str(number) for number, _ in where)
            print(f"{test}:{where[0][0]}: {RULES[rule]} (line {lines}, e.g. {where[0][1][:100]})", file=sys.stderr)
        failed += bool(found)
    if failed:
        print(f"{failed} test(s) would not stay apart on the cloud shards; see above", file=sys.stderr)
    return failed == 0


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
            for name in created_databases(source.read())[0]:
                by_name[name].append(test)
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
    parser.add_argument("--check", action="store_true", help="fail on a test that breaks the sharding rules")
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
    if args.check:
        sys.exit(0 if check(tests, serial) else 1)
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
