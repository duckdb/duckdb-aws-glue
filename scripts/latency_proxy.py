"""Forwards HTTP requests to an upstream server (moto's Glue, SeaweedFS's S3) after a fixed delay, so that a local
benchmark pays for each round trip the way it does against AWS: a request made after another one has returned adds
its delay, requests in flight at the same time share it. The delay is per operation: the Glue action of the
X-Amz-Target header (GetPartitions, GetTable, ...) or, for S3, LIST (a ListObjects request), HEAD, GET, PUT, POST or
DELETE. --max-concurrency caps the requests being delayed at the same time; the others wait for a slot, as requests
over Glue's rate limit wait for the SDK to retry them. --log appends one JSON line per request (operation, start,
end, in seconds since the epoch) for counting requests and rounds. Requests are forwarded with their Host header, so
the S3 signatures stay valid. With --cache the answers to reads (Glue Get*/Search* actions, S3 GET, HEAD and LIST) are
kept and served again, after the same delay, to the same request (body and Range): moto spends seconds on a
GetPartitions of a table with thousands of partitions, one request at a time, which would hide the round trips the
delays stand for. Only for data that does not change while the proxy runs: restart it after writing through another
endpoint."""

import argparse
import contextlib
import http.client
import json
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NOT_FORWARDED = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length"}


def is_read(method, name):
    if method == "POST":
        return name.startswith(("Get", "Search"))
    return method in ("GET", "HEAD")


def operation(method, path, headers):
    target = headers.get("X-Amz-Target")
    if target:
        return target.rsplit(".", 1)[-1]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query, keep_blank_values=True)
    if method == "GET" and ("list-type" in query or "delimiter" in query or "prefix" in query):
        return "LIST"
    return method


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.relay()

    def do_HEAD(self):
        self.relay()

    def do_PUT(self):
        self.relay()

    def do_POST(self):
        self.relay()

    def do_DELETE(self):
        self.relay()

    def relay(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        name = operation(self.command, self.path, self.headers)
        server = self.server
        start = time.time()
        key = None
        if server.cache is not None and is_read(self.command, name):
            key = (self.command, self.path, self.headers.get("X-Amz-Target"), self.headers.get("Range"), body)
        with server.slots:
            delay = server.latencies.get(name, server.latency)
            if delay > 0:
                time.sleep(delay)
            answer = server.cache.get(key) if key is not None else None
            if answer is None:
                answer = self.forward(body)
                if key is not None and 200 <= answer[0] < 300:
                    server.cache[key] = answer
            status, headers, payload = answer
        end = time.time()
        if server.log:
            with server.log_lock:
                server.log.write(json.dumps({"op": name, "start": start, "end": end}) + "\n")
        self.send_response(status)
        for header, value in headers:
            if header.lower() not in NOT_FORWARDED:
                self.send_header(header, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def forward(self, body):
        headers = {name: value for name, value in self.headers.items() if name.lower() not in NOT_FORWARDED}
        for attempt in range(2):
            connection = getattr(self, "upstream", None)
            if connection is None:
                upstream = self.server.upstream
                connection = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=120)
                self.upstream = connection
            try:
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                return response.status, response.getheaders(), response.read()
            except (ConnectionError, http.client.HTTPException):
                connection.close()
                self.upstream = None
                if attempt == 1:
                    raise
        raise AssertionError("unreachable")

    def log_message(self, *args):
        pass


def parse_latencies(values):
    latencies = {}
    for value in values:
        name, _, milliseconds = value.partition("=")
        latencies[name] = float(milliseconds) / 1000
    return latencies


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--latency-ms", type=float, default=0, help="delay of an operation without its own")
    parser.add_argument("--operation-latency-ms", action="append", default=[], metavar="OPERATION=MS",
                        help="delay of one operation, e.g. GetPartitions=60 or LIST=25; may be repeated")
    parser.add_argument("--max-concurrency", type=int, default=0, help="requests delayed at the same time (0: any)")
    parser.add_argument("--log", help="file to append one JSON line per request to")
    parser.add_argument("--cache", action="store_true", help="serve repeated reads from memory")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.daemon_threads = True
    server.upstream = urllib.parse.urlsplit(args.upstream)
    server.latency = args.latency_ms / 1000
    server.latencies = parse_latencies(args.operation_latency_ms)
    unbounded = contextlib.nullcontext()
    server.slots = threading.BoundedSemaphore(args.max_concurrency) if args.max_concurrency > 0 else unbounded
    server.log = open(args.log, "a", buffering=1) if args.log else None
    server.cache = {} if args.cache else None
    server.log_lock = threading.Lock()
    server.serve_forever()


if __name__ == "__main__":
    main()
