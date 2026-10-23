"""Forwards Glue requests to moto, except GetTables (and UpdateTable, for fault_concurrent_modification*) on a fault_*
database, which fails with that fault's Glue error."""

import argparse
import http.client
import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAULTS = {
    "AWSGlue.GetTables": {"fault_access_denied": "AccessDeniedException",
                          "fault_invalid_input": "InvalidInputException"},
    "AWSGlue.UpdateTable": {"fault_concurrent_modification": "ConcurrentModificationException"},
}
NOT_FORWARDED = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length",
                 "server", "date"}


def injected_error(target, body):
    faults = FAULTS.get(target)
    if not faults:
        return None
    try:
        database = json.loads(body or b"{}").get("DatabaseName", "")
    except ValueError:
        return None
    return next((error for prefix, error in faults.items() if database.startswith(prefix)), None)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.relay()

    def do_POST(self):
        self.relay()

    def relay(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        error = injected_error(self.headers.get("X-Amz-Target"), body)
        if error:
            payload = json.dumps({"__type": error, "Message": "injected by glue_fault_proxy"}).encode()
            self.reply(400, [("Content-Type", "application/x-amz-json-1.1"), ("x-amzn-ErrorType", error)], payload)
            return
        upstream = http.client.HTTPConnection(self.server.upstream.hostname, self.server.upstream.port, timeout=60)
        try:
            headers = {k: v for k, v in self.headers.items() if k.lower() not in NOT_FORWARDED}
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            data = response.read()
            self.reply(response.status, [(k, v) for k, v in response.getheaders() if k.lower() not in NOT_FORWARDED],
                       data)
        finally:
            upstream.close()

    def reply(self, status, headers, body):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5556)
    parser.add_argument("--upstream", default="http://localhost:5555")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.upstream = urllib.parse.urlsplit(args.upstream)
    server.serve_forever()


if __name__ == "__main__":
    main()
