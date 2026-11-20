"""Forwards S3 requests to the upstream, except that the first ListObjectsV2 of a prefix whose last path segment
contains 'fault_once' is answered with a 500 InternalError. A later listing of the same prefix is forwarded. A GET of
/_reset forgets which prefixes have failed, so that a test can fail the same directory again."""

import argparse
import http.client
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAULT_SEGMENT = "fault_once"
NOT_FORWARDED = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "content-length"}


def is_fault_prefix(prefix):
    segments = prefix.rstrip("/").split("/")
    return FAULT_SEGMENT in segments[-1]


class Server(ThreadingHTTPServer):
    # a scan opens many connections at once; the default backlog of five drops them
    request_queue_size = 256


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def relay(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        url = urllib.parse.urlsplit(self.path)
        if url.path == "/_reset":
            with self.server.lock:
                self.server.failed.clear()
            self.reply(200, [], b"")
            return
        query = urllib.parse.parse_qs(url.query)
        prefix = (query.get("prefix") or [""])[0]
        if self.command == "GET" and query.get("list-type") == ["2"] and is_fault_prefix(prefix):
            with self.server.lock:
                first = prefix not in self.server.failed
                self.server.failed.add(prefix)
            if first:
                self.reply(
                    500,
                    [("Content-Type", "application/xml")],
                    b"<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>",
                )
                return
        upstream = http.client.HTTPConnection(self.server.upstream.hostname, self.server.upstream.port, timeout=60)
        try:
            # the Host header stays the client's, which the S3 signature covers
            headers = {k: v for k, v in self.headers.items() if k.lower() not in NOT_FORWARDED}
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            data = response.read()
            headers = [(k, v) for k, v in response.getheaders() if k.lower() not in NOT_FORWARDED]
            length = response.getheader("Content-Length")
            self.reply(response.status, headers, data, head=self.command == "HEAD", length=length)
        finally:
            upstream.close()

    def reply(self, status, headers, data, head=False, length=None):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", length if head and length else str(len(data)))
        self.end_headers()
        if not head:
            self.wfile.write(data)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = relay

    def log_message(self, *args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    arguments = parser.parse_args()
    server = Server(("0.0.0.0", arguments.port), Handler)
    server.upstream = urllib.parse.urlsplit(arguments.upstream)
    server.failed = set()
    server.lock = threading.Lock()
    server.serve_forever()


if __name__ == "__main__":
    main()
