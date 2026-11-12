#!/usr/bin/env python3
"""Glue resource-link stand-in in front of moto, for test/sql/hive/bugs/resource_link_local.test
(GLUE_LINK_PROXY_ENDPOINT in test/configs/local_glue.json; scripts/docker-compose.yml runs it on port 5557).

Real Glue, asked for a table through a database resource link `link_db` (target `target_db` in another account),
answers with the TARGET's DatabaseName, and the consumer catalog has no `target_db`. moto cannot model that, so this
proxy rewrites DatabaseName link_db -> target_db in table responses (and a DuckDB view's
duckdb_view_default_database, as a view written in target_db carries it) and answers EntityNotFound for any request
that names target_db. Every request is logged to stderr (target, DatabaseName) so a run shows which name was asked for.
"""
import argparse, http.server, json, sys, urllib.request

LINK, TARGET = "link_db", "target_db"

def rewrite(obj):
    if isinstance(obj, dict):
        return {k: (TARGET if k in ("DatabaseName", "duckdb_view_default_database") and v == LINK else rewrite(v))
                for k, v in obj.items()}
    if isinstance(obj, list):
        return [rewrite(v) for v in obj]
    return obj

class Proxy(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        op = self.headers.get("X-Amz-Target", "")
        try:
            req = json.loads(body or b"{}")
        except ValueError:
            req = {}
        print(f"PROXY {op} DatabaseName={req.get('DatabaseName')} Name={req.get('Name') or req.get('TableName')}",
              file=sys.stderr, flush=True)
        if req.get("DatabaseName") == TARGET:
            out = json.dumps({"__type": "EntityNotFoundException",
                              "Message": f"Database {TARGET} not found."}).encode()
            return self.reply(400, out, {"Content-Type": "application/x-amz-json-1.1",
                                         "x-amzn-ErrorType": "EntityNotFoundException"})
        fwd = urllib.request.Request(self.server.upstream + self.path, data=body, method="POST",
                                     headers={k: v for k, v in self.headers.items() if k.lower() != "host"})
        try:
            with urllib.request.urlopen(fwd) as r:
                status, data, headers = r.status, r.read(), dict(r.headers)
        except urllib.error.HTTPError as e:
            status, data, headers = e.code, e.read(), dict(e.headers)
        if status == 200 and op.endswith(("GetTable", "GetTables")):
            data = json.dumps(rewrite(json.loads(data))).encode()
        if status == 200 and op.endswith("GetDatabase"):
            doc = json.loads(data)
            if doc.get("Database", {}).get("Name") == LINK:
                doc["Database"]["TargetDatabase"] = {"CatalogId": "210987654321", "DatabaseName": TARGET}
                data = json.dumps(doc).encode()
        self.reply(status, data, {k: v for k, v in headers.items()
                                  if k.lower() not in ("content-length", "transfer-encoding", "connection")})

    def reply(self, status, data, headers):
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", type=int, default=5557)
parser.add_argument("--upstream", default="http://localhost:5555")
args = parser.parse_args()
server = http.server.ThreadingHTTPServer((args.host, args.port), Proxy)
server.upstream = args.upstream
server.serve_forever()
