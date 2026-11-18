"""Thin public-API client plus process control for verification runs.

Only documented HTTP endpoints are used.  The on-disk journal is read
separately as independent evidence; the client never reaches into Go
internals.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from . import journal


class ApiError(RuntimeError):
    def __init__(self, method, path, status, payload):
        self.method = method
        self.path = path
        self.status = status
        self.payload = payload
        code = None
        if isinstance(payload, dict):
            code = (payload.get("error") or {}).get("code")
        self.code = code
        super().__init__("%s %s -> %s %s" % (method, path, status, json.dumps(payload)[:400]))


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, binary, data_dir, restore=None, label="main"):
        self.binary = str(binary)
        self.data_dir = str(data_dir)
        self.restore = None if restore is None else str(restore)
        self.label = label
        self.process = None
        self.port = None
        self.url = None
        self.calls = 0

    # -- lifecycle --------------------------------------------------------
    def start(self, timeout=10.0):
        self.port = _free_port()
        args = [self.binary, "-data", self.data_dir, "-listen", "127.0.0.1:%d" % self.port]
        if self.restore:
            args += ["-restore", self.restore]
        self.process = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self.url = "http://127.0.0.1:%d/v1/lamina" % self.port
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                out, err = self.process.communicate()
                raise RuntimeError("server exited early: %s%s" % (
                    out.decode(errors="replace"), err.decode(errors="replace")))
            try:
                self.call("GET", "/health")
                return self
            except (OSError, urllib.error.URLError):
                time.sleep(0.02)
        raise RuntimeError("server did not become ready")

    def stop(self, grace=12.0):
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        try:
            self.process.communicate(timeout=2)
        except Exception:
            pass
        self.process = None

    def kill(self):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL)
            self.process.wait()

    def restart(self, crash=False):
        self.stop() if not crash else self.kill()
        return self.start()

    # -- raw request ------------------------------------------------------
    def raw(self, method, path, data=None, timeout=12.0):
        body = None if data is None else json.dumps(data, ensure_ascii=False).encode()
        request = urllib.request.Request(
            self.url + path, data=body, method=method,
            headers={"Content-Type": "application/json"},
        )
        self.calls += 1
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()

    def call(self, method, path, data=None, status=200):
        code, body = self.raw(method, path, data)
        try:
            payload = json.loads(body)
        except ValueError:
            payload = body.decode(errors="replace")
        if code != status:
            raise ApiError(method, path, code, payload)
        return payload

    def try_call(self, method, path, data=None):
        code, body = self.raw(method, path, data)
        try:
            payload = json.loads(body)
        except ValueError:
            payload = body.decode(errors="replace")
        return code, payload

    # -- documented operations -------------------------------------------
    def create_relation(self, name, primary, columns, status=201):
        return self.call("POST", "/relations",
                         {"name": name, "primary": primary, "columns": columns}, status)

    def create_index(self, name, relation, column, unique=False, status=201):
        return self.call("POST", "/indexes",
                         {"name": name, "relation": relation, "column": column,
                          "unique": unique}, status)

    def begin(self, read_only=False):
        return self.call("POST", "/transactions", {"read_only": read_only}, 201)["id"]

    def put(self, tx, relation, row, insert_only=False):
        return self.try_call("POST", "/transactions/%s/put" % tx,
                             {"relation": relation, "row": row, "insert_only": insert_only})

    def delete(self, tx, relation, key):
        return self.try_call("POST", "/transactions/%s/delete" % tx,
                             {"relation": relation, "key": key})

    def commit(self, tx):
        return self.try_call("POST", "/transactions/%s/commit" % tx, {})

    def rollback(self, tx):
        return self.try_call("POST", "/transactions/%s/rollback" % tx, {})

    def query(self, request):
        return self.try_call("POST", "/query", request)

    def stats(self):
        return self.call("GET", "/stats")

    def catalog(self):
        return self.call("GET", "/catalog")

    def changes(self, after=0, limit=100):
        return self.try_call("GET", "/changes?after=%d&limit=%d" % (after, limit))

    def vacuum(self, horizon):
        return self.try_call("POST", "/vacuum", {"horizon": horizon})

    def checkpoint(self):
        return self.try_call("POST", "/checkpoint", {})

    def backup(self):
        code, body = self.raw("GET", "/backup")
        if code != 200:
            raise ApiError("GET", "/backup", code, body.decode(errors="replace"))
        return body

    def health(self):
        return self.call("GET", "/health")

    # -- independent evidence --------------------------------------------
    def journal_path(self):
        return os.path.join(self.data_dir, "journal.lmw")

    def read_journal(self):
        return journal.parse_file(self.journal_path())
