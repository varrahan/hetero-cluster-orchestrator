from __future__ import annotations

import socketserver
import tempfile
import threading
import unittest

from checkpointing.client import CheckpointClient, _background


class BackgroundTest(unittest.TestCase):
    def test_result_and_exception(self) -> None:
        def fail() -> None:
            raise ValueError("failed")

        self.assertEqual(_background(lambda: 42).result(1), 42)
        with self.assertRaisesRegex(ValueError, "failed"):
            _background(fail).result(1)

    def test_unix_request_closes_connection(self) -> None:
        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                self.request.recv(4096)
                self.request.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{"ok":true}')

        with tempfile.TemporaryDirectory() as directory:
            server = socketserver.UnixStreamServer(f"{directory}/server.sock", Handler)
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            try:
                client = CheckpointClient(job_id=1, rank=0)
                self.assertEqual(client._request(f"{directory}/server.sock", "GET", "/"), {"ok": True})
            finally:
                thread.join(1)
                server.server_close()


if __name__ == "__main__":
    unittest.main()
