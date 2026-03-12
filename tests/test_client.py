import threading
import unittest

import httpx

from gamecritic.client import MetacriticClient


class _Response:
    status_code = 200
    url = "https://example.com"
    text = ""
    content = b""

    def json(self) -> dict:
        return {}


class MetacriticClientStopTestCase(unittest.TestCase):
    def test_request_raises_interrupted_error_when_stop_set_during_inflight_request(self) -> None:
        stop_event = threading.Event()
        client = MetacriticClient(
            timeout_seconds=30.0,
            max_retries=2,
            backoff_seconds=0.1,
            delay_seconds=0.0,
            stop_event=stop_event,
        )
        get_started = threading.Event()
        close_called = threading.Event()
        original_close = client._http.close

        def _close() -> None:
            close_called.set()
            original_close()

        def _get(_: str, params=None) -> _Response:
            del params
            get_started.set()
            if not close_called.wait(1.0):
                raise AssertionError("close was not called after stop")
            raise httpx.TransportError("request interrupted")

        client._http.close = _close
        client._http.get = _get

        errors: list[BaseException] = []

        def _run() -> None:
            try:
                client._request("https://example.com")
            except BaseException as exc:  # pragma: no cover - assertion inspects collected error
                errors.append(exc)

        worker = threading.Thread(target=_run)
        worker.start()
        self.assertTrue(get_started.wait(0.2))
        stop_event.set()
        worker.join(1.0)
        self.assertFalse(worker.is_alive())
        self.assertTrue(close_called.is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], InterruptedError)
        client.close()

    def test_request_raises_interrupted_error_when_stop_is_requested_before_response_is_handled(self) -> None:
        stop_event = threading.Event()
        client = MetacriticClient(
            timeout_seconds=30.0,
            max_retries=1,
            delay_seconds=0.0,
            stop_event=stop_event,
        )

        def _get(_: str, params=None) -> _Response:
            del params
            stop_event.set()
            return _Response()

        client._http.get = _get

        with self.assertRaises(InterruptedError):
            client._request("https://example.com")

        client.close()


if __name__ == "__main__":
    unittest.main()
