from __future__ import annotations
import os
import json
import logging
import uuid
from typing import Any, Dict, Optional

from azure.servicebus import ServiceBusClient, ServiceBusMessage

from .runner import run

LOG = logging.getLogger(__name__)


class ServiceBusOrchestrator:
    """Listens to an Azure Service Bus queue and dispatches runner jobs.

    Expected message payload (JSON):
      {
        "config": "configs/vbuttons_mls_238_agent.yaml",
        "mls": 238,
        "since": "2020-10-01",
        "log_file": "./logs/job-238.jsonl",
        ... any extra context keys ...
      }

    The class supports connecting via a connection string (recommended) passed
    via constructor or environment variable SERVICEBUS_CONNECTION_STRING.
    """

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.environ.get(
            "SERVICEBUS_CONNECTION_STRING"
        )
        if not self.connection_string:
            raise ValueError("Service Bus connection string must be provided")

    def _parse_message_body(self, msg) -> Dict[str, Any]:
        # msg.body may be an iterable of bytes/str parts
        try:
            parts = [bytes(p) for p in msg.body]
            raw = b"".join(parts).decode("utf-8")
        except Exception:
            # fallback to str(msg)
            raw = str(msg)

        return json.loads(raw)

    def handle_message(self, payload: Dict[str, Any]) -> None:
        # Extract required params
        cfg = payload.get("config")
        mls = payload.get("mls")
        if cfg is None or mls is None:
            raise ValueError("Message payload must include 'config' and 'mls' fields")

        # Ensure a request_id is present for correlation (generate if missing)
        request_id = payload.get("request_id") or str(uuid.uuid4())

        # Build context from payload, excluding config and mls, but include request_id
        context = {k: v for k, v in payload.items() if k not in ("config", "mls")}
        context["request_id"] = request_id

        # Ensure mls is int
        try:
            mls_int = int(mls)
        except Exception:
            raise ValueError("'mls' must be an integer")

        LOG.info("Dispatching runner request_id=%s mls=%s config=%s", request_id, mls_int, cfg)
        # run returns (total, metadata)
        result = run(cfg, mls_int, context)
        if isinstance(result, tuple):
            total, metadata = result
        else:
            total = result
            metadata = {}

        LOG.info("Runner completed, fetched %d records", total)

        # If the caller requested an output queue, send a JSON summary message
        out_queue = payload.get("output_queue")
        out_conn = payload.get("output_connection_string") or self.connection_string
        if out_queue:
            summary = {
                "request_id": request_id,
                "config": cfg,
                "mls": mls_int,
                "total": total,
                "elapsed_seconds": metadata.get("elapsed_seconds"),
                "sample": metadata.get("sample"),
                "response_fields": metadata.get("response_fields"),
            }
            try:
                # use provided connection string if present, otherwise use existing client
                if out_conn == self.connection_string:
                    # reuse the ServiceBusClient context
                    with ServiceBusClient.from_connection_string(self.connection_string) as client:
                        with client.get_queue_sender(queue_name=out_queue) as sender:
                            sender.send_messages(ServiceBusMessage(json.dumps(summary)))
                else:
                    # explicit different connection string
                    with ServiceBusClient.from_connection_string(out_conn) as client:
                        with client.get_queue_sender(queue_name=out_queue) as sender:
                            sender.send_messages(ServiceBusMessage(json.dumps(summary)))
                LOG.info("Sent summary message to output queue %s", out_queue)
            except Exception:
                LOG.exception("Failed to send summary message to output queue %s", out_queue)

    def listen_queue(self, queue_name: str, max_wait_time: int = 5):
        """Continuously listen to the given queue and process incoming messages.

        This method blocks until interrupted.
        """
        LOG.info("Connecting to Service Bus and listening on queue '%s'", queue_name)
        with ServiceBusClient.from_connection_string(self.connection_string) as client:
            receiver = client.get_queue_receiver(queue_name=queue_name, max_wait_time=max_wait_time)
            with receiver:
                while True:
                    try:
                        messages = receiver.receive_messages(max_message_count=5, max_wait_time=max_wait_time)
                        for msg in messages:
                            try:
                                payload = self._parse_message_body(msg)
                                self.handle_message(payload)
                                receiver.complete_message(msg)
                            except Exception as e:
                                LOG.exception("Failed to process message: %s", e)
                                try:
                                    receiver.abandon_message(msg)
                                except Exception:
                                    LOG.exception("Failed to abandon message")
                    except KeyboardInterrupt:
                        LOG.info("Interrupted, stopping listener")
                        break
                    except Exception:
                        LOG.exception("Error while receiving messages, continuing")


def main_from_env():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", required=True, help="Service Bus queue name to listen to")
    parser.add_argument("--connection-string", required=False, help="Service Bus connection string (overrides env)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    orchestrator = ServiceBusOrchestrator(connection_string=args.connection_string)
    orchestrator.listen_queue(args.queue)


if __name__ == "__main__":
    main_from_env()
