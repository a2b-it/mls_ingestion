from __future__ import annotations
import os
import json
import logging
import uuid
from typing import Any, Dict, Optional
import time
from datetime import datetime, timezone

from azure.servicebus import ServiceBusClient, ServiceBusMessage
try:
    # azure.identity is optional in some test environments; import when available
    from azure.identity import DefaultAzureCredential
except Exception:  # pragma: no cover - optional dependency
    DefaultAzureCredential = None

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

    def __init__(
        self,
        connection_string: Optional[str] = None,
        input_queue: Optional[str] = None,
        output_queue: Optional[str] = None,
        listen_duration_seconds: Optional[int] = None,
        sb_fqdn: Optional[str] = None,
    ):
        # connection string: arg overrides env
        self.connection_string = connection_string or os.environ.get("SERVICEBUS_CONNECTION_STRING")
        # Optionally use Managed Identity (DefaultAzureCredential) when a
        # fully-qualified Service Bus namespace (SB_FQDN) is provided.
        # Constructor arg overrides env.
        self.sb_fqdn = sb_fqdn or os.environ.get("SB_FQDN")

        # queues: arg overrides env
        self.input_queue = input_queue or os.environ.get("INPUT_QUEUE")
        self.output_queue = output_queue or os.environ.get("OUTPUT_QUEUE")
        # How long the listener should run (seconds). If None, run until interrupted.
        # Constructor arg overrides env.
        env_dur = os.environ.get("LISTEN_DURATION_SECONDS")
        if listen_duration_seconds is None and env_dur:
            try:
                listen_duration_seconds = int(env_dur)
            except Exception:
                listen_duration_seconds = None

        self.listen_duration_seconds = listen_duration_seconds

        if self.sb_fqdn:
            if DefaultAzureCredential is None:
                raise RuntimeError("azure.identity is required for managed identity (SB_FQDN) but is not available")
            # create credential instance once and reuse
            self.credential = DefaultAzureCredential()
        else:
            self.credential = None

        # ensure we have at least one auth method: either a connection string or SB_FQDN
        if not self.connection_string and not self.sb_fqdn:
            raise ValueError("Either SERVICEBUS_CONNECTION_STRING or SB_FQDN (managed identity) must be provided")

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

        # Determine output queue (message override, else orchestrator-configured)
        out_queue = payload.get("output_queue") or self.output_queue
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
                # ensure we have a connection string to use
                    if not out_conn and not self.sb_fqdn:
                        LOG.warning("No connection string available to send summary to %s", out_queue)
                    else:
                        # prefer managed identity (sb_fqdn) when configured and no explicit out_conn
                        if self.sb_fqdn and (out_conn is None or out_conn == self.connection_string):
                            # credential was set in __init__ when sb_fqdn was provided
                            assert self.credential is not None
                            with ServiceBusClient(self.sb_fqdn, credential=self.credential) as client:
                                with client.get_queue_sender(queue_name=out_queue) as sender:
                                    sender.send_messages(ServiceBusMessage(json.dumps(summary)))
                        else:
                            # explicit connection string available; use it
                            conn_str = str(out_conn or self.connection_string)
                            with ServiceBusClient.from_connection_string(conn_str) as client:
                                with client.get_queue_sender(queue_name=out_queue) as sender:
                                    sender.send_messages(ServiceBusMessage(json.dumps(summary)))
                        LOG.info("Sent summary message to output queue %s", out_queue)
            except Exception:
                LOG.exception("Failed to send summary message to output queue %s", out_queue)

    def _send_warning_summary(self, reason: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Send a warning message to the configured output queue.

        This is used when the orchestrator stops due to its configured listen duration.
        """
        out_queue = self.output_queue
        out_conn = self.connection_string
        if not out_queue:
            LOG.warning("No output queue configured; cannot send warning summary: %s", reason)
            return

        warning = {
            "type": "warning",
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "details": details or {},
        }
        try:
            # Prefer managed identity when sb_fqdn is configured
            if self.sb_fqdn:
                assert self.credential is not None
                with ServiceBusClient(self.sb_fqdn, credential=self.credential) as client:
                    with client.get_queue_sender(queue_name=out_queue) as sender:
                        sender.send_messages(ServiceBusMessage(json.dumps(warning)))
            else:
                conn_str = str(out_conn)
                with ServiceBusClient.from_connection_string(conn_str) as client:
                    with client.get_queue_sender(queue_name=out_queue) as sender:
                        sender.send_messages(ServiceBusMessage(json.dumps(warning)))
            LOG.info("Sent warning summary to output queue %s", out_queue)
        except Exception:
            LOG.exception("Failed to send warning summary to output queue %s", out_queue)

    def listen_queue(self, queue_name: Optional[str] = None, max_wait_time: int = 5, listen_duration_seconds: Optional[int] = None):
        """Continuously listen to the given queue and process incoming messages.

        If listen_duration_seconds is provided (or configured on the instance), the listener
        will stop after that many seconds. When stopped due to timeout a warning message
        will be sent to the output queue.
        This method blocks until interrupted or duration elapses.
        """
        # choose queue: explicit arg overrides configured input_queue
        queue_name = queue_name or self.input_queue
        if not queue_name:
            raise ValueError("Input queue must be provided via constructor, env INPUT_QUEUE, or --input-queue")

        LOG.info("Connecting to Service Bus and listening on queue '%s'", queue_name)
        # determine effective listen duration (arg overrides instance config)
        effective_duration = listen_duration_seconds if listen_duration_seconds is not None else self.listen_duration_seconds
        end_time = (time.time() + effective_duration) if effective_duration and effective_duration > 0 else None

        # To ensure we process exactly one message at a time and avoid prefetching
        # multiple messages into the client, set prefetch_count=0 and request
        # a single message per receive call (max_message_count=1).
        if self.sb_fqdn:
            assert self.credential is not None
            client_ctx = ServiceBusClient(self.sb_fqdn, credential=self.credential)
        else:
            conn_str = str(self.connection_string)
            client_ctx = ServiceBusClient.from_connection_string(conn_str)

        with client_ctx as client:
            receiver = client.get_queue_receiver(queue_name=queue_name, max_wait_time=max_wait_time, prefetch_count=0)
            with receiver:
                while True:
                    # check timeout
                    if end_time is not None and time.time() >= end_time:
                        LOG.info("Listen duration of %s seconds elapsed, stopping listener", effective_duration)
                        # send a warning to the output queue with basic orchestrator info
                        details = {"input_queue": queue_name, "listen_duration_seconds": effective_duration}
                        self._send_warning_summary("listen_timeout", details=details)
                        break

                    try:
                        # receive a single message and block for up to max_wait_time
                        messages = receiver.receive_messages(max_message_count=1, max_wait_time=max_wait_time)
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
    parser.add_argument("--input-queue", required=False, help="Service Bus input queue name to listen to (overrides env INPUT_QUEUE)")
    parser.add_argument("--output-queue", required=False, help="Service Bus output queue name to send summaries to (overrides env OUTPUT_QUEUE)")
    parser.add_argument("--connection-string", required=False, help="Service Bus connection string (overrides env SERVICEBUS_CONNECTION_STRING)")
    parser.add_argument("--sb-fqdn", required=False, help="Service Bus fully-qualified namespace to use managed identity (overrides env SB_FQDN)")
    parser.add_argument("--listen-duration", required=False, type=int, help="How many seconds to listen before stopping and sending a warning summary (overrides env LISTEN_DURATION_SECONDS)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    orchestrator = ServiceBusOrchestrator(
        connection_string=args.connection_string,
        input_queue=args.input_queue,
        output_queue=args.output_queue,
        sb_fqdn=args.sb_fqdn,
    )
    orchestrator.listen_queue(listen_duration_seconds=args.listen_duration)


if __name__ == "__main__":
    main_from_env()
