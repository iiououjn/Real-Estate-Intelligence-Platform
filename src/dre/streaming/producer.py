"""Producers: real Kinesis, or a local loop that hands Kinesis-shaped batches straight to the Lambda handler."""
from __future__ import annotations

import base64
import time
from itertools import islice

from .lambda_handler import handler


def _batches(it, size):
    it = iter(it)
    while chunk := list(islice(it, size)):
        yield chunk


class KinesisProducer:
    """Uses put_records (max 500 records / 5 MB per call) and retries only the failed records."""

    def __init__(self, stream_name: str, region: str):
        import boto3

        self.stream, self.client = stream_name, boto3.client("kinesis", region_name=region)

    def send(self, payloads, partition_key_of) -> dict:
        sent = failed = 0
        for batch in _batches(payloads, 500):
            entries = [{"Data": p.encode(), "PartitionKey": partition_key_of(p)} for p in batch]
            for attempt in range(5):
                resp = self.client.put_records(StreamName=self.stream, Records=entries)
                if not resp["FailedRecordCount"]:
                    break
                entries = [e for e, r in zip(entries, resp["Records"]) if "ErrorCode" in r]   # retry failures only
                time.sleep(0.2 * 2 ** attempt)
            failed += len(entries) if resp["FailedRecordCount"] else 0
            sent += len(batch)
        return {"sent": sent, "failed": failed}


class LocalProducer:
    """Local stand-in for Kinesis + Lambda: wraps batches in the Kinesis event shape and invokes the handler."""

    def __init__(self, sink, batch_size: int = 100):
        self.sink, self.batch_size = sink, batch_size

    def send(self, payloads, partition_key_of=None) -> dict:
        totals = {"received": 0, "valid": 0, "rejected": 0}
        seq = 0
        for batch in _batches(payloads, self.batch_size):
            records = []
            for p in batch:
                seq += 1
                records.append({"kinesis": {"data": base64.b64encode(p.encode()).decode(), "sequenceNumber": f"{seq:020d}",
                                            "partitionKey": "local"}})
            result = handler({"Records": records}, None, sink=self.sink)
            for k in totals:
                totals[k] += result["stats"][k]
        return totals
