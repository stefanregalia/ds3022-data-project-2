# Importing dependencies

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


UVA_ID = "xtm9px" 
PLATFORM = "airflow"
SCATTER_ENDPOINT = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
AWS_REGION = "us-east-1"

# Behavior knobs 
TOTAL_MESSAGES_EXPECTED = 21
ATTR_POLL_INTERVAL_SEC = 5          # how often to check attributes between empty polls
ATTR_POLL_TIMEOUT_SEC = 16 * 60     # hard stop if delayes go past 900s for any reason
RECEIVE_WAIT_TIME_SEC = 10          # SQS long-poll time per call
RECEIVE_MAX_MSGS = 10               # SQS max per receive
RUNS_DIR = Path(__file__).resolve().parent / "runs"  # local artifacts live under dags/runs

def sqs_client(region: str = AWS_REGION):
    return boto3.client("sqs", region_name=region)

def now_utc_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

default_args = {
    "owner": "me",
    "depends_on_past": False,
    "retries": 2,  # light global retries
    "retry_delay": timedelta(seconds=3),
}

@dag(
    dag_id="dp2_quote_assembler",
    start_date=datetime(2025, 10, 1),
    schedule=None,  
    catchup=False,
    default_args=default_args,
    tags=["dp2", "sqs", "uvasds"],
)
def dp2_quote_assembler():
    # Tasks
    
    @task(retries=0)
    def scatter() -> str:
        """
        POST once per DAG run to populate my SQS queue with exactly 21 delayed messages.
        Returns my queue URL. Not called again mid-run.
        """
        r = requests.post(SCATTER_ENDPOINT, timeout=20)
        r.raise_for_status()
        payload = r.json()
        qurl = payload["sqs_url"]
        print(f"[{now_utc_ts()}] Queue URL received: {qurl}")
        return qurl

    @task(retries=2, retry_delay=timedelta(seconds=3))
    def delete_messages(queue_url: str, receipt_handles: List[str]) -> Dict:
        """
        Delete messages from SQS in batches of 10. Never leave dangling messages.
        """
        if not receipt_handles:
            print(f"[{now_utc_ts()}] Nothing to delete this round.")
            return {"deleted": 0, "failed": 0}

        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        sqs = sqs_client()
        total_deleted, total_failed = 0, 0
        for batch in chunks(receipt_handles, 10):
            entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(batch)]
            try:
                resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
            except (BotoCoreError, ClientError) as e:
                print(f"[{now_utc_ts()}] delete_message_batch failed: {e}")
                total_failed += len(entries)
                continue

            deleted = len(resp.get("Successful", []))
            failed = len(resp.get("Failed", []))
            total_deleted += deleted
            total_failed += failed
            if failed:
                failed_ids = [f.get("Id") for f in resp.get("Failed", [])]
                print(f"[{now_utc_ts()}] Partial delete failure. Failed Ids: {failed_ids}")

        print(f"[{now_utc_ts()}] Deleted {total_deleted}; failed {total_failed}.")
        return {"deleted": total_deleted, "failed": total_failed}

    @task(retries=0)
    def collect_all(queue_url: str) -> List[Dict]:
        """
        Main collection loop done inline (no cross-task calls inside a task).
        Strategy: snapshot counts, long-poll, parse, persist, delete; repeat until 21/21 or timeout.
        """
        sqs = sqs_client()

        def snapshot_counts(qurl: str) -> dict:
            try:
                resp = sqs.get_queue_attributes(
                    QueueUrl=qurl,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed",
                    ],
                )
                attrs = resp.get("Attributes", {})
                visible = int(attrs.get("ApproximateNumberOfMessages", 0))
                not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
                total = visible + not_visible + delayed
                print(f"[{now_utc_ts()}] Snapshot | visible={visible} not_visible={not_visible} delayed={delayed} total={total}")
                return {"visible": visible, "not_visible": not_visible, "delayed": delayed, "total": total}
            except (BotoCoreError, ClientError) as e:
                print(f"[{now_utc_ts()}] Snapshot failed: {e}")
                return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}

        def receive_once(qurl: str) -> List[Dict]:
            try:
                resp = sqs.receive_message(
                    QueueUrl=qurl,
                    MaxNumberOfMessages=RECEIVE_MAX_MSGS,
                    WaitTimeSeconds=RECEIVE_WAIT_TIME_SEC,
                    MessageAttributeNames=["All"],
                )
            except (BotoCoreError, ClientError) as e:
                print(f"[{now_utc_ts()}] receive_message failed: {e}")
                return []
            messages = resp.get("Messages", [])
            if not messages:
                print(f"[{now_utc_ts()}] No messages on this poll.")
                return []
            out: List[Dict] = []
            for m in messages:
                msg_id = m.get("MessageId", "UNKNOWN")
                attrs = m.get("MessageAttributes", {})
                rh = m.get("ReceiptHandle")
                order_attr = attrs.get("order_no")
                word_attr = attrs.get("word")
                if not rh or not isinstance(attrs, dict) or order_attr is None or word_attr is None:
                    print(f"[{now_utc_ts()}] Skipping malformed {msg_id}")
                    continue
                raw_order = order_attr.get("StringValue") if isinstance(order_attr, dict) else order_attr
                word = word_attr.get("StringValue") if isinstance(word_attr, dict) else word_attr
                try:
                    order_no = int(raw_order)
                except (ValueError, TypeError):
                    print(f"[{now_utc_ts()}] Non-integer order_no in {msg_id}: {raw_order}")
                    continue
                out.append({"message_id": msg_id, "order_no": order_no, "word": word, "receipt_handle": rh})
            print(f"[{now_utc_ts()}] Parsed {len(out)} valid fragment(s) this poll.")
            return out

        def delete_receipts(qurl: str, receipt_handles: List[str]) -> None:
            if not receipt_handles:
                return
            def chunks(lst, n):
                for i in range(0, len(lst), n):
                    yield lst[i:i+n]
            for batch in chunks(receipt_handles, 10):
                entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(batch)]
                try:
                    resp = sqs.delete_message_batch(QueueUrl=qurl, Entries=entries)
                except (BotoCoreError, ClientError) as e:
                    print(f"[{now_utc_ts()}] delete_message_batch failed: {e}")
                    continue
                failed = resp.get("Failed", [])
                if failed:
                    print(f"[{now_utc_ts()}] Partial delete failure: {[f.get('Id') for f in failed]}")

        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        by_order: dict[int, Dict] = {}
        seen_ids: set[str] = set()

        t_start = time.time()
        consecutive_empty = 0
        CONSECUTIVE_EMPTY_LIMIT = 5

        while len(by_order) < TOTAL_MESSAGES_EXPECTED:
            stats = snapshot_counts(queue_url)
            batch = receive_once(queue_url)

            if not batch:
                # No messages right now. Could be delayed or in-flight
                if stats["visible"] == 0 and stats["not_visible"] == 0 and stats["delayed"] == 0:
                    consecutive_empty += 1
                    print(f"[{now_utc_ts()}] Empty snapshot {consecutive_empty}/{CONSECUTIVE_EMPTY_LIMIT}")
                    if consecutive_empty >= CONSECUTIVE_EMPTY_LIMIT:
                        have = sorted(by_order.keys())
                        if have:
                            mn = min(have)
                            required = set(range(mn, mn + TOTAL_MESSAGES_EXPECTED))
                            missing = sorted(required - set(have))
                        else:
                            missing = list(range(1, TOTAL_MESSAGES_EXPECTED + 1))
                        raise AirflowFailException(
                            f"Stall: queue empty for {CONSECUTIVE_EMPTY_LIMIT} consecutive polls "
                            f"but collected {len(by_order)}/{TOTAL_MESSAGES_EXPECTED}. Missing: {missing}"
                        )
                else:
                    
                    consecutive_empty = 0

                # Global timeout guard (To stop forever looping errors)
                if time.time() - t_start > ATTR_POLL_TIMEOUT_SEC:
                    have = sorted(by_order.keys())
                    raise AirflowFailException(
                        f"Timeout after {ATTR_POLL_TIMEOUT_SEC}s with {len(by_order)}/{TOTAL_MESSAGES_EXPECTED} collected. "
                        f"Have orders: {have}"
                    )

                time.sleep(ATTR_POLL_INTERVAL_SEC)
                continue

            # We received messages in this poll
            consecutive_empty = 0

            # Persist before delete (only new MessageIDs)
            safe_batch = [
                {"message_id": f["message_id"], "order_no": f["order_no"], "word": f["word"]}
                for f in batch if f["message_id"] not in seen_ids
            ]
            if safe_batch:
                path = RUNS_DIR / "dp2_fragments.jsonl"
                try:
                    with path.open("a", encoding="utf-8") as fp:
                        for row in safe_batch:
                            fp.write(json.dumps(row, ensure_ascii=False) + "\n")
                        fp.flush()
                        os.fsync(fp.fileno())
                    print(f"[{now_utc_ts()}] Persisted {len(safe_batch)} fragment(s) -> {path}")
                except OSError as e:
                    print(f"[{now_utc_ts()}] Persist error: {e}")

            # Track unique by order_no; keep the first one we see for each slot
            added = 0
            for m in batch:
                if m["message_id"] not in seen_ids:
                    seen_ids.add(m["message_id"])
                    if m["order_no"] not in by_order:
                        by_order[m["order_no"]] = m
                        added += 1

            # Delete everything we received (including duplicates we didn't keep)
            delete_receipts(queue_url, [m["receipt_handle"] for m in batch])

            if added:
                print(f"[{now_utc_ts()}] Progress: {len(by_order)} unique / {TOTAL_MESSAGES_EXPECTED} (+{added})")

            # Timeout guard again
            if time.time() - t_start > ATTR_POLL_TIMEOUT_SEC:
                have = sorted(by_order.keys())
                raise AirflowFailException(
                    f"Timeout after {ATTR_POLL_TIMEOUT_SEC}s with {len(by_order)}/{TOTAL_MESSAGES_EXPECTED} collected. "
                    f"Have orders: {have}"
                )

        print(f"[{now_utc_ts()}] Collected all {TOTAL_MESSAGES_EXPECTED} fragments.")
        return list(by_order.values())

    @task(retries=2, retry_delay=timedelta(seconds=3))
    def assemble_and_submit_fast(fragments: List[Dict]) -> str:
        """
        Validate, assemble, submit with attributes and verify HTTP 200.
        Strict checks: duplicates, missing, non-empty words, strictly increasing order_no.
        """
        if not fragments:
            raise AirflowFailException("No fragments provided to assemble_and_submit_fast.")

        order_nos = [int(f["order_no"]) for f in fragments]
        words = [f["word"] for f in fragments]

        if len(fragments) != TOTAL_MESSAGES_EXPECTED:
            print(f"[{now_utc_ts()}] Fragment count {len(fragments)} != expected {TOTAL_MESSAGES_EXPECTED}")

        dupes = [n for n in set(order_nos) if order_nos.count(n) > 1]
        if dupes:
            raise AirflowFailException(f"Duplicate order_no detected: {sorted(dupes)}")

        if any(w is None or w == "" for w in words):
            raise AirflowFailException("Empty word found in fragments.")

        mn = min(order_nos)
        required = set(range(mn, mn + TOTAL_MESSAGES_EXPECTED))
        missing = sorted(required - set(order_nos))
        extra = sorted(set(order_nos) - required)
        if missing:
            raise AirflowFailException(f"Missing order numbers: {missing}")
        if extra:
            raise AirflowFailException(f"Out-of-range order numbers: {extra}")

        ordered = sorted(fragments, key=lambda f: int(f["order_no"]))
        if any(ordered[i]["order_no"] >= ordered[i + 1]["order_no"] for i in range(len(ordered) - 1)):
            raise AirflowFailException("order_no not strictly increasing after sort.")

        phrase = " ".join(f["word"] for f in ordered).strip()
        print(f"[{now_utc_ts()}] ASSEMBLED PHRASE ({len(ordered)} words): {phrase}")

      
        try:
            preview = RUNS_DIR / "assembled_phrase_airflow.txt"
            RUNS_DIR.mkdir(parents=True, exist_ok=True)
            with preview.open("w", encoding="utf-8") as fp:
                fp.write(phrase + "\n")
            print(f"[{now_utc_ts()}] Saved preview -> {preview}")
        except OSError as e:
            print(f"[{now_utc_ts()}] Could not save preview: {e}")

        # Send to submit queue with required attributes
        sqs = sqs_client()
        resp = sqs.send_message(
            QueueUrl=SUBMIT_QUEUE_URL,
            MessageBody=phrase,
            MessageAttributes={
                "uvaid": {"DataType": "String", "StringValue": UVA_ID},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": PLATFORM},
            },
        )
        status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 200:
            raise AirflowFailException(f"Submission failed (status {status}): {resp}")

        print(f"[{now_utc_ts()}] Submission accepted (HTTP 200).")
        return phrase

    # scatter -> collect_all -> assemble_and_submit_fast 
    qurl = scatter()
    frags = collect_all(qurl)
    phrase = assemble_and_submit_fast(frags)
    # Final phrase is stored in XCom for visibility

dp2_quote_assembler()

