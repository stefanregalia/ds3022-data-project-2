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

# --------------------------
# CONFIG — edit these two
# --------------------------
UVA_ID = "xtm9px"  # replace with *my* UVA ID
PLATFORM = "airflow"
SCATTER_ENDPOINT = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
AWS_REGION = "us-east-1"

# Behavior knobs (kept identical to my Prefect flow)
TOTAL_MESSAGES_EXPECTED = 21
ATTR_POLL_INTERVAL_SEC = 5          # how often to check queue attributes
ATTR_POLL_TIMEOUT_SEC = 16 * 60     # hard cap ~16 minutes (random delays up to 900s)
RECEIVE_WAIT_TIME_SEC = 10          # SQS long poll per receive_message call
RECEIVE_MAX_MSGS = 10               # max messages per poll (SQS limit)
RUNS_DIR = Path(__file__).resolve().parent / "runs"  # local artifacts live under dags/runs

def sqs_client(region: str = AWS_REGION):
    return boto3.client("sqs", region_name=region)

def now_utc_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

default_args = {
    "owner": "me",
    "depends_on_past": False,
    "retries": 2,  # light retries where it helps (individual tasks also retry if needed)
    "retry_delay": timedelta(seconds=3),
}

@dag(
    dag_id="dp2_quote_assembler",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # I can switch to a cron later if I want scheduled runs
    catchup=False,
    default_args=default_args,
    tags=["dp2", "sqs", "uvasds"],
)
def dp2_quote_assembler():
    # --- Helper tasks implemented with TaskFlow to keep my code readable and testable ---

    @task(retries=0)
    def scatter() -> str:
        """
        POST once per DAG run to populate my SQS queue with exactly 21 delayed messages.
        Returns my queue URL. This step *must not* be repeated mid-run.
        """
        r = requests.post(SCATTER_ENDPOINT, timeout=20)
        r.raise_for_status()
        payload = r.json()
        qurl = payload["sqs_url"]
        print(f"[{now_utc_ts()}] Queue URL received: {qurl}")
        return qurl

    @task(retries=2, retry_delay=timedelta(seconds=3))
    def get_queue_info(queue_url: str) -> dict:
        """
        Grab ApproximateNumberOfMessages, NotVisible, and Delayed as a quick snapshot.
        If I hit an API hiccup, return zeros and keep going — the loop logic accounts for that.
        """
        try:
            sqs = sqs_client()
            resp = sqs.get_queue_attributes(
                QueueUrl=queue_url,
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
            print(f"[{now_utc_ts()}] Queue counts | visible={visible} not_visible={not_visible} delayed={delayed} total={total}")
            return {"visible": visible, "not_visible": not_visible, "delayed": delayed, "total": total}
        except (BotoCoreError, ClientError) as e:
            print(f"[{now_utc_ts()}] Failed to get queue attributes: {e}")
            return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}

    @task(retries=0)
    def receive_and_parse(queue_url: str) -> List[Dict]:
        """
        Long-poll once for up to 10 messages. Parse MessageAttributes (order_no, word),
        carry the receipt handle for deletes, and be defensive about malformed stuff.
        Returns a list of parsed fragment dicts.
        """
        sqs = sqs_client()
        try:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
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

        print(f"[{now_utc_ts()}] Received {len(messages)} message(s). Parsing...")
        parsed: List[Dict] = []
        for m in messages:
            msg_id = m.get("MessageId", "UNKNOWN")
            attrs = m.get("MessageAttributes")
            if not isinstance(attrs, dict):
                print(f"[{now_utc_ts()}] Message {msg_id} missing/invalid MessageAttributes")
                continue

            rh = m.get("ReceiptHandle")
            order_attr = attrs.get("order_no")
            word_attr = attrs.get("word")

            if order_attr is None or word_attr is None:
                print(f"[{now_utc_ts()}] Message {msg_id} missing required attributes")
                continue

            # Extract StringValue; tolerate plain strings but log it.
            raw_order = order_attr.get("StringValue") if isinstance(order_attr, dict) else order_attr
            word = word_attr.get("StringValue") if isinstance(word_attr, dict) else word_attr
            if raw_order is None or word is None:
                print(f"[{now_utc_ts()}] Message {msg_id} missing StringValue")
                continue

            try:
                order_no = int(raw_order)
            except (ValueError, TypeError):
                print(f"[{now_utc_ts()}] Message {msg_id} has non-integer order_no='{raw_order}'")
                continue

            if not rh:
                print(f"[{now_utc_ts()}] Message {msg_id} missing ReceiptHandle")
                continue

            parsed.append(
                {"message_id": msg_id, "order_no": order_no, "word": word, "receipt_handle": rh}
            )
            print(f"[{now_utc_ts()}] Parsed {msg_id[:8]}... order_no={order_no}, word='{word}'")

        print(f"[{now_utc_ts()}] Parsed {len(parsed)} valid fragment(s).")
        return parsed

    @task(retries=0)
    def persist_fragments(frags: List[Dict]) -> str:
        """
        Append parsed fragments to a JSONL artifact under dags/runs.
        I don’t commit this to the repo; it’s just a runtime breadcrumb trail.
        """
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        path = RUNS_DIR / "dp2_fragments.jsonl"

        if not frags:
            print(f"[{now_utc_ts()}] No new fragments to persist this round.")
            return str(path)

        safe_batch = [
            {"message_id": f["message_id"], "order_no": f["order_no"], "word": f["word"]}
            for f in frags
        ]
        try:
            with path.open("a", encoding="utf-8") as fp:
                for row in safe_batch:
                    fp.write(json.dumps(row, ensure_ascii=False) + "\n")
                fp.flush()
                os.fsync(fp.fileno())
            print(f"[{now_utc_ts()}] Persisted {len(safe_batch)} fragment(s) -> {path}")
        except OSError as e:
            # Not fatal to the run — just warn
            print(f"[{now_utc_ts()}] Failed to persist fragments: {e}")
        return str(path)

    @task(retries=2, retry_delay=timedelta(seconds=3))
    def delete_messages(queue_url: str, receipt_handles: List[str]) -> Dict:
        """
        Delete messages from SQS in batches of 10. Don’t leave anything dangling.
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

    @task(retries=2, retry_delay=timedelta(seconds=3))
    def assemble_and_submit_fast(fragments: List[Dict]) -> str:
        """
        Same strict validation as my Prefect version:
          - must have exactly one of each required order_no (dup/missing => fail)
          - words must be non-empty
          - sorted order must be strictly increasing
        Then submit the phrase to dp2-submit with uvaid/phrase/platform attributes.
        """
        if not fragments:
            raise AirflowFailException("No fragments provided to assemble_and_submit_fast.")

        order_nos = [int(f["order_no"]) for f in fragments]
        words = [f["word"] for f in fragments]

        # Warn if counts differ (this DAG aims to be strict anyway)
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

        # Save a tiny preview artifact (optional)
        try:
            preview = RUNS_DIR / "assembled_phrase_airflow.txt"
            RUNS_DIR.mkdir(parents=True, exist_ok=True)
            with preview.open("w", encoding="utf-8") as fp:
                fp.write(phrase + "\n")
            print(f"[{now_utc_ts()}] Saved preview -> {preview}")
        except OSError as e:
            print(f"[{now_utc_ts()}] Could not save preview: {e}")

        # Submit to dp2-submit
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

    @task(retries=0)
    def collect_all(queue_url: str) -> List[Dict]:
        """
        Main collection loop (single task to keep the DAG graph sane).
        I track unique fragments by order_no and delete everything I touch.
        I stop when I have the full set or the attribute-based timeout fires.
        """
        by_order: dict[int, Dict] = {}
        seen_ids: set[str] = set()

        t_start = time.time()
        consecutive_empty = 0
        CONSECUTIVE_EMPTY_LIMIT = 5

        while len(by_order) < TOTAL_MESSAGES_EXPECTED:
            # Snap attributes to know what's going on before the poll
            stats = get_queue_info.expand(queue_url=[queue_url])[0]  # call the task inline once
            print(f"[{now_utc_ts()}] Snapshot: {stats}")

            batch = receive_and_parse.expand(queue_url=[queue_url])[0]
            # .expand returns a list result; since we pass one element, pull the first
            if not isinstance(batch, list):
                batch = []

            if not batch:
                # No messages this poll — either delayed, in-flight, or actually empty
                if stats["visible"] == 0 and stats["not_visible"] == 0 and stats["delayed"] == 0:
                    consecutive_empty += 1
                    print(f"[{now_utc_ts()}] Empty snapshot {consecutive_empty}/{CONSECUTIVE_EMPTY_LIMIT}")
                    if consecutive_empty >= CONSECUTIVE_EMPTY_LIMIT:
                        have = sorted(by_order.keys())
                        missing = []
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
                    consecutive_empty = 0  # there is life in the queue; just not visible to me yet

                # Also guard on overall timeout tied to the delayed range
                if time.time() - t_start > ATTR_POLL_TIMEOUT_SEC:
                    have = sorted(by_order.keys())
                    raise AirflowFailException(
                        f"Timeout after {ATTR_POLL_TIMEOUT_SEC}s with {len(by_order)}/{TOTAL_MESSAGES_EXPECTED} collected. "
                        f"Have orders: {have}"
                    )

                # Short breath between polls so I’m not hammering the API
                time.sleep(ATTR_POLL_INTERVAL_SEC)
                continue

            # Reset empty counter on any receipt
            consecutive_empty = 0

            # Only new messages by MessageId
            new_msgs = [m for m in batch if m["message_id"] not in seen_ids]
            for m in new_msgs:
                seen_ids.add(m["message_id"])

            # Persist what I got before deletion (artifact)
            persist_fragments.expand(frags=[new_msgs])

            # Keep first occurrence per order_no
            added = 0
            for m in new_msgs:
                o = m["order_no"]
                if o not in by_order:
                    by_order[o] = m
                    added += 1

            # Delete everything I received this poll (including duplicates I didn’t keep)
            delete_messages.expand(
                queue_url=[queue_url],
                receipt_handles=[[m["receipt_handle"] for m in batch]],
            )

            if added:
                print(f"[{now_utc_ts()}] Progress: {len(by_order)} unique / {TOTAL_MESSAGES_EXPECTED} (+{added})")

            # Guard on overall timeout
            if time.time() - t_start > ATTR_POLL_TIMEOUT_SEC:
                have = sorted(by_order.keys())
                raise AirflowFailException(
                    f"Timeout after {ATTR_POLL_TIMEOUT_SEC}s with {len(by_order)}/{TOTAL_MESSAGES_EXPECTED} collected. "
                    f"Have orders: {have}"
                )

        print(f"[{now_utc_ts()}] Collected all {TOTAL_MESSAGES_EXPECTED} fragments.")
        return list(by_order.values())

    # --- Orchestration: scatter -> collect_all -> assemble_and_submit_fast ---
    qurl = scatter()
    frags = collect_all(qurl)
    phrase = assemble_and_submit_fast(frags)
    # phrase XCom holds the final string; visible in the UI/logs

dp2_quote_assembler()

