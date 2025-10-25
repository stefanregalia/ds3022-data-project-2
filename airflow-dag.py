from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import List, Dict
import json
import os
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 10, 24, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,  # overridden to 0 on the scatter task to ensure a single POST
    "retry_delay": timedelta(seconds=3),
}

API_URL = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xtm9px"
INSTRUCTOR_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
UVAID = "xtm9px"
TARGET_COUNT = 21
PERSIST_PATH = "/tmp/airflow_fragments.jsonl"


def sqs_client(region: str = "us-east-1"):
    """Create and return a boto3 SQS client for the specified region."""
    return boto3.client("sqs", region_name=region)


def get_queue_info_task() -> Dict[str, int]:
    """
    Return queue counts for visible, in-flight, delayed messages (diagnostics).
    """
    context = get_current_context()
    log = context["ti"].log
    queue_url = context["ti"].xcom_pull(key="queue_url")
    sqs = sqs_client()

    try:
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
        stats = {"visible": visible, "not_visible": not_visible, "delayed": delayed, "total": total}
        log.info(
            "Queue counts | visible=%d not_visible=%d delayed=%d total=%d",
            visible, not_visible, delayed, total
        )
        return stats
    except (BotoCoreError, ClientError) as e:
        log.exception("Failed to get queue attributes: %s", e)
        return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}


def receive_and_parse_task() -> List[Dict]:
    """
    Long-poll the SQS queue for up to 10 messages and parse MessageAttributes
    into {message_id, order_no, word, receipt_handle}.
    """
    context = get_current_context()
    log = context["ti"].log
    queue_url = context["ti"].xcom_pull(key="queue_url")
    sqs = sqs_client()

    try:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,        # enable long polling
            MessageAttributeNames=["All"],
        )
    except (BotoCoreError, ClientError) as e:
        log.exception("SQS receive_message failed: %s", e)
        return []

    messages = resp.get("Messages", [])
    if not messages:
        log.info("No messages available on this poll.")
        return []

    log.info("Received %d message(s) from SQS. Parsing…", len(messages))

    parsed: List[Dict] = []
    for m in messages:
        msg_id = m.get("MessageId", "UNKNOWN")
        try:
            attrs = m.get("MessageAttributes")
            if attrs is None or not isinstance(attrs, dict):
                log.warning("Message %s has invalid MessageAttributes: %r", msg_id, attrs)
                continue

            rh = m.get("ReceiptHandle")
            order_attr = attrs.get("order_no")
            word_attr = attrs.get("word")

            if order_attr is None or word_attr is None:
                log.warning("Message %s missing required attributes. Present: %s", msg_id, list(attrs.keys()))
                continue

            raw_order = order_attr.get("StringValue") if isinstance(order_attr, dict) else order_attr
            word = word_attr.get("StringValue") if isinstance(word_attr, dict) else word_attr

            if raw_order is None or word is None:
                log.warning("Message %s missing StringValue(s). order_no=%r word=%r", msg_id, order_attr, word_attr)
                continue

            try:
                order_no = int(raw_order)
            except (ValueError, TypeError) as e:
                log.warning("Message %s has non-integer order_no=%r: %s", msg_id, raw_order, e)
                continue

            if not rh:
                log.warning("Message %s missing ReceiptHandle.", msg_id)
                continue

            parsed.append(
                {"message_id": msg_id, "order_no": order_no, "word": word, "receipt_handle": rh}
            )
            log.info("Parsed message %s… order_no=%d, word='%s'", msg_id[:8], order_no, word)

        except Exception as e:
            log.exception("UNEXPECTED ERROR parsing message %s: %s", msg_id, e)
            continue

    log.info("Parsed %d valid fragment(s) from %d received.", len(parsed), len(messages))
    return parsed


def persist_fragments_task(fragments: List[Dict], path: str = PERSIST_PATH) -> str:
    """
    Persist a batch of parsed fragments to a JSONL file for durability before deletion.
    """
    context = get_current_context()
    log = context["ti"].log

    if not fragments:
        log.info("No new fragments to persist this round.")
        return path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    safe_batch = [{"message_id": f["message_id"], "order_no": f["order_no"], "word": f["word"]} for f in fragments]

    try:
        with p.open("a", encoding="utf-8") as fp:
            for row in safe_batch:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
            fp.flush()
            os.fsync(fp.fileno())
        log.info("Persisted %d fragment(s) to %s.", len(safe_batch), p.as_posix())
    except OSError as e:
        log.warning("Failed to persist fragments to %s: %s", p.as_posix(), e)

    return str(p)


def delete_messages_task(receipt_handles: List[str]) -> Dict[str, int]:
    """
    Delete messages from SQS in batches of 10. Return counts of deleted/failed.
    """
    context = get_current_context()
    log = context["ti"].log
    queue_url = context["ti"].xcom_pull(key="queue_url")
    sqs = sqs_client()

    if not receipt_handles:
        log.info("No messages to delete this round.")
        return {"deleted": 0, "failed": 0}

    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i: i + n]

    total_deleted = 0
    total_failed = 0

    for batch in _chunks(receipt_handles, 10):
        entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(batch)]
        try:
            resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        except (BotoCoreError, ClientError) as e:
            log.warning("delete_message_batch failed for a batch of %d: %s", len(entries), e)
            total_failed += len(entries)
            continue

        deleted = len(resp.get("Successful", []))
        failed = len(resp.get("Failed", []))
        total_deleted += deleted
        total_failed += failed

        if failed:
            failed_ids = [f.get("Id") for f in resp.get("Failed", [])]
            log.warning("Batch delete partial failure. Failed Ids: %s", failed_ids)

    log.info("Deleted %d message(s); %d failed to delete.", total_deleted, total_failed)
    return {"deleted": total_deleted, "failed": total_failed}

def scatter_task() -> str:
    """
    Call the scatter API once to populate the student's queue with 21 messages.
    Pushes the queue URL to XCom for downstream tasks.
    """
    context = get_current_context()
    log = context["ti"].log
    log.info("Calling the scatter API endpoint for %s", UVAID)

    try:
        r = requests.post(API_URL, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        log.exception("Scatter API request failed: %s", e)
        raise

    payload = r.json()
    queue_url = payload["sqs_url"]
    log.info("Queue URL received: %s", queue_url)

    context["ti"].xcom_push(key="queue_url", value=queue_url)
    return queue_url


def collection_loop_task() -> List[Dict]:
    """
    Orchestrate queue monitoring, collection, persistence, and deletion
    until all 21 unique fragments (order_no 1..21) are collected.
    """
    context = get_current_context()
    log = context["ti"].log
    context["ti"].xcom_pull(key="queue_url")
    by_order: Dict[int, Dict] = {}
    seen_ids = set()
    CONSECUTIVE_EMPTY_LIMIT = 5
    consecutive_empty = 0

    log.info("Starting collection loop; aiming for %d unique fragments (order_no 1..21).", TARGET_COUNT)

    while len(by_order) < TARGET_COUNT:
        stats = get_queue_info_task()
        log.info("Queue snapshot before poll: %s", stats)

        batch = receive_and_parse_task()

        if not batch:
            if stats["visible"] == 0 and stats["not_visible"] == 0 and stats["delayed"] == 0:
                consecutive_empty += 1
                log.info("Empty-queue snapshot %d/%d.", consecutive_empty, CONSECUTIVE_EMPTY_LIMIT)
                if consecutive_empty >= CONSECUTIVE_EMPTY_LIMIT:
                    have = sorted(by_order.keys())
                    missing = sorted(set(range(1, TARGET_COUNT + 1)) - set(have))
                    raise RuntimeError(
                        f"Stall detected: queue empty for {CONSECUTIVE_EMPTY_LIMIT} consecutive polls, "
                        f"but only collected {len(by_order)}/{TARGET_COUNT}. Missing order numbers: {missing}"
                    )
            else:
                consecutive_empty = 0
            continue

        consecutive_empty = 0

        # Filter redeliveries
        new_msgs = [m for m in batch if m["message_id"] not in seen_ids]
        for m in new_msgs:
            seen_ids.add(m["message_id"])

        # Persist before deletion
        persist_fragments_task(new_msgs)

        # Keep first occurrence for each order_no
        added = 0
        for m in new_msgs:
            o = m["order_no"]
            if 1 <= o <= TARGET_COUNT and o not in by_order:
                by_order[o] = m
                added += 1

        delete_messages_task([m["receipt_handle"] for m in batch])

        if added:
            log.info("Collected %d unique / %d required. (+%d this round)", len(by_order), TARGET_COUNT, added)

    log.info("Collected all required fragments.")
    fragments = list(by_order.values())
    context["ti"].xcom_push(key="fragments", value=fragments)
    return fragments


def assemble_and_submit_task() -> str:
    """
    Validate fragments, assemble the phrase in order, and submit it
    to the instructor queue with platform="airflow".
    """
    context = get_current_context()
    log = context["ti"].log
    fragments: List[Dict] = context["ti"].xcom_pull(key="fragments")

    if not fragments or len(fragments) != TARGET_COUNT:
        raise RuntimeError(f"Expected {TARGET_COUNT} fragments; got {0 if not fragments else len(fragments)}")

    order_nos = [int(f["order_no"]) for f in fragments]
    words = [f["word"] for f in fragments]  # noqa: F841

    dupes = [n for n in set(order_nos) if order_nos.count(n) > 1]
    if dupes:
        raise ValueError(f"Duplicate order_no detected: {sorted(dupes)}")

    required = set(range(1, TARGET_COUNT + 1))
    missing = sorted(required - set(order_nos))
    if missing:
        raise ValueError(f"Missing order numbers: {missing}")

    ordered = [None] * TARGET_COUNT
    for f in fragments:
        ordered[f["order_no"] - 1] = f["word"]

    phrase = " ".join(ordered).strip()
    log.info("ASSEMBLED PHRASE (%d words): %s", len(ordered), phrase)

    with open("airflow_assembled_phrase.txt", "w", encoding="utf-8") as fp:
        fp.write(phrase + "\n")
    log.info("Saved preview -> airflow_assembled_phrase.txt")

    # Submit to queue
    sqs = sqs_client()
    resp = sqs.send_message(
        QueueUrl=INSTRUCTOR_QUEUE_URL,
        MessageBody=phrase,
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVAID},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "airflow"},
        },
    )
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 200:
        raise RuntimeError(f"Submission failed (status {status}): {resp}")

    log.info("Submission accepted (HTTP 200).")
    return phrase


# DAG

with DAG(
    dag_id="dp2_quote_assembler",
    default_args=default_args,
    description="DP2 Quote Assembler - Airflow Version",
    schedule=None,        # Airflow 3 preferred; schedule_interval=None also fine
    catchup=False,
    tags=["dp2", "sqs", "quote-assembler"],
) as dag:

    scatter = PythonOperator(
        task_id="scatter_api",
        python_callable=scatter_task,
        retries=0,  # call the POST exactly once
    )

    collect = PythonOperator(
        task_id="collection_loop",
        python_callable=collection_loop_task,
    )

    assemble = PythonOperator(
        task_id="assemble_and_submit",
        python_callable=assemble_and_submit_task,
    )

    scatter >> collect >> assemble

