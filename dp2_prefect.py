# Changed the name of the prefect file because of import/name overlap issues

# Importing dependencies

from __future__ import annotations
import requests
from prefect import flow, task, get_run_logger
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pathlib import Path
import json, os
from typing import List, Dict

# Defining the API endpoint
url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xtm9px"

# Defining a boto3 SQS client to ensure current region
def sqs_client(region: str = "us-east-1"):
	return boto3.client("sqs", region_name = region)

# Defining our task to request the API to populate the queue
@task
def scatter() -> str:
	"""
	This task defines the prefect logger, makes a POST request, raises
	an error if the request failed, logs the call from the API, 
	converts json into Python dictionary, extracts the queue url,
	logs the queue url, and returns the queue url
	"""
	logger = get_run_logger() # Defining the prefect logger
	logger.info(f"Calling the scattered API endpoint for xtm9px")
	r = requests.post(url, timeout = 20) #Making a POST request
	r.raise_for_status() # Raising an error if one occurs
	payload = r.json() # Converting to python dictionary
	qurl = payload["sqs_url"] # Extracting the queue url
	logger.info(f"Queue URL received: {qurl}") 
	
	return qurl

# Task will re-run twice if it fails and waits 3 seconds to do so

@task(retries = 2, retry_delay_seconds = 3)
def get_queue_info(queue_url: str) -> dict:
	"""
	This tasks fetches SQS approximate counts and returns a structured dictionary, monitoring the queue
	"""

	logger = get_run_logger() # Defining the prefect logger
	sqs = sqs_client() # Instantiating the sqs client

	# Error handling: try block to fetch the counts of messages from sqs and parse it into integers, then into a log and a dictionary

	try:
		resp = sqs.get_queue_attributes(
			QueueUrl = queue_url,
			AttributeNames = [
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

		logger.info(
			f"Queue counts | visible={visible} not_visible={not_visible} delayed={delayed} total={total}"
		)

		return {
			"visible": visible,
			"not_visible": not_visible,
			"delayed": delayed,
			"total": total,
		}

	# Error handling: except block to log the type of error and return default values
	except (BotoCoreError, ClientError) as e:
		logger.warning(f"Failed to get queue attributes: {e}")
		return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}

@task
def receive_and_parse(
	queue_url: str, 
	max_messages: int = 10,
	wait_seconds: int = 10,
) -> list[dict]:

	logger = get_run_logger()
	sqs = sqs_client()

	try:
		resp = sqs.receive_message(
			QueueUrl = queue_url,
			MaxNumberOfMessages = max_messages,
			WaitTimeSeconds = wait_seconds,
			MessageAttributeNames = ["All"],

		)

	except (BotoCoreError, ClientError) as e:
		logger.warning(f"SQS receive_message failed: {e}")
		return []

	messages = resp.get("Messages", [])
	if not messages:
		logger.info("No messages available on this poll")
		return []
	
	parsed: list[dict] = []
	for m in messages:
		msg_id = m.get("MessageId", "")
		attrs = m.get("MessageAttributes") or {}
		rh = m.get("ReceiptHandle")

		raw_order = (attrs.get("order_no") or {}).get("StringValue")
		word = (attrs.get("word") or {}).get("StringValue")

		if raw_order is None or word is None:
			logger.warning(
				f"Message {msg_id} missing required attributes; skipping. attrs = {list(attrs.keys())}"
			)

			continue

		try:
			order_no = int(raw_order)
		except ValueError:
			logger.warning(f"Message {msg_id} has non-integer order_no='{raw_order}'; skipping.")
			continue

		if not rh:
			logger.warning(f"Message {msg_id} missing ReceiptHandle; cannot delete later. Skipping.")
			continue

		parsed.append(
			{
				"message_id": msg_id,
				"order_no": order_no,
				"word": word,
				"receipt_handle": rh,
			}
		)

	logger.info(f"Parsed {len(parsed)} valid fragment(s) from this poll.")
	return parsed

@task
def persist_fragments(path: str, frags: list[dict]) -> str:
    """
    Append parsed fragments to a local JSONL file (one JSON object per line).
    Called BEFORE deleting from SQS so the data is durable if the process dies.
    """
    logger = get_run_logger()
    if not frags:
        logger.info("No new fragments to persist this round.")
        return path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)  # ensure folder exists

    # write only the essentials; keep it tidy
    safe_batch = [
        {"message_id": f["message_id"], "order_no": f["order_no"], "word": f["word"]}
        for f in frags
    ]

    try:
        with p.open("a", encoding="utf-8") as fp:
            for row in safe_batch:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
            fp.flush()
            os.fsync(fp.fileno())  # best-effort durability
        logger.info(f"Persisted {len(safe_batch)} fragment(s) to {p}.")
    except OSError as e:
        # Don’t crash the run; log and continue. (You can choose to gate deletion on success if you prefer.)
        logger.warning(f"Failed to persist fragments to {p}: {e}")

    return str(p)

@task(retries = 2, retry_delay_seconds = 3)
def delete_messages(queue_url: str, receipt_handles: list[str]) -> dict:
	logger = get_run_logger()
	sqs = sqs_client()

	if not receipt_handles:
		logger.info("No messages to delete this round.")
		return {"deleted": 0, "failed": 0}

	
	def _chunks(lst, n):
		for i in range(0, len(lst), n):
			yield lst[i : i + n]

	total_deleted = 0
	total_failed = 0

	for batch in _chunks(receipt_handles, 10):
		entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(batch)]
		try:
			resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
		except (BotoCoreError, ClientError) as e:

			logger.warning(f"delete_message_batch failed: {e}")
			total_failed += len(entries)
			continue

		deleted = len(resp.get("Successful", []))
		failed = len(resp.get("Failed", []))
		total_deleted += deleted
		total_failed += failed

		if failed:
			failed_ids = [f.get("Id") for f in resp.get("Failed", [])]
			logger.warning(f"Batch delete partial failure. Failed Ids: {failed_ids}")

	logger.info(f"Deleted {total_deleted} message(s); {total_failed} failed to delete.")
	return {"deleted": total_deleted, "failed": total_failed}	

@task(retries=2, retry_delay_seconds=3)
def assemble_and_submit_fast(
    fragments: List[Dict],
    uvaid: str,
    platform: str = "prefect",
    expected_count: int = 21,
    submit_queue_url: str = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
    dry_run: bool = False,                 # set True to preview without submitting
    save_preview_path: str | None = "assembled_phrase.txt",
) -> str:
    logger = get_run_logger()
    if not fragments:
        raise RuntimeError("No fragments provided to assemble_and_submit_fast.")

    # --- validation (accepts 0..20 or 1..21) ---
    order_nos = [int(f["order_no"]) for f in fragments]
    words     = [f["word"] for f in fragments]

    if expected_count and len(fragments) != expected_count:
        logger.warning(f"Fragment count {len(fragments)} != expected {expected_count}.")

    dupes = [n for n in set(order_nos) if order_nos.count(n) > 1]
    if dupes:
        raise ValueError(f"Duplicate order_no detected: {sorted(dupes)}")
    if any(w is None or w == "" for w in words):
        raise ValueError("Empty word found in fragments.")

    # Require any contiguous block of length expected_count (0- or 1-based)
    mn = min(order_nos)
    required = set(range(mn, mn + expected_count))
    missing = sorted(required - set(order_nos))
    extra   = sorted(set(order_nos) - required)
    if missing:
        raise ValueError(f"Missing order numbers: {missing}")
    if extra:
        raise ValueError(f"Out-of-range order numbers: {extra}")

    # --- assemble ---
    ordered = sorted(fragments, key=lambda f: int(f["order_no"]))
    # sanity: strictly increasing?
    if any(ordered[i]["order_no"] >= ordered[i+1]["order_no"] for i in range(len(ordered)-1)):
        raise ValueError("order_no not strictly increasing after sort.")
    phrase = " ".join(f["word"] for f in ordered).strip()

    # Preview/logging
    logger.info(f"ASSEMBLED PHRASE ({len(ordered)} words): {phrase}")
    if save_preview_path:
        try:
            with open(save_preview_path, "w", encoding="utf-8") as fp:
                fp.write(phrase + "\n")
            logger.info(f"Saved preview -> {save_preview_path}")
        except OSError as e:
            logger.warning(f"Could not save preview file: {e}")

    if dry_run:
        logger.info("Dry run enabled: skipping submission to dp2-submit.")
        return phrase

    # --- submit once ---
    sqs = sqs_client()
    resp = sqs.send_message(
        QueueUrl=submit_queue_url,
        MessageBody=phrase,
        MessageAttributes={
            "uvaid":   {"DataType": "String", "StringValue": uvaid},
            "phrase":  {"DataType": "String", "StringValue": phrase},
            "platform":{"DataType": "String", "StringValue": platform},
        },
    )
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 200:
        raise RuntimeError(f"Submission failed (status {status}): {resp}")
    logger.info("✅ Submission accepted (HTTP 200).")
    return phrase

    # --- submit once ---
    sqs = sqs_client()
    resp = sqs.send_message(
        QueueUrl=submit_queue_url,
        MessageBody=phrase,
        MessageAttributes={
            "uvaid":   {"DataType": "String", "StringValue": uvaid},
            "phrase":  {"DataType": "String", "StringValue": phrase},
            "platform":{"DataType": "String", "StringValue": platform},
        },
    )
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 200:
        raise RuntimeError(f"Submission failed (status {status}): {resp}")
    logger.info("✅ Submission accepted (HTTP 200).")
    return phrase


# Defining our flow order to execute our tasks
import time

@flow
def dp2(target_count: int = 21):
    logger = get_run_logger()
    queue_url = scatter()

    by_order: dict[int, dict] = {}
    seen_ids: set[str] = set()

    # Stall detector config: how many consecutive “empty queue” snapshots before we bail
    CONSECUTIVE_EMPTY_LIMIT = 5
    consecutive_empty = 0

    logger.info(f"Starting collection loop; aiming for {target_count} unique fragments.")

    while len(by_order) < target_count:
        # Observability snapshot
        stats = get_queue_info(queue_url)
        logger.info(f"Queue snapshot before poll: {stats}")

        # Long-poll receive (WaitTimeSeconds=10 inside receive_and_parse)
        batch = receive_and_parse(queue_url)

        if not batch:
            # If broker says nothing is visible, not-visible, or delayed, count toward stall
            if stats["visible"] == 0 and stats["not_visible"] == 0 and stats["delayed"] == 0:
                consecutive_empty += 1
                logger.info(f"Empty-queue snapshot {consecutive_empty}/{CONSECUTIVE_EMPTY_LIMIT}.")
                if consecutive_empty >= CONSECUTIVE_EMPTY_LIMIT:
                    have = sorted(m["order_no"] for m in by_order.values())
                    if have:
                        mn = min(have)
                        required = set(range(mn, mn + target_count))
                        missing = sorted(required - set(have))
                    else:
                        # fallback if we literally have nothing yet
                        missing = list(range(1, target_count + 1))
                    raise RuntimeError(
                        f"Stall detected: queue empty for {CONSECUTIVE_EMPTY_LIMIT} consecutive polls, "
                        f"but only collected {len(by_order)}/{target_count}. Missing order numbers: {missing}"
                    )
            else:
                # There’s backlog somewhere (delayed or in-flight); reset stall counter
                consecutive_empty = 0
            continue

        # We received a batch -> reset stall counter
        consecutive_empty = 0

        # Dedup by MessageId (defense vs redelivery)
        new_msgs = [m for m in batch if m["message_id"] not in seen_ids]
        for m in new_msgs:
            seen_ids.add(m["message_id"])

        # Persist before delete (per rubric)
        persist_fragments("runs/dp2_fragments.jsonl", new_msgs)

        # Keep first per order_no
        added = 0
        for m in new_msgs:
            o = m["order_no"]
            if o not in by_order:
                by_order[o] = m
                added += 1

        # Delete EVERYTHING we polled so no dangles
        delete_messages(queue_url, [m["receipt_handle"] for m in batch])

        if added:
            logger.info(f"Collected {len(by_order)} unique / {target_count} required. (+{added} this round)")

    logger.info("Collected all required fragments; proceeding to validation/assembly.")

    fragments = list(by_order.values())
    final_phrase = assemble_and_submit_fast(
        fragments,
        uvaid="xtm9px",
        platform="prefect",
        expected_count=target_count,
        dry_run=True,  # set to False when you're ready to actually submit
    )
    return final_phrase





if __name__ == "__main__":
    # quick smoke test: collect just a few fragments so you see logs immediately
    dp2()
