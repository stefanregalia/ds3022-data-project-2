# Changed the name of the prefect file because of import/name overlap issues

# Importing dependencies

from __future__ import annotations
import requests
from prefect import flow, task, get_run_logger
import boto3
from botocore.exceptions import BotoCoreError, ClientError

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
def persist_fragments(path: str, frags: list[dict]) -> None:
    import json, os
    if not frags:
        return
    with open(path, "a") as f:
        for m in frags:
            f.write(json.dumps({"order_no": m["order_no"], "word": m["word"], "message_id": m["message_id"]}) + "\n")

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

# Defining our flow order to execute our tasks
@flow
def dp2(target_count: int = 21):
    logger = get_run_logger()

    # 1) Populate/reset the queue (Task 1). Run once per end-to-end attempt.
    queue_url = scatter()

    # 2) Accumulate parsed fragments here across multiple polls
    fragments: list[dict] = []
    seen_ids: set[str] = set()  # defensive dedup if SQS redelivers

    logger.info(f"Starting collection loop; aiming for {target_count} fragments.")

    # 3) Loop until we’ve received and deleted everything
    while len(fragments) < target_count:
        # (optional) Observability: check counts so logs show progress
        stats = get_queue_info(queue_url)
        logger.info(f"Queue snapshot before poll: {stats}")

        # Receive and parse up to 10 messages once (long polling inside)
        batch = receive_and_parse(queue_url)

        if not batch:
            # Nothing visible yet (likely delayed/invisible); try again
            # (Long polling already waited; no need for extra sleep)
            continue

        # Keep only truly new messages (if any dup deliveries occur)
        new_batch = [m for m in batch if m["message_id"] not in seen_ids]
        for m in new_batch:
            seen_ids.add(m["message_id"])

        # Store fragments for Task 3 (order_no + word kept in each dict)
        fragments.extend(new_batch)

        # Delete what we just stored so we don’t leave dangling messages
        rhandles = [m["receipt_handle"] for m in new_batch]
        delete_messages(queue_url, rhandles)

        logger.info(f"Collected {len(fragments)} / {target_count} so far.")

    logger.info("All fragments collected and deleted. Ready for Task 3.")
    # Return fragments for the next step (assembly/submission in Task 3)
    return fragments


if __name__ == "__main__":
    # quick smoke test: collect just a few fragments so you see logs immediately
    dp2()
