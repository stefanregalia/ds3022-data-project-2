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

# Task to receive and parse messages
@task
def receive_and_parse(
	queue_url: str, 
	max_messages: int = 10, # Takes a max of 10 messages per poll
	wait_seconds: int = 10, # Waits 10 seconds to check for newly received messages
) -> list[dict]:

	"""
	This task polls the SQS queue for messages, parsing the MessageAttributes (order_no, word),
	then returns a list of parsed fragments with receipt handles. It also applies error handling for
	instances where there are malformed or missing attributes.
	"""

	logger = get_run_logger()
	sqs = sqs_client()

	# Try and except block for error handling failed message receptions
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

	# Log for when messages are either received or not received in a poll
	messages = resp.get("Messages", [])
	if not messages:
		logger.info("No messages available on this poll")
		return []
	
	logger.info(f"Received {len(messages)} message(s) from SQS. Beginning parse.")

	# Getting the MessageID, implementing error handling for errors related to MessageAttributes
	parsed: list[dict] = []
	for m in messages:
		msg_id = m.get("MessageId", "UNKNOWN")
		
		try:
			attrs = m.get("MessageAttributes")
			logger.info(f"📋 Message {msg_id[:8]}... attrs type: {type(attrs)}")  
			
			if attrs is None:
				logger.warning(f"Message {msg_id} has MessageAttributes = None")
				continue
			
			if not isinstance(attrs, dict):
				logger.warning(f"Message {msg_id} has MessageAttributes of wrong type: {type(attrs)}")
				continue
			
			rh = m.get("ReceiptHandle") # Defining the receipt handle

			order_attr = attrs.get("order_no")
			word_attr = attrs.get("word")
			
			logger.info(f"Message {msg_id[:8]}... order_attr type: {type(order_attr)}, word_attr type: {type(word_attr)}")

			# Logging missing word or order_no 
			if order_attr is None:
				logger.warning(f"Message {msg_id} missing 'order_no' attribute. Available: {list(attrs.keys())}")
				continue
			
			if word_attr is None:
				logger.warning(f"Message {msg_id} missing 'word' attribute. Available: {list(attrs.keys())}")
				continue
			
			# Extract StringValue 
			if isinstance(order_attr, dict):
				raw_order = order_attr.get("StringValue")
			elif isinstance(order_attr, str):
				logger.warning(f"Message {msg_id} has order_no as plain string: '{order_attr}' (expected dict)")
				raw_order = order_attr
			else:
				logger.warning(f"Message {msg_id} has order_no of unexpected type: {type(order_attr)}")
				continue
			
			if isinstance(word_attr, dict):
				word = word_attr.get("StringValue")
			elif isinstance(word_attr, str):
				logger.warning(f"Message {msg_id} has word as plain string: '{word_attr}' (expected dict)")
				word = word_attr
			else:
				logger.warning(f"Message {msg_id} has word of unexpected type: {type(word_attr)}")
				continue

			if raw_order is None or word is None:
				logger.warning(
					f"Message {msg_id} missing StringValue. order_no={order_attr}, word={word_attr}"
				)
				continue

			try:
				order_no = int(raw_order)
			except (ValueError, TypeError) as e:
				logger.warning(f"Message {msg_id} has non-integer order_no='{raw_order}': {e}")
				continue

			if not rh:
				logger.warning(f"Message {msg_id} missing ReceiptHandle")
				continue

			# Appending all correct message_id's, order_no's, words, and recepit handles to the parsed list
			parsed.append(
				{
					"message_id": msg_id,
					"order_no": order_no,
					"word": word,
					"receipt_handle": rh,
				}
			)
			logger.info(f"Parsed message {msg_id[:8]}... order_no={order_no}, word='{word}'")  

		# Logging parsing errors
		except Exception as e:
			logger.error(f"UNEXPECTED ERROR parsing message {msg_id}: {type(e).__name__}: {e}")
			logger.error(f"Message structure: {m}")
			continue

	logger.info(f"Parsed {len(parsed)} valid fragment(s) from {len(messages)} received.")
	return parsed

# Task to create a file storing the parsed fragments
@task
def persist_fragments(path: str, frags: list[dict]) -> str:
    """
    Append parsed fragments to a local JSONL file
    """
    logger = get_run_logger()
    if not frags:
        logger.info("No new fragments to persist this round.") # Logging when there are no new fragments
        return path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)  # Ensuring folder exists

    # Arranging the parsed fragments
    safe_batch = [
        {"message_id": f["message_id"], "order_no": f["order_no"], "word": f["word"]}
        for f in frags
    ]

	# Error handling for whether or not the json file is created successfully from the parsed fragments
    try:
        with p.open("a", encoding="utf-8") as fp:
            for row in safe_batch:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
            fp.flush()
            os.fsync(fp.fileno())  
        logger.info(f"Persisted {len(safe_batch)} fragment(s) to {p}.")
    except OSError as e:
        logger.warning(f"Failed to persist fragments to {p}: {e}")

    return str(p)

# Task to delete messages

@task(retries = 2, retry_delay_seconds = 3)
def delete_messages(queue_url: str, receipt_handles: list[str]) -> dict:
	"""
	Deletes messages from SQS queue in batches of up to 10, then returns a summary of successful 
	and failed deletions. Task will retry up to 2 times if it fails, waiting 3 seconds between attempts.
	"""
	# Initializing Prefect logger and SQS client
	logger = get_run_logger()
	sqs = sqs_client()
	
	# If there are no receipt handles, nothing to delete
	if not receipt_handles:
		logger.info("No messages to delete this round.")
		return {"deleted": 0, "failed": 0}
	
	# Helper function to split receipt handles into chunks of size n
	# SQS allows max 10 messages per delete_message_batch call
	def _chunks(lst, n):
		for i in range(0, len(lst), n):
			yield lst[i : i + n]
	
	# Initialize counters for tracking deletion results
	total_deleted = 0
	total_failed = 0
	
	# Process receipt handles in batches of 10
	for batch in _chunks(receipt_handles, 10):
		# Format entries for SQS batch delete API
		entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(batch)]
		
		# Attempt to delete this batch with error handling
		try:
			resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
		except (BotoCoreError, ClientError) as e:
			# If the entire batch delete fails, log error and count all as failed
			logger.warning(f"delete_message_batch failed: {e}")
			total_failed += len(entries)
			continue  # Move to next batch
		
		# Count successful and failed deletions from the response
		deleted = len(resp.get("Successful", []))
		failed = len(resp.get("Failed", []))
		total_deleted += deleted
		total_failed += failed
		
		# Logs if some messages in the batch failed to delete
		if failed:
			failed_ids = [f.get("Id") for f in resp.get("Failed", [])]
			logger.warning(f"Batch delete partial failure. Failed Ids: {failed_ids}")
	
	# Log final summary of deletion operation
	logger.info(f"Deleted {total_deleted} message(s); {total_failed} failed to delete.")
	return {"deleted": total_deleted, "failed": total_failed}

# Task to reassemble messages and submit to SQS
@task(retries=2, retry_delay_seconds=3)
def assemble_and_submit_fast(
    fragments: List[Dict],
    uvaid: str,
    platform: str = "prefect",
    expected_count: int = 21,
    submit_queue_url: str = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
    dry_run: bool = False,                 
    save_preview_path: str | None = "assembled_phrase.txt",
) -> str:
    """
    Validates fragments, assembles them into a complete phrase, and submits to SQS.
    Performs comprehensive validation to ensure all required order numbers are present
    with no duplicates. Task will retry up to 2 times if submission fails.
    """
    # Initializing Prefect logger
    logger = get_run_logger()
    
    # Ensure we have fragments to work with
    if not fragments:
        raise RuntimeError("No fragments provided to assemble_and_submit_fast.")
    
    # Extract order numbers and words from all fragments for validation
    order_nos = [int(f["order_no"]) for f in fragments]
    words     = [f["word"] for f in fragments]
    
    # Warning if fragment count doesn't match expected
    if expected_count and len(fragments) != expected_count:
        logger.warning(f"Fragment count {len(fragments)} != expected {expected_count}.")
    
    # Check for duplicate order numbers (API debugging)
	
    dupes = [n for n in set(order_nos) if order_nos.count(n) > 1]
    if dupes:
        raise ValueError(f"Duplicate order_no detected: {sorted(dupes)}")
    
    # Ensure no fragments have empty or missing words
    if any(w is None or w == "" for w in words):
        raise ValueError("Empty word found in fragments.")
    
    # Validate we have a complete contiguous sequence of order numbers
    # Handles both 0-based (0-20) and 1-based (1-21) indexing (API Debugging)
    mn = min(order_nos)
    required = set(range(mn, mn + expected_count))
    missing = sorted(required - set(order_nos))
    extra   = sorted(set(order_nos) - required)
    
    # Fail if any required order numbers are missing
    if missing:
        raise ValueError(f"Missing order numbers: {missing}")
    
    # Fail if we have order numbers outside the expected range
    if extra:
        raise ValueError(f"Out-of-range order numbers: {extra}")
    
    # Sort fragments by order_no to get correct word sequence
    ordered = sorted(fragments, key=lambda f: int(f["order_no"]))
    
    # Sanity check that order numbers are strictly increasing after sort
    if any(ordered[i]["order_no"] >= ordered[i+1]["order_no"] for i in range(len(ordered)-1)):
        raise ValueError("order_no not strictly increasing after sort.")
    
    # Join all words together with spaces to create the final phrase
    phrase = " ".join(f["word"] for f in ordered).strip()
    
    # Log the assembled phrase for verification
    logger.info(f"ASSEMBLED PHRASE ({len(ordered)} words): {phrase}")
    
    # Save phrase to a local file for preview
    if save_preview_path:
        try:
            with open(save_preview_path, "w", encoding="utf-8") as fp:
                fp.write(phrase + "\n")
            logger.info(f"Saved preview -> {save_preview_path}")
        except OSError as e:
            # Don't fail the entire task if file save fails
            logger.warning(f"Could not save preview file: {e}")
    
    # If dry run is enabled, skip submission and return the phrase
    if dry_run:
        logger.info("Dry run enabled: skipping submission to dp2-submit.")
        return phrase
    
    # Create SQS client for submission
    sqs = sqs_client()
    
    # Submitting the assembled phrase
    resp = sqs.send_message(
        QueueUrl=submit_queue_url,
        MessageBody=phrase,
        MessageAttributes={
            "uvaid":   {"DataType": "String", "StringValue": uvaid},      
            "phrase":  {"DataType": "String", "StringValue": phrase},    
            "platform":{"DataType": "String", "StringValue": platform},   
        },
    )
    
    # Extract HTTP status code from response metadata
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    # Verify submission was successful (200 OK)
    if status != 200:
        raise RuntimeError(f"Submission failed (status {status}): {resp}")
    
    # Log successful submission
    logger.info("Submission accepted (HTTP 200).")
    return phrase

# Defining our flow order to execute our tasks
@flow
def dp2(target_count: int = 21):
    """
    Main Prefect flow that orchestrates the entire data pipeline:
    1. Calls scatter API to populate SQS queue
    2. Monitors queue and collects all message fragments
    3. Assembles fragments into complete phrase
    4. Submits result to instructor queue
    """
    # Initialize Prefect logger and call scatter API to get queue URL
    logger = get_run_logger()
    queue_url = scatter()
    
    # Dictionary to store unique fragments by order_no (prevents duplicates)
    by_order: dict[int, dict] = {}
    # Set to track message IDs we've already processed (prevents redelivery duplicates)
    seen_ids: set[str] = set()
    
    # Stall detector configuration to prevent infinite loops
    # If queue is completely empty for this many consecutive polls, assume something is wrong
    CONSECUTIVE_EMPTY_LIMIT = 5
    consecutive_empty = 0
    
    # Log the start of collection process
    logger.info(f"Starting collection loop; aiming for {target_count} unique fragments.")
    
    # Main collection loop - continue until we have all required fragments
    while len(by_order) < target_count:
        # Get current queue statistics before polling for messages
        stats = get_queue_info(queue_url)
        logger.info(f"Queue snapshot before poll: {stats}")
        
        # Long-poll for messages (waits up to 10 seconds for messages to arrive)
        batch = receive_and_parse(queue_url)
        
        # Handle case where no messages were received this poll
        if not batch:
            # Check if queue is completely empty (no visible, in-flight, or delayed messages)
            if stats["visible"] == 0 and stats["not_visible"] == 0 and stats["delayed"] == 0:
                # Increment empty queue counter
                consecutive_empty += 1
                logger.info(f"Empty-queue snapshot {consecutive_empty}/{CONSECUTIVE_EMPTY_LIMIT}.")
                
                # If queue has been empty too long, something is wrong
                if consecutive_empty >= CONSECUTIVE_EMPTY_LIMIT:
                    # Calculate which order numbers we're still missing
                    have = sorted(m["order_no"] for m in by_order.values())
                    if have:
                        mn = min(have)
                        required = set(range(mn, mn + target_count))
                        missing = sorted(required - set(have))
                    else:
                        # If we haven't collected anything yet, assume 1-based indexing
                        missing = list(range(1, target_count + 1))
                    
                    # Raise error with diagnostic information
                    raise RuntimeError(
                        f"Stall detected: queue empty for {CONSECUTIVE_EMPTY_LIMIT} consecutive polls, "
                        f"but only collected {len(by_order)}/{target_count}. Missing order numbers: {missing}"
                    )
            else:
                # Queue has messages but they're delayed or in-flight, reset stall counter
                consecutive_empty = 0
            continue  # Try polling again
        
        # We received messages, so reset the stall counter
        consecutive_empty = 0
        
        # Filter out any messages we've already seen (defense against SQS redelivery)
        new_msgs = [m for m in batch if m["message_id"] not in seen_ids]
        # Add new message IDs to our tracking set
        for m in new_msgs:
            seen_ids.add(m["message_id"])
        
        # Persist fragments to local file before deleting from queue (per rubric requirement)
        persist_fragments("runs/dp2_fragments.jsonl", new_msgs)
        
        # Add fragments to our collection, keeping only first occurrence of each order_no
        added = 0
        for m in new_msgs:
            o = m["order_no"]
            # Only store if we haven't seen this order_no before
            if o not in by_order:
                by_order[o] = m
                added += 1
        
        # Delete all messages from this batch from queue to prevent reprocessing
        # This prevents dangling messages per rubric requirement
        delete_messages(queue_url, [m["receipt_handle"] for m in batch])
        
        # Log progress if we added any new unique fragments this round
        if added:
            logger.info(f"Collected {len(by_order)} unique / {target_count} required. (+{added} this round)")
    
    # We've collected all required fragments, proceed to assembly
    logger.info("Collected all required fragments; proceeding to validation/assembly.")
    
    # Convert dictionary values to list for assembly function
    fragments = list(by_order.values())
    
    # Validate, assemble, and submit the final phrase
    final_phrase = assemble_and_submit_fast(
        fragments,
        uvaid="xtm9px",              # UVA computing ID
        platform="prefect",          # Workflow platform for submission
        expected_count=target_count,
        dry_run=True,  # set to False when you're ready to actually submit
    )
    
    return final_phrase

# Entry point for running the flow directly
if __name__ == "__main__":
    # Run the complete pipeline
    dp2()
