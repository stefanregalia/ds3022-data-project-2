# airflow-dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pathlib import Path
import json
import os
import time
from typing import List, Dict

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Retry failed tasks up to 2 times
    'retry_delay': timedelta(seconds=3),  # Wait 3 seconds between retries
}

# API endpoint with UVA computing ID
API_URL = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xtm9px"

def sqs_client(region: str = "us-east-1"):
    """Create and return an SQS client for the specified region"""
    return boto3.client("sqs", region_name=region)

def scatter_task(**context):
    """
    Task to call scatter API and populate SQS queue with 21 messages.
    Pushes queue URL to XCom for downstream tasks to access.
    """
    print(f"Calling the scatter API endpoint for xtm9px")
    
    # Make POST request to scatter API
    r = requests.post(API_URL, timeout=20)
    r.raise_for_status()  # Raise error if request failed
    
    # Extract queue URL from response
    payload = r.json()
    queue_url = payload["sqs_url"]
    
    print(f"Queue URL received: {queue_url}")
    
    # Store queue URL in XCom so other tasks can access it
    context['task_instance'].xcom_push(key='queue_url', value=queue_url)
    
    return queue_url

def get_queue_info_task(**context):
    """
    Get current queue statistics including visible, in-flight, and delayed message counts.
    Used for monitoring queue state during collection.
    """
    # Retrieve queue URL from XCom (set by scatter_task)
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
    sqs = sqs_client()
    
    try:
        # Request queue attributes from SQS
        resp = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",  # Visible messages
                "ApproximateNumberOfMessagesNotVisible",  # In-flight messages
                "ApproximateNumberOfMessagesDelayed",  # Delayed messages
            ],
        )
        
        # Extract attributes from response
        attrs = resp.get("Attributes", {})
        
        # Parse counts as integers
        visible = int(attrs.get("ApproximateNumberOfMessages", 0))
        not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
        total = visible + not_visible + delayed
        
        # Package stats into dictionary
        stats = {
            "visible": visible,
            "not_visible": not_visible,
            "delayed": delayed,
            "total": total,
        }
        
        print(f"Queue counts | visible={visible} not_visible={not_visible} delayed={delayed} total={total}")
        
        return stats
        
    except (BotoCoreError, ClientError) as e:
        # If SQS call fails, log error and return zeros
        print(f"Failed to get queue attributes: {e}")
        return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}

def receive_and_parse_task(**context):
    """
    Poll SQS queue for messages and parse MessageAttributes to extract order_no and word.
    Returns list of parsed fragments with receipt handles for deletion.
    """
    # Get queue URL from XCom
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
    sqs = sqs_client()
    
    # Attempt to receive messages from queue
    try:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # Fetch up to 10 messages per poll
            WaitTimeSeconds=10,  # Long-polling: wait up to 10 seconds for messages
            MessageAttributeNames=["All"],  # Retrieve all message attributes
        )
    except (BotoCoreError, ClientError) as e:
        # If receive fails, log and return empty list
        print(f"SQS receive_message failed: {e}")
        return []
    
    # Extract messages array from response
    messages = resp.get("Messages", [])
    if not messages:
        print("No messages available on this poll")
        return []
    
    print(f"Received {len(messages)} message(s) from SQS. Beginning parse.")
    
    # Initialize list for successfully parsed fragments
    parsed = []
    
    # Process each message in the batch
    for m in messages:
        # Extract MessageId for logging
        msg_id = m.get("MessageId", "UNKNOWN")
        
        try:
            # Get MessageAttributes dict
            attrs = m.get("MessageAttributes")
            print(f"Message {msg_id[:8]}... attrs type: {type(attrs)}")
            
            # Validate MessageAttributes exists
            if attrs is None:
                print(f"Message {msg_id} has MessageAttributes = None")
                continue
            
            # Validate MessageAttributes is a dict
            if not isinstance(attrs, dict):
                print(f"Message {msg_id} has MessageAttributes of wrong type: {type(attrs)}")
                continue
            
            # Extract ReceiptHandle (needed for deletion)
            rh = m.get("ReceiptHandle")
            
            # Extract order_no and word attributes
            order_attr = attrs.get("order_no")
            word_attr = attrs.get("word")
            
            print(f"Message {msg_id[:8]}... order_attr type: {type(order_attr)}, word_attr type: {type(word_attr)}")
            
            # Check if order_no attribute is present
            if order_attr is None:
                print(f"Message {msg_id} missing 'order_no' attribute. Available: {list(attrs.keys())}")
                continue
            
            # Check if word attribute is present
            if word_attr is None:
                print(f"Message {msg_id} missing 'word' attribute. Available: {list(attrs.keys())}")
                continue
            
            # Extract StringValue from order_no (handle different formats)
            if isinstance(order_attr, dict):
                raw_order = order_attr.get("StringValue")
            elif isinstance(order_attr, str):
                print(f"Message {msg_id} has order_no as plain string: '{order_attr}' (expected dict)")
                raw_order = order_attr
            else:
                print(f"Message {msg_id} has order_no of unexpected type: {type(order_attr)}")
                continue
            
            # Extract StringValue from word (handle different formats)
            if isinstance(word_attr, dict):
                word = word_attr.get("StringValue")
            elif isinstance(word_attr, str):
                print(f"Message {msg_id} has word as plain string: '{word_attr}' (expected dict)")
                word = word_attr
            else:
                print(f"Message {msg_id} has word of unexpected type: {type(word_attr)}")
                continue
            
            # Validate both StringValues were extracted successfully
            if raw_order is None or word is None:
                print(f"Message {msg_id} missing StringValue. order_no={order_attr}, word={word_attr}")
                continue
            
            # Convert order_no from string to integer (as required by rubric)
            try:
                order_no = int(raw_order)
            except (ValueError, TypeError) as e:
                print(f"Message {msg_id} has non-integer order_no='{raw_order}': {e}")
                continue
            
            # Validate ReceiptHandle is present
            if not rh:
                print(f"Message {msg_id} missing ReceiptHandle")
                continue
            
            # Add successfully parsed fragment to results
            parsed.append({
                "message_id": msg_id,
                "order_no": order_no,
                "word": word,
                "receipt_handle": rh,
            })
            print(f"Parsed message {msg_id[:8]}... order_no={order_no}, word='{word}'")
            
        except Exception as e:
            # Catch any unexpected errors during parsing
            print(f"UNEXPECTED ERROR parsing message {msg_id}: {type(e).__name__}: {e}")
            print(f"Message structure: {m}")
            continue
    
    print(f"Parsed {len(parsed)} valid fragment(s) from {len(messages)} received.")
    return parsed

def persist_fragments_task(fragments: List[Dict], path: str = "runs/airflow_fragments.jsonl"):
    """
    Persist fragments to local JSONL file before deleting from queue.
    Ensures data durability in case process fails during deletion.
    """
    if not fragments:
        print("No new fragments to persist this round.")
        return path
    
    # Create Path object and ensure parent directory exists
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract only essential fields for storage
    safe_batch = [
        {"message_id": f["message_id"], "order_no": f["order_no"], "word": f["word"]}
        for f in fragments
    ]
    
    try:
        # Append fragments to JSONL file (one JSON object per line)
        with p.open("a", encoding="utf-8") as fp:
            for row in safe_batch:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
            fp.flush()
            os.fsync(fp.fileno())  # Force write to disk
        print(f"Persisted {len(safe_batch)} fragment(s) to {p}.")
    except OSError as e:
        # Don't fail entire task if persistence fails, just log warning
        print(f"Failed to persist fragments to {p}: {e}")
    
    return str(p)

def delete_messages_task(receipt_handles: List[str], **context):
    """
    Delete messages from SQS queue in batches of 10 (SQS API limit).
    Tracks successful and failed deletions.
    """
    # Get queue URL from XCom
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
    sqs = sqs_client()
    
    # If no receipt handles provided, nothing to delete
    if not receipt_handles:
        print("No messages to delete this round.")
        return {"deleted": 0, "failed": 0}
    
    # Helper function to split list into chunks of size n
    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    
    # Initialize counters for tracking results
    total_deleted = 0
    total_failed = 0
    
    # Process receipt handles in batches of 10
    for batch in _chunks(receipt_handles, 10):
        # Format entries for SQS batch delete API
        entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(batch)]
        
        try:
            # Attempt to delete this batch
            resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        except (BotoCoreError, ClientError) as e:
            # If entire batch fails, log and count all as failed
            print(f"delete_message_batch failed: {e}")
            total_failed += len(entries)
            continue
        
        # Count successful and failed deletions from response
        deleted = len(resp.get("Successful", []))
        failed = len(resp.get("Failed", []))
        total_deleted += deleted
        total_failed += failed
        
        # Log any partial failures within the batch
        if failed:
            failed_ids = [f.get("Id") for f in resp.get("Failed", [])]
            print(f"Batch delete partial failure. Failed Ids: {failed_ids}")
    
    print(f"Deleted {total_deleted} message(s); {total_failed} failed to delete.")
    return {"deleted": total_deleted, "failed": total_failed}

def collection_loop_task(**context):
    """
    Main collection loop that orchestrates queue monitoring, message collection,
    persistence, and deletion until all 21 fragments are collected.
    """
    # Get queue URL from XCom
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
    
    # Initialize collection state
    by_order = {}  # Store unique fragments by order_no
    seen_ids = set()  # Track processed message IDs
    target_count = 21  # Need 21 total fragments
    CONSECUTIVE_EMPTY_LIMIT = 5  # Stall detection threshold
    consecutive_empty = 0
    
    print(f"Starting collection loop; aiming for {target_count} unique fragments.")
    
    # Continue until we have all required fragments
    while len(by_order) < target_count:
        # Get current queue statistics
        stats = get_queue_info_task(**context)
        print(f"Queue snapshot before poll: {stats}")
        
        # Poll for messages
        batch = receive_and_parse_task(**context)
        
        # Handle case where no messages received
        if not batch:
            # Check if queue is completely empty
            if stats["visible"] == 0 and stats["not_visible"] == 0 and stats["delayed"] == 0:
                consecutive_empty += 1
                print(f"Empty-queue snapshot {consecutive_empty}/{CONSECUTIVE_EMPTY_LIMIT}.")
                
                # If queue empty too long, raise error with diagnostic info
                if consecutive_empty >= CONSECUTIVE_EMPTY_LIMIT:
                    have = sorted(m["order_no"] for m in by_order.values())
                    if have:
                        mn = min(have)
                        required = set(range(mn, mn + target_count))
                        missing = sorted(required - set(have))
                    else:
                        missing = list(range(1, target_count + 1))
                    raise RuntimeError(
                        f"Stall detected: queue empty for {CONSECUTIVE_EMPTY_LIMIT} consecutive polls, "
                        f"but only collected {len(by_order)}/{target_count}. Missing order numbers: {missing}"
                    )
            else:
                # Queue has messages but they're delayed or in-flight, reset counter
                consecutive_empty = 0
            continue
        
        # Received messages, reset stall counter
        consecutive_empty = 0
        
        # Filter out any messages we've already seen (defense against redelivery)
        new_msgs = [m for m in batch if m["message_id"] not in seen_ids]
        for m in new_msgs:
            seen_ids.add(m["message_id"])
        
        # Persist fragments to file before deletion
        # persist_fragments_task(new_msgs)
        
        # Add new fragments to collection, keeping only first occurrence of each order_no
        added = 0
        for m in new_msgs:
            o = m["order_no"]
            if o not in by_order:
                by_order[o] = m
                added += 1
        
        # Delete all messages from this batch to prevent reprocessing
        delete_messages_task([m["receipt_handle"] for m in batch], **context)
        
        # Log progress if we added new fragments
        if added:
            print(f"Collected {len(by_order)} unique / {target_count} required. (+{added} this round)")
    
    print("Collected all required fragments!")
    
    # Store collected fragments in XCom for downstream task
    context['task_instance'].xcom_push(key='fragments', value=list(by_order.values()))
    
    return list(by_order.values())

def assemble_and_submit_task(**context):
    """
    Validate collected fragments, assemble into complete phrase,
    and submit to instructor SQS queue with platform="airflow".
    """
    # Retrieve fragments from XCom (set by collection_loop_task)
    fragments = context['task_instance'].xcom_pull(key='fragments')
    
    if not fragments:
        raise RuntimeError("No fragments provided to assemble.")
    
    # Extract order numbers and words for validation
    order_nos = [int(f["order_no"]) for f in fragments]
    words = [f["word"] for f in fragments]
    
    # Check for duplicate order numbers
    dupes = [n for n in set(order_nos) if order_nos.count(n) > 1]
    if dupes:
        raise ValueError(f"Duplicate order_no detected: {sorted(dupes)}")
    
    # Validate we have a complete contiguous sequence
    mn = min(order_nos)
    required = set(range(mn, mn + 21))
    missing = sorted(required - set(order_nos))
    
    if missing:
        raise ValueError(f"Missing order numbers: {missing}")
    
    # Sort fragments by order_no and join words into phrase
    ordered = sorted(fragments, key=lambda f: int(f["order_no"]))
    phrase = " ".join(f["word"] for f in ordered).strip()
    
    print(f"ASSEMBLED PHRASE ({len(ordered)} words): {phrase}")
    
    # Save phrase to local file for preview
    with open("airflow_assembled_phrase.txt", "w") as fp:
        fp.write(phrase + "\n")
    print(f"Saved preview -> airflow_assembled_phrase.txt")
    
    dry_run = False
    
    if dry_run:
        print("Dry run enabled: skipping submission to dp2-submit.")
        return phrase
    
    # Create SQS client and submit phrase
    sqs = sqs_client()
    resp = sqs.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
        MessageBody=phrase,
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": "xtm9px"},  # UVA computing ID
            "phrase": {"DataType": "String", "StringValue": phrase},  # Complete phrase
            "platform": {"DataType": "String", "StringValue": "airflow"},  # Workflow platform
        },
    )
    
    # Verify submission was successful
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 200:
        raise RuntimeError(f"Submission failed (status {status}): {resp}")
    
    print("Submission accepted (HTTP 200).")
    return phrase

# Define the DAG
with DAG(
    'dp2_quote_assembler',  # DAG ID
    default_args=default_args,
    description='DP2 Quote Assembler - Airflow Version',
    schedule_interval=None,  # Manual trigger only (not scheduled)
    catchup=False,  # Don't backfill past runs
    tags=['dp2', 'sqs', 'quote-assembler'],
) as dag:
    
    # Call scatter API to populate queue
    scatter = PythonOperator(
        task_id='scatter_api',
        python_callable=scatter_task,
        provide_context=True,  # Provide context for XCom access
    )
    
    # Collection loop to gather all fragments
    collect = PythonOperator(
        task_id='collection_loop',
        python_callable=collection_loop_task,
        provide_context=True,
    )
    
    # Assemble phrase and submit to the queue
    assemble = PythonOperator(
        task_id='assemble_and_submit',
        python_callable=assemble_and_submit_task,
        provide_context=True,
    )
    
    # Define task dependencies: scatter -> collect -> assemble
    scatter >> collect >> assemble
