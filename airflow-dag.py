# airflow_dag.py

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
    'retries': 2,
    'retry_delay': timedelta(seconds=3),
}

# API endpoint
API_URL = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xtm9px"

def sqs_client(region: str = "us-east-1"):
    """Create SQS client"""
    return boto3.client("sqs", region_name=region)

def scatter_task(**context):
    """Task to call scatter API and populate queue"""
    print(f"Calling the scatter API endpoint for xtm9px")
    
    r = requests.post(API_URL, timeout=20)
    r.raise_for_status()
    
    payload = r.json()
    queue_url = payload["sqs_url"]
    
    print(f"Queue URL received: {queue_url}")
    
    # Push queue URL to XCom for other tasks
    context['task_instance'].xcom_push(key='queue_url', value=queue_url)
    
    return queue_url

def get_queue_info_task(**context):
    """Get queue statistics"""
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
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
        
        stats = {
            "visible": visible,
            "not_visible": not_visible,
            "delayed": delayed,
            "total": total,
        }
        
        print(f"Queue counts | visible={visible} not_visible={not_visible} delayed={delayed} total={total}")
        
        return stats
        
    except (BotoCoreError, ClientError) as e:
        print(f"Failed to get queue attributes: {e}")
        return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}

def receive_and_parse_task(**context):
    """Receive and parse messages from queue"""
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
    sqs = sqs_client()
    
    try:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )
    except (BotoCoreError, ClientError) as e:
        print(f"SQS receive_message failed: {e}")
        return []
    
    messages = resp.get("Messages", [])
    if not messages:
        print("No messages available on this poll")
        return []
    
    print(f"📨 Received {len(messages)} message(s) from SQS. Beginning parse.")
    
    parsed = []
    for m in messages:
        msg_id = m.get("MessageId", "UNKNOWN")
        
        try:
            attrs = m.get("MessageAttributes")
            print(f"📋 Message {msg_id[:8]}... attrs type: {type(attrs)}")
            
            if attrs is None:
                print(f"⚠️ Message {msg_id} has MessageAttributes = None")
                continue
            
            if not isinstance(attrs, dict):
                print(f"⚠️ Message {msg_id} has MessageAttributes of wrong type: {type(attrs)}")
                continue
            
            rh = m.get("ReceiptHandle")
            order_attr = attrs.get("order_no")
            word_attr = attrs.get("word")
            
            print(f"📋 Message {msg_id[:8]}... order_attr type: {type(order_attr)}, word_attr type: {type(word_attr)}")
            
            if order_attr is None:
                print(f"⚠️ Message {msg_id} missing 'order_no' attribute. Available: {list(attrs.keys())}")
                continue
            
            if word_attr is None:
                print(f"⚠️ Message {msg_id} missing 'word' attribute. Available: {list(attrs.keys())}")
                continue
            
            # Extract StringValue safely
            if isinstance(order_attr, dict):
                raw_order = order_attr.get("StringValue")
            elif isinstance(order_attr, str):
                print(f"⚠️ Message {msg_id} has order_no as plain string: '{order_attr}' (expected dict)")
                raw_order = order_attr
            else:
                print(f"⚠️ Message {msg_id} has order_no of unexpected type: {type(order_attr)}")
                continue
            
            if isinstance(word_attr, dict):
                word = word_attr.get("StringValue")
            elif isinstance(word_attr, str):
                print(f"⚠️ Message {msg_id} has word as plain string: '{word_attr}' (expected dict)")
                word = word_attr
            else:
                print(f"⚠️ Message {msg_id} has word of unexpected type: {type(word_attr)}")
                continue
            
            if raw_order is None or word is None:
                print(f"⚠️ Message {msg_id} missing StringValue. order_no={order_attr}, word={word_attr}")
                continue
            
            try:
                order_no = int(raw_order)
            except (ValueError, TypeError) as e:
                print(f"⚠️ Message {msg_id} has non-integer order_no='{raw_order}': {e}")
                continue
            
            if not rh:
                print(f"⚠️ Message {msg_id} missing ReceiptHandle")
                continue
            
            parsed.append({
                "message_id": msg_id,
                "order_no": order_no,
                "word": word,
                "receipt_handle": rh,
            })
            print(f"✅ Parsed message {msg_id[:8]}... order_no={order_no}, word='{word}'")
            
        except Exception as e:
            print(f"🔥 UNEXPECTED ERROR parsing message {msg_id}: {type(e).__name__}: {e}")
            print(f"Message structure: {m}")
            continue
    
    print(f"✅ Parsed {len(parsed)} valid fragment(s) from {len(messages)} received.")
    return parsed

def persist_fragments_task(fragments: List[Dict], path: str = "runs/airflow_fragments.jsonl"):
    """Persist fragments to JSONL file"""
    if not fragments:
        print("No new fragments to persist this round.")
        return path
    
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    
    safe_batch = [
        {"message_id": f["message_id"], "order_no": f["order_no"], "word": f["word"]}
        for f in fragments
    ]
    
    try:
        with p.open("a", encoding="utf-8") as fp:
            for row in safe_batch:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
            fp.flush()
            os.fsync(fp.fileno())
        print(f"Persisted {len(safe_batch)} fragment(s) to {p}.")
    except OSError as e:
        print(f"Failed to persist fragments to {p}: {e}")
    
    return str(p)

def delete_messages_task(receipt_handles: List[str], **context):
    """Delete messages from queue"""
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
    sqs = sqs_client()
    
    if not receipt_handles:
        print("No messages to delete this round.")
        return {"deleted": 0, "failed": 0}
    
    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    
    total_deleted = 0
    total_failed = 0
    
    for batch in _chunks(receipt_handles, 10):
        entries = [{"Id": str(i), "ReceiptHandle": rh} for i, rh in enumerate(batch)]
        try:
            resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        except (BotoCoreError, ClientError) as e:
            print(f"delete_message_batch failed: {e}")
            total_failed += len(entries)
            continue
        
        deleted = len(resp.get("Successful", []))
        failed = len(resp.get("Failed", []))
        total_deleted += deleted
        total_failed += failed
        
        if failed:
            failed_ids = [f.get("Id") for f in resp.get("Failed", [])]
            print(f"Batch delete partial failure. Failed Ids: {failed_ids}")
    
    print(f"Deleted {total_deleted} message(s); {total_failed} failed to delete.")
    return {"deleted": total_deleted, "failed": total_failed}

def collection_loop_task(**context):
    """Main collection loop"""
    queue_url = context['task_instance'].xcom_pull(key='queue_url')
    
    by_order = {}
    seen_ids = set()
    target_count = 21
    CONSECUTIVE_EMPTY_LIMIT = 5
    consecutive_empty = 0
    
    print(f"Starting collection loop; aiming for {target_count} unique fragments.")
    
    while len(by_order) < target_count:
        # Get queue stats
        stats = get_queue_info_task(**context)
        print(f"Queue snapshot before poll: {stats}")
        
        # Receive and parse messages
        batch = receive_and_parse_task(**context)
        
        if not batch:
            if stats["visible"] == 0 and stats["not_visible"] == 0 and stats["delayed"] == 0:
                consecutive_empty += 1
                print(f"Empty-queue snapshot {consecutive_empty}/{CONSECUTIVE_EMPTY_LIMIT}.")
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
                consecutive_empty = 0
            continue
        
        consecutive_empty = 0
        
        # Dedup by MessageId
        new_msgs = [m for m in batch if m["message_id"] not in seen_ids]
        for m in new_msgs:
            seen_ids.add(m["message_id"])
        
        # Persist
        persist_fragments_task(new_msgs)
        
        # Keep first per order_no
        added = 0
        for m in new_msgs:
            o = m["order_no"]
            if o not in by_order:
                by_order[o] = m
                added += 1
        
        # Delete
        delete_messages_task([m["receipt_handle"] for m in batch], **context)
        
        if added:
            print(f"Collected {len(by_order)} unique / {target_count} required. (+{added} this round)")
    
    print("Collected all required fragments!")
    
    # Store fragments in XCom
    context['task_instance'].xcom_push(key='fragments', value=list(by_order.values()))
    
    return list(by_order.values())

def assemble_and_submit_task(**context):
    """Assemble phrase and submit"""
    fragments = context['task_instance'].xcom_pull(key='fragments')
    
    if not fragments:
        raise RuntimeError("No fragments provided to assemble.")
    
    order_nos = [int(f["order_no"]) for f in fragments]
    words = [f["word"] for f in fragments]
    
    # Validation
    dupes = [n for n in set(order_nos) if order_nos.count(n) > 1]
    if dupes:
        raise ValueError(f"Duplicate order_no detected: {sorted(dupes)}")
    
    mn = min(order_nos)
    required = set(range(mn, mn + 21))
    missing = sorted(required - set(order_nos))
    
    if missing:
        raise ValueError(f"Missing order numbers: {missing}")
    
    # Assemble
    ordered = sorted(fragments, key=lambda f: int(f["order_no"]))
    phrase = " ".join(f["word"] for f in ordered).strip()
    
    print(f"ASSEMBLED PHRASE ({len(ordered)} words): {phrase}")
    
    # Save to file
    with open("airflow_assembled_phrase.txt", "w") as fp:
        fp.write(phrase + "\n")
    print(f"Saved preview -> airflow_assembled_phrase.txt")
    
    # Submit (set dry_run=False when ready)
    dry_run = True  # Change to False to actually submit
    
    if dry_run:
        print("Dry run enabled: skipping submission to dp2-submit.")
        return phrase
    
    sqs = sqs_client()
    resp = sqs.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
        MessageBody=phrase,
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": "xtm9px"},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "airflow"},
        },
    )
    
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 200:
        raise RuntimeError(f"Submission failed (status {status}): {resp}")
    
    print("✅ Submission accepted (HTTP 200).")
    return phrase

# Define the DAG
with DAG(
    'dp2_quote_assembler',
    default_args=default_args,
    description='DP2 Quote Assembler - Airflow Version',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['dp2', 'sqs', 'quote-assembler'],
) as dag:
    
    # Task 1: Call scatter API
    scatter = PythonOperator(
        task_id='scatter_api',
        python_callable=scatter_task,
        provide_context=True,
    )
    
    # Task 2: Collection loop
    collect = PythonOperator(
        task_id='collection_loop',
        python_callable=collection_loop_task,
        provide_context=True,
    )
    
    # Task 3: Assemble and submit
    assemble = PythonOperator(
        task_id='assemble_and_submit',
        python_callable=assemble_and_submit_task,
        provide_context=True,
    )
    
    # Define task dependencies
    scatter >> collect >> assemble
