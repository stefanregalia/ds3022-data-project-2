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
	This tasks fetches SQS approximate counts and returns a structured dictionary
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

# Defining our flow order to execute our tasks
@flow
def dp2():
	qurl = scatter() # Running our scatter task
	return qurl 

# Running the script
if __name__ == "__main__":
	dp2()
