import json
import uuid
from datetime import datetime

def lambda_handler(event, comtext):

	try:
		body = json.loads(event.get("body", "{}"))


		if "eventType" not in body or "payload" not in body:
			return {
				"statusCode": 400,
				"body": json.dumps({"error": "eventType and payload required"})
			}

		event_id = str(uuid.uuid4())


		response = {

			"eventId": event_id,
			"receivedAt": datetime.utcnow().isoformat(),
			"message": "Event accepted (not yet published tp SNS)"
		}

		print("Received event:", body)


		return {
			"statusCode": 200,
			"body": json.dumps(response)
		}

	except:
		return {
			"statusCode": 500,
			"body": json.dumps({"error": str(e)})
		}