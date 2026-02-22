import json
import os
import uuid
from datetime import datetime, timezone

import boto3

sns = boto3.client("sns")

TOPIC_ARN = os.environ.get("TOPIC_ARN", "")


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    try:
        if not TOPIC_ARN:
            return _response(500, {"error": "TOPIC_ARN env var not set"})

        raw_body = event.get("body") or "{}"
        body = json.loads(raw_body)

        event_type = body.get("eventType")
        payload = body.get("payload")

        if not event_type or payload is None:
            return _response(400, {"error": "eventType and payload are required"})

        event_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        message = {
            "eventId": event_id,
            "eventType": event_type,
            "timestamp": now,
            "payload": payload,
        }

        sns.publish(
            TopicArn=TOPIC_ARN,
            Message=json.dumps(message),
            MessageAttributes={
                "eventType": {"DataType": "String", "StringValue": event_type},
                "eventId": {"DataType": "String", "StringValue": event_id},
            },
        )

        return _response(200, {"status": "published", "eventId": event_id})

    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON in request body"})
    except Exception as e:
        print("ERROR:", str(e))
        return _response(500, {"error": "Internal error"})