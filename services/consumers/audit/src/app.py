import json
import os
import boto3
from botocore.exceptions import ClientError

TABLE_NAME = os.environ.get("TABLE_NAME", "rtm-events-table")

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def put_if_absent(item: dict) -> bool:
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(eventId)",
        )
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return False
        raise


def lambda_handler(event, context):
    created_count = 0
    duplicate_count = 0

    for record in event.get("Records", []):
        body_str = record.get("body", "{}")

        try:
            msg = json.loads(body_str)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON body: {body_str}")

        event_id = msg.get("eventId")
        event_type = msg.get("eventType")
        timestamp = msg.get("timestamp")

        if not event_id or not event_type or not timestamp:
            raise ValueError(f"Missing required fields in message: {msg}")

        item = {
            "eventId": event_id,
            "eventType": event_type,
            "timestamp": timestamp,
            "payload": msg.get("payload", {}),
        }

        if put_if_absent(item):
            created_count += 1
        else:
            duplicate_count += 1

    
    return {
        "statusCode": 200,
        "body": json.dumps(
            {"created": created_count, "duplicates": duplicate_count}
        ),
    }