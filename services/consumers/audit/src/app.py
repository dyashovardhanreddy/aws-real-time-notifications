import json

def lambda_handler(event, context):

	for record in event.get("records", []):
		message_body = json.loads(record["body"])
		print("AUDIT LOG EVENT:", json.dumps(message_body, indent=2))

	return {"status": "processed"}