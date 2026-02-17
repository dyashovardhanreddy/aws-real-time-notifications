import json

def lamda_handler(event, context):
	for record in event.get("Records", []):

		message_body = json.loads(record["body"])

		print(f"Would POS to webhook: {message_body}")

	return {"status": "sent"}