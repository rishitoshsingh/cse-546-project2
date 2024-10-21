import uuid
from flask import Flask, request
import boto3
from werkzeug.utils import secure_filename
import json
import time
import asyncio
import aioboto3  # Async Boto3 client
from asgiref.wsgi import WsgiToAsgi

import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Use aioboto3 for async AWS operations
session = aioboto3.Session()

# S3 buckets
REQ_S3 = "1222358839-in-bucket"
RES_S3 = "1222358839-out-bucket"

# SQS queues
REQ_SQS = "https://sqs.us-east-1.amazonaws.com/481665097158/1222358839-req-queue"
RES_SQS = "https://sqs.us-east-1.amazonaws.com/481665097158/1222358839-resp-queue"

async def send_to_queue(user_request):
    async with session.client('sqs') as sqs_client:
        response = await sqs_client.send_message(
            QueueUrl=REQ_SQS,
            MessageBody=json.dumps(user_request)
        )
    return response

async def read_from_queue(request_id):
    async with session.client('sqs') as sqs_client:
        while True:
            response = await sqs_client.receive_message(
                QueueUrl=RES_SQS,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            messages = response.get('Messages', [])
            if messages:
                for message in messages:
                    res_message = json.loads(message['Body'])
                    if res_message['request_id'] == request_id:
                        await sqs_client.delete_message(
                            QueueUrl=RES_SQS,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        return res_message
            # Uncomment to add delay between retries if no messages
            await asyncio.sleep(2)

async def upload_to_s3(request_id, file):
    filename = secure_filename(file.filename)
    filename = request_id + "-" + filename
    async with session.client('s3') as s3_client:
        await s3_client.upload_fileobj(file, REQ_S3, filename)

@app.route('/', methods=['POST'])
async def root_post():
    logging.info("\n" + "="*60 + "\n" + "      Received Request      ")

    if 'inputFile' not in request.files:
        return "No file part in the request", 400

    input_file = request.files['inputFile']
    request_id = str(uuid.uuid4())
    await upload_to_s3(request_id, input_file)
    
    user_request = {
        "request_id": request_id,
        "filename": input_file.filename
    }
    await send_to_queue(user_request)
    
    logging.info("Request sent to queue: %s", user_request)
    response = await read_from_queue(request_id)
    logging.info("Sending response to user: %s", response)
    
    return response["filename"] + ":" + response["result"]

asgi_app = WsgiToAsgi(app)

if __name__ == '__main__':
    # Use Uvicorn to run the app as ASGI to support async
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, threaded=True)
    logging.info("\n" + "="*60 + "\n" + "      web-tier is starting      " + "\n" + "="*60)

