import uuid
from flask import Flask, request
import boto3
from werkzeug.utils import secure_filename
import json
import time

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
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# S3 buckets
REQ_S3 = "1222358839-in-bucket"
RES_S3 = "1222358839-out-bucket"

# SQS queues
REQ_SQS = "https://sqs.us-east-1.amazonaws.com/481665097158/1222358839-req-queue"
RES_SQS = "https://sqs.us-east-1.amazonaws.com/481665097158/1222358839-resp-queue"

def send_to_queue(user_request):
    response = sqs_client.send_message(
        QueueUrl=REQ_SQS,
        MessageBody=json.dumps(user_request)
    )
    return response

def read_from_queue(request_id):
    while True:
        response = sqs_client.receive_message(
            QueueUrl=RES_SQS,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        messages = response.get('Messages', [])
        if messages:
            for message in messages:
                res_message = json.loads(message['Body'])
                if res_message['request_id'] == request_id:
                    sqs_client.delete_message(
                        QueueUrl=RES_SQS,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    return res_message
        # else:
        #     logging.info("Waiting for response...")
        # time.sleep(5)

def cleanup(request_id, file):
    filename = secure_filename(file.filename)
    object_name = file.filename.split(".")[0]
    filename = request_id + "-" + filename
    s3_client.delete_object(Bucket=REQ_S3, Key=filename)
    logging.info(object_name)
    s3_client.delete_object(Bucket=RES_S3, Key=object_name)

def upload_to_s3(request_id, file):
    filename = secure_filename(file.filename)
    filename = request_id + "-" + filename
    s3_client.upload_fileobj(file, REQ_S3, filename)


@app.route('/', methods=['POST'])
def root_post():
    if 'inputFile' not in request.files:
        return "No file part in the request", 400

    input_file = request.files['inputFile']
    request_id = str(uuid.uuid4())
    upload_to_s3(request_id, input_file)
    user_request = {
        "request_id": request_id,
        "filename": input_file.filename
    }
    send_to_queue(user_request)
    logging.info("request sent to queue: %s", user_request)
    response = read_from_queue(request_id)
    logging.info("sending response to user: %s", response)
    cleanup(request_id, input_file)
    return response["filename"]+":"+response["result"]

# asgi_app = WsgiToAsgi(app)
if __name__ == '__main__':
    logging.info("\n" + "="*60 + "\n" + "      web-tier is starting      " + "\n" + "="*60)
    # uvicorn.run(asgi_app, host="0.0.0.0", port=8000)
    app.run(threaded=True)