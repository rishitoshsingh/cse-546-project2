import uuid
from flask import Flask, request
import boto3
from werkzeug.utils import secure_filename
import json
import time

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
            MaxNumberOfMessages=5,
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
        else:
            print("Waiting for response...")
        time.sleep(5)

def upload_to_s3(request_id, file):
    filename = secure_filename(file.filename)
    filename = request_id + "-" + filename
    print(filename)
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
    response = read_from_queue(request_id)
    print(response)
    # return read_from_queue(request_id)
    return response["result"]

if __name__ == '__main__':
    app.run(debug=True)