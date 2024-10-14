import boto3
import time
from model import face_recognition
import json

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

def download_from_s3(user_request):
    object_name = user_request["request_id"] + "-" + user_request['filename']
    save_file_path = "/tmp/" + object_name
    s3_client.download_file(REQ_S3, object_name, save_file_path)
    return save_file_path

def result_to_s3(user_request, result):
    object_name = user_request["filename"].split(".")[0]
    s3_client.put_object(Bucket=RES_S3, Key=object_name, Body=json.dumps(result))


def send_to_queue(user_request, result):
    response = {
        "request_id": user_request["request_id"],
        "filename": user_request["filename"],
        "result": result
    }
    response = sqs_client.send_message(
        QueueUrl=RES_SQS,
        MessageBody=json.dumps(response)
    )
    return response

def process_message(message):
    user_request = json.loads(message)
    save_file_path = download_from_s3(user_request)
    result = face_recognition.face_match(save_file_path, 'app-tier/data.pt')
    result = result[0]
    result_to_s3(user_request, result)
    send_to_queue(user_request, result)

def main():
    while True:
        response = sqs_client.receive_message(
            QueueUrl=REQ_SQS,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )

        messages = response.get('Messages', [])
        if messages:
            for message in messages:
                process_message(message['Body'])
                
                # Delete the message from the queue after processing
                sqs_client.delete_message(
                    QueueUrl=REQ_SQS,
                    ReceiptHandle=message['ReceiptHandle']
                )
        else:
            print("No messages to process. Waiting...")
        time.sleep(5)

if __name__ == "__main__":
    main()