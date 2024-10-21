import boto3
import time
import logging
import sys
import re
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/481665097158/1222358839-req-queue'
MAX_INSTANCES = 20
MIN_INSTANCES = 0
MESSAGE_PER_INSTANCE = 0.5 # seconds required to process a message
COOLDOWN = timedelta(minutes=5)
INSTANCE_CREATION_TIME = None
MAIN_TIME_LOOP = 5 #seconds

sqs_client = boto3.client('sqs')
ec2_client = boto3.client('ec2')
ec2 = boto3.resource('ec2')

def get_app_tier_instances():
    filters = [{
        'Name': 'tag:type',  # Tag key
        'Values': ["app-tier"]  # Tag value
    }]
    response = ec2_client.describe_instances(Filters=filters)
    filtered_instances = {
        "all-instances": [],
        "running-instances": []
    }
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            filtered_instances["all-instances"].append(instance["InstanceId"])
            if instance['State']['Name'] == 'running':
                filtered_instances["running-instances"].append(instance["InstanceId"])
    filtered_instances["all-instances"].sort()
    filtered_instances["running-instances"].sort()
    return filtered_instances

def get_queue_message_count():
    response = sqs_client.get_queue_attributes(
        QueueUrl=sqs_queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])


def create_ec2_instance(instance_number):
    INSTANCE_CREATION_TIME = datetime.now()
    logging.info(f"Instance created at: {instance_creation_time}")
    instance = ec2.create_instances(
        ImageId='ami-0b72c0ab73a677cc6',
        MinCount=1,
        MaxCount=1,
        InstanceType='t2.micro',
        SecurityGroupIds=['sg-0f3a8cdf91ddad72a'],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [
                {'Key': 'Name', 'Value': f'app-tier-instance-{instance_number}'},
                {'Key': 'type', 'Value': 'app-tier'}
            ]
        }]
    )
    return instance[0].id

def terminate_ec2_instance(instance_id):
    try:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
    except Exception as e:
        logging.error("Error terminating instance %s: %s", instance_id, e)

def get_available_ids(instance_names):
    instance_numbers = []
    for name in instance_names:
        match = re.search(r'\d+', name)
        if match:
            instance_numbers.append(int(match.group()))
    avail = list(set(range(1, 21)) - set(instance_numbers))
    return avail

def scale_ec2_instances(instances, desired_instances):
    running_instances = len(instances["running-instances"])
    allocated_instances = len(instances["all-instances"])
    if running_instances == MAX_INSTANCES:
        logging.info("Max instances reached. Cannot scale up.")
    elif desired_instances > running_instances:
        instances_to_add = int(desired_instances-running_instances)
        logging.info(f"Need to add {instances_to_add} instances")
        for i in get_available_ids(instances["running-instances"])[:instances_to_add]:
            logging.info("Creating new instance #%d", i)
            create_ec2_instance(i)
    elif desired_instances < running_instances:
        if datetime.now() - INSTANCE_CREATION_TIME < COOLDOWN:
            logging.info("Cooldown period active. Cannot scale down.")
            return
        instances_to_remove = int(running_instances-desired_instances)
        logging.info(f"Need to terminate {instances_to_remove} instances")
        for i in instances["running-instances"][:instances_to_remove]:
            logging.info("Terminating instance %s", i)
            terminate_ec2_instance(i)

def get_desired_instances(message_count):
    return min(message_count//MESSAGE_PER_INSTANCE, MAX_INSTANCES)

def main():
    logging.info("\n" + "="*60 + "\n" + "      scaler is starting      " + "\n" + "="*60)
    while True:
        message_count = get_queue_message_count()
        logging.info(f"Current message count: {message_count}")
        desired_instances = get_desired_instances(message_count)
        logging.info(f"Desired Instances: {desired_instances}")
        # desired_instances=0
        instances = get_app_tier_instances()
        logging.info(f"Instances: {instances}")
        scale_ec2_instances(instances, desired_instances)
        time.sleep(MAIN_TIME_LOOP)

if __name__ == '__main__':
    main()
