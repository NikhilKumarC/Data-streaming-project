import argparse
import json
import random
import time
from datetime import datetime
from uuid import uuid4
import logging
import sys
sys.path.append('/Users/nikhilkumar/PycharmProjects/Streamingproject/Usergenerate')

from botocore.exceptions import ClientError
from Usergenerate import Secrets as secret
import Usergenerate.main as usgen
from Usergenerate import Configuration as conf

import boto3
from faker import Faker

fake = Faker()

channel=['goodle_adds','gmail','facebook','google search']
# Stream clicks and checkouts data

# Generate a random user agent string
def random_user_agent():
    return fake.user_agent()


# Generate a random IP address
def random_ip():
    return fake.ipv4()

def random_channel():
    return channel[fake.random_int(min=0, max=3)]

# Generate a click event with the current datetime_occured
def generate_click_event(user_id, product_id=None,channel=None):
    click_id = str(uuid4())
    if product_id is None:
        product_id = fake.random_int(min=1, max=100)
    product = fake.word()
    if channel is None:
        channel = random_channel()
    price = fake.pyfloat(left_digits=2, right_digits=2, positive=True)
    url = fake.uri()
    user_agent = random_user_agent()
    ip_address = random_ip()
    datetime_occured = datetime.now()

    click_event = {
        "click_id": click_id,
        "user_id": user_id,
        "product_id": product_id,
        "product": product,
        "channel":channel,
        "price": price,
        "url": url,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ],
    }

    return click_event


# Generate a checkout event with the current datetime_occured
def generate_checkout_event(user_id, product_id):
    payment_method = fake.credit_card_provider()
    total_amount = fake.pyfloat(left_digits=3, right_digits=2, positive=True)
    shipping_address = fake.address()
    billing_address = fake.address()
    user_agent = random_user_agent()
    ip_address = random_ip()
    datetime_occured = datetime.now()

    checkout_event = {
        "checkout_id": str(uuid4()),
        "user_id": user_id,
        "product_id": product_id,
        "payment_method": payment_method,
        "total_amount": total_amount,
        "shipping_address": shipping_address,
        "billing_address": billing_address,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ],
    }

    return checkout_event


# Function to push the events to a Kinesis stream
def push_to_kinesis(event, stream):
    secobj = usgen.get_secret()
    kin = boto3.client("kinesis",
                      region_name='us-east-1',
                      aws_access_key_id=secobj.getaccessKey(),
                      aws_secret_access_key=secobj.getsecretKey())
    try:
        if stream=="clicks":
            response = kin.put_record(Data=json.dumps(event).encode('utf-8'), StreamName=stream,
                                      PartitionKey=event['click_id'])
        else:
            response = kin.put_record(Data=json.dumps(event).encode('utf-8'),StreamName=stream,PartitionKey=event['checkout_id'])
        print(response['SequenceNumber'],response['ShardId'],stream)
    except ClientError as e:
        print(e.response['Error']['Message'])


def gen_clickstream_data(num_click_records: int) -> None:
    for _ in range(num_click_records):
        user_id = random.randint(1, 100)
        click_event = generate_click_event(user_id)
        push_to_kinesis(click_event, 'clicks')

        # simulate multiple clicks & checkouts 50% of the time
        while random.randint(1, 100) >= 50:
            click_event = generate_click_event(
                click_event["user_id"], click_event['product_id'], click_event['channel']
            )
            push_to_kinesis(click_event, 'clicks')

            checkout_event=generate_checkout_event(
                click_event["user_id"], click_event["product_id"]
            )
            push_to_kinesis(
                    checkout_event,
                    'checkouts',
            )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
       "-nc",
       "--num_click_records",
       type=int,
       help="Number of click records to generate",
       default=1000000,
    )
    args = parser.parse_args()
    while True:
        gen_clickstream_data(args.num_click_records)
        time.sleep(60)