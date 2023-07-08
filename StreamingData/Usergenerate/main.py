#Author: Nikhil

import argparse
import csv

import boto3
from botocore.exceptions import ClientError
import json
import Configuration
import Secrets

from faker import Faker
fake = Faker()

# Generate user data
def addUser(num_user_records: int):
    # field names
    fields = ['Id','Name','Password']
    csvfile = "users.csv"
    file = open(csvfile,'w+')
    csvwriter = csv.writer(file)
    # writing the fields
    csvwriter.writerow(fields)
    for id in range(num_user_records):
        #print(id, fake.user_name(), fake.password())
        csvwriter.writerow([id, fake.user_name(), fake.password()])
    copytos3("users.csv",Configuration.targetbucket)

# Generate user data
def addProducts(num_of_products: int):
    # field names
    fields = ['id', 'Name', 'Description', 'Price']
    csvfile = "products.csv"
    file = open(csvfile,'w+')
    csvwriter = csv.writer(file)
    # writing the fields
    csvwriter.writerow(fields)
    for id in range(num_of_products):
        csvwriter.writerow([id, fake.name(), fake.text(), fake.pyfloat(left_digits=3, right_digits=2, positive=True)])
    copytos3("products.csv", Configuration.targetbucket)

#secret manager for accessing aws credential
def get_secret():
    secret_name = Configuration.secretname
    region_name = Configuration.region

    s=Secrets.SecretsManage()
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response['SecretString']
        resp = json.loads(secret)
        s = Secrets.SecretsManage(resp["s3accesskey"], resp["s3secret"])
    except ClientError as e:
        print(e.response['Error']['Message'])
    return s

def copytos3(filename,bucket):
    secobj=get_secret()
    s3 = boto3.client("s3",
                      region_name='us-east-1',
                      aws_access_key_id=secobj.getaccessKey(),
                      aws_secret_access_key=secobj.getsecretKey())
    try:
        s3.upload_file(Filename=filename,
                   Bucket=bucket,
                   Key=filename)
    except ClientError as e:
        print(e.response['Error']['Message'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_user_records",
        type=int,
        help="Number of user records to generate",
        default=300,
    )

    parser.add_argument(
        "--num_of_products",
        type=int,
        help="Number of user records to generate",
        default=100,
    )
    parser.add_argument(
        "--config_file",
        type=str,
        help="config file path",
        default="./config.toml",
    )

    args = parser.parse_args()
    addUser(args.num_user_records)
    print("***************************")
    addProducts(args.num_user_records)