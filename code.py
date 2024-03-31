import boto3
import json
import pymysql
import sys
import time
import os


# Set up AWS credentials
session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name=''
)

# Configration start
#   #config rds
host_name = ''
database_user='adminanand'
database_password='anand123'
database_name='chat'
table_name = 'message'
#   #config sqs
queue_url = 'https://sqs.us-east-1.amazonaws.com/524028545999/ChatQueue'

# Configration end

logs = []
def print_status(status="",log=None):
    os.system("cls" if os.name == "nt" else "clear")
    if not (log == None):
        logs.append(log)
    sys.stdout.write("\r{0}{1}{2}{3}".format('\n'.join(logs),"\nCURRENT STATUS: ",status,"\n"))
    sys.stdout.flush()
    time.sleep(0.5)


def connect_to_database():
    return pymysql.connect(
        host = host_name,
        user=database_user,
        password=database_password,
        db=database_name,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def reconnect_rds():
    while True:
        rds = RDS()
        if rds.open == False:
            try:
                ls = [1,2,3,4]
                print_status(status="Trying to reconnect to RDS..")
                rds = connect_to_database()
                print_status(log="Connected to RDS.")
                break
            except:
                print_status(status="Failed to reconnect to RDS..")
                time.sleep(3)


print_status(log=f"Running {os.path.basename(__file__)}")

# Set up SQS and RDS clients
sqs = session.client('sqs')

global rds
class RDS:
    def __init__(self):
        self.open = False
rds = RDS()
try:
    rds = connect_to_database()
    print_status(status="Connected to RDS.")
except:
    print_status(log="Failed to connect to RDS.")
    reconnect_rds()

# Check if the message table exists
with rds.cursor() as cursor:
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor.fetchone()

if result:
    print_status(log=f'The \'{table_name}\' table exists.')
else:
    # Create the message table
    with rds.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE message (
              id SERIAL PRIMARY KEY,
              name VARCHAR(255) NOT NULL,
              message TEXT NOT NULL
          );
        """)
    rds.commit()
    print_status(log='The message table was created successfully.')

# Continuously check for messages in the queue
while True:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    if 'Messages' in response:
        for message in response['Messages']:
            # Extract the message body as a JSON string
            body = message['Body']
            
            # Convert the JSON string to a dictionary
            data = json.loads(body)
            # Insert the data into the RDS database
            with rds.cursor() as cursor:
                sql = "INSERT INTO message (name, message) VALUES (%s, %s)"
                try:
                    cursor.execute(sql, (data['Name'], data['Message']))
                    rds.commit()
                    print_status(log=f"Submitted to RDS. Name: {data['Name']}, Message: {data['Message']}")
                except pymysql.err.OperationalError as e:
                    print_status(status='Disconnected from RDS database.')
                    reconnect_rds()

            # Delete the message from the queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            print_status(log=f"Deleted from SQS. Name: {data['Name']}, Message: {data['Message']}")
    else:
        print_status(status='No messages in queue.')
