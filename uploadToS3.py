import logging
import boto3
import os
import sys
from botocore.exceptions import ClientError

ACCESS_KEY = str(sys.argv[1])
SECRET_KEY = str(sys.argv[2])
path = 'C:/Users/patri/Documents/SSolutions/loadFile/'
file = sorted(os.listdir(path))[::-1][0]
s3Folder = 'consumos'

def uploadToS3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

resp = uploadToS3(path+str(file),'consumos-ss',s3Folder+'/'+str(file))
print(resp)
