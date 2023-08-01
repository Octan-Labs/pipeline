import boto3
from re import findall
from os import environ


class EntityType:
    BLOCK = 'block'
    TRANSACTION = 'transaction'
    LOG = 'log'
    TOKEN_TRANSFER = 'token_transfer'
    TRACE = 'trace'
    CONTRACT = 'contract'
    TOKEN = 'token'


def get_file_name(entity):
    block_numbers = []
    for obj in bucket.objects.filter(Prefix=f"{sub_path}{entity}s/date={date}/hour"):
        block_numbers.append(findall(r'\d+', obj.key.split("/")[-1]))
        obj.delete()

    start_block = min((min(block_number)
                      for block_number in block_numbers), default="")
    end_block = max((max(block_number)
                    for block_number in block_numbers), default="")
    return f"{entity}s_{start_block}_{end_block}.parquet"


def rename(entity):
    file_name = get_file_name(entity)

    for obj in bucket.objects.filter(Prefix=f"{sub_path}{entity}s/date={date}/repartition"):
        if ("/part" not in obj.key):
            obj.delete()
        else:
            s3.Object(bucket_name, f'{sub_path}{entity}s/date={date}/{file_name}').copy_from(CopySource=f'{bucket_name}/{obj.key}')
            s3.Object(bucket_name, obj.key).delete()

    print(f'{entity} on {date} merged into s3://{bucket_name}/{sub_path}{entity}s/date={date}/{file_name}')


def main():
    global s3
    global date
    global bucket
    global bucket_name
    global sub_path

    base_path = environ.get("BASE_PATH")
    date = environ.get("DATE")
    entities = environ.get("ENTITIES").split(",")
    aws_access_key_id = environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = environ.get("AWS_SECRET_ACCESS_KEY")
    path = base_path.split("/")[2:]
    bucket_name = path[0]
    sub_path = "/".join(path[1:]) + "/" if len(path) > 1 else ""
    
    # Create connection to S3
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Get bucket object
    bucket = s3.Bucket(bucket_name)

    for entity in entities:
        if EntityType.BLOCK == entity:
            rename(EntityType.BLOCK)

        if EntityType.TRANSACTION == entity:
            rename(EntityType.TRANSACTION)

        if EntityType.LOG == entity:
            rename(EntityType.LOG)

        if EntityType.TOKEN_TRANSFER == entity:
            rename(EntityType.TOKEN_TRANSFER)

        if EntityType.TRACE == entity:
            rename(EntityType.TRACE)

        if EntityType.CONTRACT == entity:
            rename(EntityType.CONTRACT)

        if EntityType.TOKEN == entity:
            rename(EntityType.TOKEN)

main()