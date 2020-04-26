import boto3
import click

def get_all_s3_keys(bucket, source_folder, repaired_tag):
    s3 = boto3.client('s3')
    keys=[]
    kwargs = {'Bucket': bucket,
              'Prefix': source_folder}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            tags = s3.get_object_tagging(
              Bucket=bucket,
              Key=key
            )['TagSet']
            if not any(x.Key == repaired_tag for x in tags):
              keys.extend(key)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


@click.command()
@click.option("--source-folder", help="Source folder in S3", required=True)
@click.option("--s3-bucket", help="Source S3 bucket", required=True)
@click.option("--repaired-tag", help="Name of the tag to flag object repaired", required=True)
def main(source_folder, s3_bucket, repaired_tag):
    keys = get_all_s3_keys(s3_bucket, source_folder, repaired_tag)
    print(keys)

if __name__ == '__main__':
    main()
