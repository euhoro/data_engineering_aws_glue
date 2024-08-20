import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
from botocore.exceptions import NoCredentialsError

def count_rows_in_json_pandas(directory):
    total_rows = 0
    for sub in os.listdir(directory):
        sub_dir = os.path.join(directory, sub)
        for filename in os.listdir(sub_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(sub_dir, filename)
                try:
                    with open(file_path, 'r') as file:
                        for line in file:
                            try:
                                json.loads(line)
                                total_rows += 1
                            except json.JSONDecodeError:
                                continue  # Skip lines that are not valid JSON
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    return total_rows

def count_rows_in_json_spark(directory):
    spark = SparkSession.builder.appName("CountRows").getOrCreate()
    total_rows = 0
    for sub in os.listdir(directory):
        sub_dir = os.path.join(directory, sub)
        for filename in os.listdir(sub_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(sub_dir, filename)
                try:
                    df = spark.read.json(file_path)
                    total_rows += df.count()
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    return total_rows

def count_rows_in_json_s3_pandas(bucket_name, prefix):
    s3 = boto3.client('s3')
    total_rows = 0
    try:
        result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        for obj in result.get('Contents', []):
            if obj['Key'].endswith('.json'):
                obj_body = s3.get_object(Bucket=bucket_name, Key=obj['Key'])['Body']
                for line in obj_body.iter_lines():
                    try:
                        json.loads(line)
                        total_rows += 1
                    except json.JSONDecodeError:
                        continue
    except NoCredentialsError:
        print("Credentials not available")
    return total_rows

def count_rows_in_json_s3_spark(bucket_name, prefix):
    spark = SparkSession.builder.appName("CountRows").getOrCreate()
    s3_path = f"s3a://{bucket_name}/{prefix}/*.json"
    try:
        df = spark.read.json(s3_path)
        total_rows = df.count()
    except Exception as e:
        print(f"Error reading {s3_path}: {e}")
        total_rows = 0
    return total_rows

def landing_to_trusted_pandas(directory, output_dir):
    for sub in os.listdir(directory):
        sub_dir = os.path.join(directory, sub)
        output_sub_dir = os.path.join(output_dir, sub)
        os.makedirs(output_sub_dir, exist_ok=True)
        for filename in os.listdir(sub_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(sub_dir, filename)
                output_file_path = os.path.join(output_sub_dir, filename)
                try:
                    df = pd.read_json(file_path, lines=True)
                    df_filtered = df[df['sharewithresearchasofdate'].notnull() & (df['sharewithresearchasofdate'] != 0)]
                    df_filtered.to_json(output_file_path, orient='records', lines=True)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

def landing_to_trusted_spark(directory, output_dir):
    spark = SparkSession.builder.appName("LandingToTrusted").getOrCreate()
    for sub in os.listdir(directory):
        if '.' in sub:
            continue
        sub_dir = os.path.join(directory, sub)
        output_sub_dir = os.path.join(output_dir, sub)
        os.makedirs(output_sub_dir, exist_ok=True)
        for filename in os.listdir(sub_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(sub_dir, filename)
                output_file_path = os.path.join(output_sub_dir, filename)
                try:
                    df = spark.read.json(file_path)
                    df_filtered = df.filter(col('sharewithresearchasofdate').isNotNull() & (col('sharewithresearchasofdate') != 0))
                    df_filtered.write.json(output_file_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

def landing_to_trusted_s3_pandas(bucket_name, prefix, output_bucket_name, output_prefix):
    s3 = boto3.client('s3')
    try:
        result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        for obj in result.get('Contents', []):
            if obj['Key'].endswith('.json'):
                obj_body = s3.get_object(Bucket=bucket_name, Key=obj['Key'])['Body']
                df = pd.read_json(obj_body, lines=True)
                df_filtered = df[df['sharewithresearchasofdate'].notnull() & (df['sharewithresearchasofdate'] != 0)]
                output_key = os.path.join(output_prefix, obj['Key'].split('/')[-1])
                s3.put_object(Bucket=output_bucket_name, Key=output_key, Body=df_filtered.to_json(orient='records', lines=True))
    except NoCredentialsError:
        print("Credentials not available")

def landing_to_trusted_s3_spark(bucket_name, prefix, output_bucket_name, output_prefix):
    spark = SparkSession.builder.appName("LandingToTrusted").getOrCreate()
    s3_path = f"s3a://{bucket_name}/{prefix}/*.json"
    output_path = f"s3a://{output_bucket_name}/{output_prefix}"
    try:
        df = spark.read.json(s3_path)
        df_filtered = df.filter(col('sharewithresearchasofdate').isNotNull() & (col('sharewithresearchasofdate') != 0))
        df_filtered.write.json(output_path)
    except Exception as e:
        print(f"Error processing {s3_path}: {e}")
                    
def main():
    base_dir = 'project_data/landing'
    storage_type = 'local'  # or 's3'
    #storage_type = 'local'  # or 's3'
    processing_engine = 'spark'  
    #processing_engine = 'pandas'  # or 'spark'
    
    bucket_name = 'your-s3-bucket-name'
    prefix = 'your/s3/prefix'
    
    expected_counts = {
        'customer': 956,
        'accelerometer': 81273,
        'step_trainer': 28680
    }

    for folder, expected_count in expected_counts.items():
        if storage_type == 'local':
            folder_path = os.path.join(base_dir, folder)
            if processing_engine == 'pandas':
                actual_count = count_rows_in_json_pandas(folder_path)
            elif processing_engine == 'spark':
                actual_count = count_rows_in_json_spark(folder_path)
        elif storage_type == 's3':
            prefix_path = os.path.join(prefix, folder)
            if processing_engine == 'pandas':
                actual_count = count_rows_in_json_s3_pandas(bucket_name, prefix_path)
            elif processing_engine == 'spark':
                actual_count = count_rows_in_json_s3_spark(bucket_name, prefix_path)
        else:
            print("Invalid storage type")
            continue
        
        if actual_count == expected_count:
            print(f"Test passed for {folder}: {actual_count} rows")
        else:
            print(f"Test failed for {folder}: expected {expected_count}, found {actual_count}")

    # Filter and save to trusted
    customer_in = 'project_data/landing/customer'
    customer_out = 'project_data/trusted/customer'
    output_bucket_name = 'your-output-s3-bucket-name'
    output_prefix = 'your/output/s3/prefix'
    if storage_type == 'local':
        if processing_engine == 'pandas':
            landing_to_trusted_pandas(customer_in, customer_out)
        elif processing_engine == 'spark':
            landing_to_trusted_spark(customer_in, customer_out)
    elif storage_type == 's3':
        if processing_engine == 'pandas':
            landing_to_trusted_s3_pandas(bucket_name, prefix, output_bucket_name, output_prefix)
        elif processing_engine == 'spark':
            landing_to_trusted_s3_spark(bucket_name, prefix, output_bucket_name, output_prefix)


if __name__ == "__main__":
    main()
