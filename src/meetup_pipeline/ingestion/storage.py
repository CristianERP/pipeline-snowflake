from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_file_to_s3(delta_info: dict, bucket: str, aws_conn_id: str = "aws_default") -> dict:
    hook = S3Hook(aws_conn_id=aws_conn_id)

    hook.load_file(
        filename=delta_info["file_path"],
        key=delta_info["s3_key"],
        bucket_name=bucket,
        replace=True,
    )

    return delta_info