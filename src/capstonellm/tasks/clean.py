import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import json
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


def clean(spark: SparkSession, environment: str, tag: str):
    # Read directly from S3
    questions_df = spark.read.json(
        "s3a://dataminded-academy-capstone-llm-data-us/input/dbt/questions.json"
    )

    # Explode the items array to get one row per item
    questions_df = questions_df.select(explode("items").alias("item"))

    # Flatten the struct to get individual columns
    questions_df = questions_df.select("item.*")

    answers_df = spark.read.json(
        "s3a://dataminded-academy-capstone-llm-data-us/input/dbt/answers.json"
    )

    # Explode the items array to get one row per item
    answers_df = answers_df.select(explode("items").alias("item"))

    # Flatten the struct to get individual columns
    answers_df = answers_df.select("item.*")

    result_df = questions_df.join(
        answers_df, questions_df.accepted_answer_id == answers_df.answer_id, "inner"
    ).select(
        questions_df.question_id.alias("question_id"),
        questions_df.title.alias("question_title"),
        questions_df.body.alias("question_body"),
        answers_df.body.alias("answer_body"),
    )

    # Initialize S3 client
    s3_client = boto3.client("s3")
    bucket_name = "dataminded-academy-capstone-llm-data-us"

    # Set username
    username = "jonathan"

    # Collect all rows from the dataframe
    rows = result_df.collect()

    # Upload each row as a separate JSON file to S3
    for row in rows:
        question_id = row["question_id"]

        # Create JSON object with all fields
        json_data = {
            "question_id": int(question_id),
            "question_title": row["question_title"],
            "question_body": row["question_body"],
            "answer_body": row["answer_body"],
        }

        # Upload to S3 under cleaned/<user>/{tag}/{question_id}.json
        s3_key = f"cleaned/{username}/{tag}/{question_id}.json"
        json_string = json.dumps(json_data, ensure_ascii=False, indent=2)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_string.encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Uploaded s3://{bucket_name}/{s3_key}")

    print(
        f"Successfully uploaded {len(rows)} JSON files to S3 under cleaned/{username}/{tag}/"
    )


def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e",
        "--env",
        dest="env",
        help="environment we are executing in",
        required=False,
        default="local",
    )
    parser.add_argument(
        "-t",
        "--tag",
        dest="tag",
        help="the tag to process",
        default="python-polars",
        required=False,
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession(
            "capstone_llm", spark_config=common_spark_config
        ) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
