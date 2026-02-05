import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import json
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
import os
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

    # Create output directory if it doesn't exist
    output_dir = "data/parsed/"
    os.makedirs(output_dir, exist_ok=True)

    # Collect all rows from the dataframe
    rows = result_df.collect()

    # Save each row as a separate JSON file
    for row in rows:
        question_id = row["question_id"]

        # Create JSON object with all fields
        json_data = {
            "question_id": int(question_id),
            "question_title": row["question_title"],
            "question_body": row["question_body"],
            "answer_body": row["answer_body"],
        }

        # Save to file named <question_id>.json
        filename = f"{output_dir}{question_id}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"Created {len(rows)} JSON files in {output_dir}")


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
