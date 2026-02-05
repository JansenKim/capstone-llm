import argparse
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
import json

logger = logging.getLogger(__name__)

S3_Q_INPUT_PATH = "s3a://dataminded-academy-capstone-llm-data-us/input/dbt/questions.json"
S3_A_INPUT_PATH = "s3a://dataminded-academy-capstone-llm-data-us/input/dbt/answers.json"
S3_OUTPUT_BASE_PATH = "s3a://dataminded-academy-capstone-llm-data-us/cleaned/Kim"

def clean(spark: SparkSession, environment: str, tag: str):
    # create 1 json document per question containing the title, question body and response body
    # from both the questions and answers and join them together using the question_id field.
    df_questions = spark.read.json(f"{S3_Q_INPUT_PATH}")
    df_answers = spark.read.json(f"{S3_A_INPUT_PATH}")

    #df_questions.printSchema()
    #df_answers.printSchema()

    df_questions_explode = df_questions.select(sf.explode(sf.col('items')).alias("item"))
    #df_questions_explode.show(5)
    #df_questions_explode.printSchema()

    df_questions_flat = df_questions_explode.select(
            sf.col("item.body").alias("question_body"),
            sf.col("item.question_id").alias("question_id"),
            sf.col("item.title").alias("title"),
            sf.explode(sf.col("item.tags")).alias("tag"),
            )

    df_answers_explode = df_answers.select(sf.explode(sf.col('items')).alias("item"))
    df_answers_explode.show(5)
    df_answers_explode.printSchema()

    df_answers_flat = df_answers_explode.select(
            sf.col("item.body").alias("answer_body"),
            sf.col("item.question_id").alias("question_id"),
            )        

    df_answers_flat.printSchema()
    df_answers_flat.show(5)

    df_combined = df_questions_flat.join(df_answers_flat, on = "question_id")
    df_combined.show(5)
    print("df_combined shape:",(df_combined.count(), len(df_combined.columns)))

    df_one = df_combined.groupBy("question_id", "tag").agg(
    sf.first("title", ignorenulls=True).alias("title"),
    sf.first("question_body", ignorenulls=True).alias("question_body"),
    sf.first("answer_body", ignorenulls=True).alias("answer_body"),)

    print("df_one shape:",(df_one.count(), len(df_one.columns)))
    df_one.show(5)

    df_one.repartition(df_one.count()).write.mode("overwrite").json(f"{S3_OUTPUT_BASE_PATH}")

    # return 1 json document per question 
    pass

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
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
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
