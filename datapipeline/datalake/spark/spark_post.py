import argparse
from os.path import join
import sys

sys.path.append('/home/marcelo/Documentos/datapipeline/airflow/')

import connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import psycopg2
from sqlalchemy import create_engine

def curbanco():
    con = psycopg2.connect(
        user='',
        password='',
        host='',
        port='5432',
        database=''
    )
    return con;

try:
    con = curbanco()
    cur = con.cursor()
    connected = 1

except:
    print("Erro ao conectar")


def get_tweets_data(df):
    return df \
        .select(
        f.explode("data").alias("tweets")
    ).select(
        "tweets.author_id",
        "tweets.conversation_id",
        "tweets.created_at",
        "tweets.id",
        "tweets.public_metrics.*",
        "tweets.text"
    )


def get_users_data(df):
    return df \
        .select(
        f.explode("includes.users").alias("users")
    ).select(
        "users.id",
        "users.name",
        "users.username",
        "users.created_at"

    )


def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)


def twitter_transform(spark, src):
    df = spark.read.json(src)

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    user_df = user_df.toPandas()
    tweet_df = tweet_df.toPandas()
    print(user_df)
    
    insertIntoTable(tweet_df, user_df, con, cur)


def insertIntoTable(tweet_df, user_df, con, cur):
    user_df.to_sql('mytemptable_users', db, index=False, if_exists='replace', method='multi')
    tweet_df.to_sql('mytemptable_conversas', db, index=False, if_exists='replace', method='multi')
    cur = con.cursor()
    cur.execute(
        "INSERT INTO users (id,name,username,created_at) SELECT id::bigint,name,username,created_at FROM mytemptable_users ON CONFLICT (id) DO NOTHING;")
    cur.execute(
        "INSERT INTO conversas (id, author_id, conversation_id, created_at, in_reply_to_user_id, like_count, "
        "reply_count, retweet_count, text) " 
        "SELECT id::bigint, author_id::bigint, conversation_id::bigint, created_at, NULL, "
        "like_count::int, reply_count::int, retweet_count::int, text " 
        " FROM mytemptable_conversas ON CONFLICT (id) DO NOTHING;")
    cur.execute("drop table mytemptable_users;")
    cur.execute("drop table mytemptable_conversas;")
    con.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    # parser.add_argument("--src", required=True)
    # args = parser.parse_args()
    parser.add_argument("--src", required=True)
    args = parser.parse_args()

    spark = SparkSession \
        .builder \
        .appName("twitter_BDD_two") \
        .getOrCreate()

    twitter_transform(spark, args.src)
