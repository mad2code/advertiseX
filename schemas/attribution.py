from pyspark.sql.functions import col, struct, expr

def create_attribution_schema(ad_impressions_df, clicks_conversions_df, bid_requests_df):
    # Rename timestamp columns to avoid ambiguity and add watermark
    ad_impressions_df = ad_impressions_df \
        .withColumnRenamed("timestamp", "ad_impressions_timestamp").alias("ad_impressions")

    clicks_conversions_df = clicks_conversions_df \
        .withColumnRenamed("timestamp", "clicks_conversions_timestamp").alias("clicks_conversions")

    bid_requests_df = bid_requests_df \
        .withColumnRenamed("timestamp", "bid_requests_timestamp").alias("bid_requests")

    # Join all three DataFrames in a single expression
    attribution_df = bid_requests_df.join(
        ad_impressions_df, expr("""
            bid_requests.user_id = ad_impressions.user_id
        """), "leftOuter"
    ).join(
        clicks_conversions_df, expr("""
            ad_impressions.user_id = clicks_conversions.user_id 
        """), "leftOuter"
    ).select(
        col("ad_impressions.user_id"),
        struct(
            col("auction_id"),
            col("targeting_criteria"),
            col("bid_requests_timestamp").alias("timestamp")
        ).alias("bid_requests"),
        struct(
            col("ad_creative_id"),
            col("website"),
            col("ad_impressions_timestamp").alias("timestamp")
        ).alias("ad_impressions"),
        struct(
            col("campaign_id"),
            col("conversion_type"),
            col("clicks_conversions_timestamp").alias("timestamp")
        ).alias("clicks_conversions")
    )

    return attribution_df
