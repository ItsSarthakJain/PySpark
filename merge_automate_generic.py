from pyspark.sql import SparkSession

def generate_merge_query(spark, source_db, source_table, target_db, target_table, merge_column, use_max_batchid):
    # Read the schema of the target table
    target_schema = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .table(f"{target_db}.{target_table}") \
        .schema

    # Generate column list for SET clause
    set_clause = ", ".join([f"target.{field.name} = source.{field.name}" for field in target_schema if field.name != merge_column])

    # Generate merge query
    if use_max_batchid:
    # Generate subquery to select records with max batchid from source
        subquery = f"""
            SELECT *
            FROM {source_db}.{source_table} AS source
            WHERE batchid = (
                SELECT MAX(CAST(batchid AS BIGINT)) AS max_batchid
                FROM {source_db}.{source_table}
            )
        """

        merge_query = f"""
                MERGE INTO {target_db}.{target_table} AS target
                USING ({subquery}) AS source
                ON target.{merge_column} = source.{merge_column}
                WHEN MATCHED THEN
                    UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({", ".join([field.name for field in target_schema])})
                    VALUES ({", ".join([f"source.{field.name}" for field in target_schema])})
            """
    else:
        merge_query = f"""
                    MERGE INTO {target_db}.{target_table} AS target
                    USING {source_db}.{source_table} AS source
                    ON target.{merge_column} = source.{merge_column}
                    WHEN MATCHED THEN
                        UPDATE SET {set_clause}
                    WHEN NOT MATCHED THEN
                        INSERT ({", ".join([field.name for field in target_schema])})
                        VALUES ({", ".join([f"source.{field.name}" for field in target_schema])})
                """

    return merge_query

# Example usage
if __name__ == "__main__":
    # Example parameters
    source_db = "lh_raw_chi_dev"
    source_table = "aprice"
    target_db = "lh_smith_chi_dev"
    target_table = "aprice"
    merge_column = "oid"
    use_max_batchid = False  # Set to True to use max batchid from source

    # Generate merge query
    merge_query = generate_merge_query(spark, source_db, source_table, target_db, target_table, merge_column, use_max_batchid)
    print(merge_query)