from pyspark.sql import *
from pyspark.sql.functions import col, count, desc, when, countDistinct, \
    min as pyspark_min, max as pyspark_max, mean as pyspark_mean, \
    stddev as pyspark_stddev, sum as pyspark_sum, avg as pyspark_avg
from pyspark.sql.types import *
from functools import reduce  # Add this line at the beginning of your script
from notebookutils import mssparkutils

class DataQualityChecker:
    def __init__(self):{

    }

    # Define a function to clean column names
    def clean_column_name(self,col_name):
        return col_name.replace('-', '').replace('_', '').replace(' ', '').replace('"','').replace(',','')
        
    def profile_data(self, df):
        profile = []
        for column in df.columns:
            # print(df.schema[column].dataType)
            if df.schema[column].dataType in (StringType(), IntegerType(),TimestampType(),DateType(),BooleanType()):
                distinct_count = df.select(column).distinct().count()
                profile.append(("distinct_count", column, str(distinct_count)))  # Convert count to string
            else:
                summary_stats = self.calculate_summary_stats(df, column)
                profile.extend([
                    ("min", column, str(summary_stats["min"])),  # Convert numerical values to string
                    ("max", column, str(summary_stats["max"])),
                    ("mean", column, str(summary_stats["mean"])),
                    ("stddev", column, str(summary_stats["stddev"])),
                    ("sum", column, str(summary_stats["sum"])),  # Add sum to profile
                    ("avg", column, str(summary_stats["avg"]))   # Add avg to profile
                ])

        return profile

    def calculate_summary_stats(self, df, column):
        summary_stats = df.select(
            pyspark_min(col(column)).alias("min"),
            pyspark_max(col(column)).alias("max"),
            pyspark_mean(col(column)).alias("mean"),
            pyspark_stddev(col(column)).alias("stddev"),
            pyspark_sum(col(column)).alias("sum"),  # Calculate sum
            pyspark_avg(col(column)).alias("avg")   # Calculate avg
        ).collect()[0]

        return {
            "min": summary_stats["min"],
            "max": summary_stats["max"],
            "mean": summary_stats["mean"],
            "stddev": summary_stats["stddev"],
            "sum": summary_stats["sum"],
            "avg": summary_stats["avg"]
        }

    def compare_profiles(self,source_profile_df, target_profile_df):
        try:
            comparison_df = source_profile_df.alias("source").join(
                target_profile_df.alias("target"),
                (col("source.Column") == col("target.Column")) & (col("source.Statistic") == col("target.Statistic")),
                how="outer"
            )

            comparison_df = comparison_df.withColumn(
                "Comparison",
                when(col("source.Value") == col("target.Value"), "Match").otherwise("Mismatch")
            )

            comparison_df = comparison_df.select(
                col("source.Column").alias("Column"),
                col("source.Statistic").alias("Statistic"),
                col("source.Value").alias("SourceValue"),
                col("target.Value").alias("TargetValue"),
                col("Comparison")
            )
            print("\nProfiling Completed\n")
            return comparison_df

        except Exception as e:
            print("An error occurred during comparison:", str(e))
            return None

    def get_source_dataframe(self, source):
        if isinstance(source, DataFrame):
            return source
        # Load dataframe from source
        if source.startswith("abfss://"):
            if source.endswith(".parquet"):
                return spark.read.parquet(source)
            elif source.endswith(".csv"):
                return spark.read.option("quote", "\"") \
                                .option("escape", "\"") \
                                .csv(source, header=True, inferSchema=True)
            else:
                return spark.read.load(source)
        else:
            return spark.table(source)

    def convert_datatype(self,datatype):
        if hasattr(datatype, "__class__"):
            return str(datatype)
        else:
            return datatype

    def compare_schema(self,source, target, source_name, target_name):
        # Compare schemas of source and target dataframes
        source_schema = source.schema
        target_schema = target.schema

        source_columns = {field.name: self.convert_datatype(field.dataType) for field in source_schema}
        target_columns = {field.name: self.convert_datatype(field.dataType) for field in target_schema}

        comparison_results = []

        for column in sorted(set(source_columns.keys()) | set(target_columns.keys())):
            source_datatype = source_columns.get(column, "Not present")
            target_datatype = target_columns.get(column, "Not present")

            comparison_results.append((column, source_datatype, target_datatype, source_datatype != "Not present", target_datatype != "Not present"))

        schema = StructType([
            StructField("Column Name", StringType(), True),
            StructField("Source Datatype", StringType(), True),
            StructField("Target Datatype", StringType(), True),
            StructField("Present in Source", StringType(), True),
            StructField("Present in Target", StringType(), True)
        ])

        df_comparison = spark.createDataFrame(comparison_results, schema)

        if df_comparison.filter((df_comparison["Present in Source"] == False) | (df_comparison["Present in Target"] == False) | (df_comparison["Source Datatype"] != df_comparison["Target Datatype"])).count() == 0:
            print("\nSchema comparison: Passed.\n")
        else:
            print("\nSchema comparison: Failed.\n")

        return df_comparison

    def check_data_completeness(self, df, table_name):
        completeness_counts = df.select([count(when(col(c).isNull(), True)).alias(c) for c in df.columns])

        # Filter out columns with non-zero counts
        non_zero_counts = completeness_counts.filter(reduce(lambda x, y: x | y, [(col(c) > 0) for c in df.columns]))

        # Filter out columns with non-zero counts for display
        non_zero_columns = [c for c in non_zero_counts.columns if non_zero_counts.select(col(c)).collect()[0][0] > 0]

        if non_zero_counts.count() > 0:
            display(non_zero_counts.select(*non_zero_columns))
        else:
            print(f"\nNo missing values found in {table_name}.\n")

    def find_different_rows(self, source, target, primary_key_columns):
        # Find rows with different data based on primary key columns
        source_cols = source.columns
        target_cols = target.columns
        # Join source and target DataFrames on primary key columns
        join_condition = reduce(lambda x, y: x & y, [source[col] == target[col] for col in primary_key_columns])
        diff_df = source.join(target, on=join_condition, how="left_anti")

        # Calculate the count of different records
        diff_count = diff_df.count()
        if diff_count == 0:
            return None  # Return None when there are no differences
        else:
            # Select columns from the source DataFrame and alias them with the source table name
            return diff_df.select([col(col_name).alias(f"{source}_{col_name}") for col_name in source_cols])

    def display_discrepancies(self, source, target, primary_key_columns):
        source_cols = [col for col in source.columns if col not in primary_key_columns]

        discrepancies_exist = False

        for col_name in source_cols:
            discrepancy_rows = source.select(primary_key_columns + [source[col_name].alias("source")]) \
                .join(target.select(primary_key_columns + [target[col_name].alias("target")]), on=primary_key_columns, how="inner") \
                .filter(col("source") != col("target"))

            discrepancy_count = discrepancy_rows.count()

            if discrepancy_count == 0:
                {}
            else:
                print(f"\nDiscrepancies found in column '{col_name}' for {discrepancy_count} rows:\n")
                if discrepancy_count > 100:
                    display(discrepancy_rows.limit(50))
                else:
                    display(discrepancy_rows)
                discrepancies_exist = True

        if not discrepancies_exist:
            print("\nNo discrepancies found in any column.\n")

    def source_df_col_rename(self,df):
        # Get the current column names
        columns = df.columns
        # Loop over each column and rename it in the DataFrame
        for column in columns:
            cleaned_col_name = self.clean_column_name(column)
            df = df.withColumnRenamed(column, cleaned_col_name)
        return df

    def main(self, source, target, primary_key_columns,\
             profile_check=True,schema_check=True,data_completeness_check=True,\
             row_wise_check=True):
        # Load source and target dataframes
        source_name=source
        target_name=target
        source_table = self.source_df_col_rename(self.get_source_dataframe(source))
        target_table = self.source_df_col_rename(self.get_source_dataframe(target))

        print("Starting data quality checks...\n")

        # Log the checks to be performed
        checks_to_perform = []
        if profile_check:
            checks_to_perform.append("Data Profiling")
        if schema_check:
            checks_to_perform.append("Schema Comparison")
        if data_completeness_check:
            checks_to_perform.append("Data Completeness Check")
        if row_wise_check:
            checks_to_perform.append("Row-wise Data Comparison")

        print("The following checks will be performed:")
        for check in checks_to_perform:
            print(f"- {check}")

        if(profile_check):
            # 1. Data Profiling
            print(f"Profiling results for \n{source}\nvs\n{target}\n")
            source_profile = self.profile_data(source_table)
            target_profile = self.profile_data(target_table)

            source_profile_df = spark.createDataFrame(source_profile, schema=StructType([
                StructField("Statistic", StringType(), nullable=False),
                StructField("Column", StringType(), nullable=False),
                StructField("Value", StringType(), nullable=True)
            ]))

            target_profile_df = spark.createDataFrame(target_profile, schema=StructType([
                StructField("Statistic", StringType(), nullable=False),
                StructField("Column", StringType(), nullable=False),
                StructField("Value", StringType(), nullable=True)
            ]))

            #Comparing Profiling results for source vs target
            comparison_df = self.compare_profiles(source_profile_df, target_profile_df)

            if comparison_df is not None:
                # Display comparison results
                display(comparison_df.filter(col("Comparison")=="Mismatch"))
                print(f"\n\n")
            else:
                print("Comparison failed.")

        if schema_check:
            # 3. Schema Comparison
            print(f"\nComparing Schema of:\n{source_name} \n\tvs\n{target_name}\n")
            schema_check_results=self.compare_schema(source_table,target_table,source_name,target_name)
            display(schema_check_results)

        if data_completeness_check:
            # 4. Data Completeness Check
            print(f"\nChecking Data Completeness for: {source_name}\n\n")
            self.check_data_completeness(source_table, source)
            print(f"\n\nChecking Data Completeness for: {target_name}\n\n")
            self.check_data_completeness(target_table, target)
            print("")

        # Find rows with different data in source and target
        print(f"\nComparing Data of:\n{source_name} \n\tvs\n{target_name}\n")
        diff_in_source = self.find_different_rows(source_table, target_table, primary_key_columns)
        if diff_in_source is not None:
            if diff_in_source.isEmpty():
                {}
            else:
                diff_count = diff_in_source.count()
                print(f"Count of rows with different data in {source}: {diff_count}")
                print(f"Rows with different data in {source}:\n")
                display(diff_in_source.take(10))
        else:
            print(f"No differences found in {source} table.\n")

        diff_in_target = self.find_different_rows(target_table, source_table, primary_key_columns)
        if diff_in_target is not None:
            if diff_in_target.isEmpty():
                {}
            else:
                diff_count = diff_in_target.count()
                print(f"Count of rows with different data in {target}: {diff_count}")
                print(f"Rows with different data in {target}:\n")
                display(diff_in_target.take(10))
        else:
            print(f"No differences found in {source} table.\n")

        if row_wise_check:
            print(f"\nChecking Discrepancies for each Column(Row matching) for:\n{source}\n\tvs\n{target}\n\n")
            self.display_discrepancies(source_table, target_table, primary_key_columns)
            print("\n")

        print(f"Data quality checks completed.")


############################# Usage #####################################

# data_quality_checker = DataQualityChecker()
# data_quality_checker.main(
#   source = "sourcet table/df/file/folderpath",
#    target = "target table/df/file/folderpath",
#     primary_key_columns=["OrderNumber"],
#     profile_check=False,
#     schema_check=False,
#     data_completeness_check=False,
#     row_wise_check=True
#     )
