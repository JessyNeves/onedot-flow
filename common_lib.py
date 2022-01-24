import json
from pyspark.sql.functions import *
from pyspark.sql.types import * 


def validate_and_load_configuration(config_path="configuration/normalization.json"):

    # Deserialize JSON 
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        
        # Check if all columns defined have mappings
        if set(config.get("to_normalize")) == set(config.get("mappings").keys()):
            return config
        else: 
            raise ValueError("One or more Mappings are missing for the columns specified")
        
    except ValueError as e:
        print(e)

        
def normalizer(value, col, mappings):
    
    # Load column settings
    settings = mappings.get(col)

    # If there is a mapped value, use it
    if value in settings.keys():
        return settings.get(value)
    elif settings.get(value) == value:
        return value 
    else: 
        return "Other"

    
def apply_normalization(df, columns, mappings):
    # Create Auxiliary DF for in-place changes 
    normalized_df = df
    
    for col in columns:
        # Define UDF 
        normalize_column = udf(lambda x: normalizer(x, col, mappings), StringType())
        # Apply normalization to Column
        normalized_df = normalized_df.withColumn(col, normalize_column(df[col]))
        
    return normalized_df


def check_and_enforce_schema(df, table_name):

    with open("schemas/target.json", "r") as f:
        json_schema = json.load(f)

    schema = StructType.fromJson(json_schema[table_name])


    # If schemas are equal, return df as is.
    if set(schema) == set(df.schema):
        return df
    # If only the data types differ, fix by casting.
    elif set(df.columns) == set(schema.names):
        # Enforce schema
        for field in schema:
            column_name = str(field.name)
            if df.schema[column_name].dataType == field.dataType:
                # If the data type is equal, there is no need to enforce.
                continue

            df = df.withColumn(
                    column_name,
                    col(column_name).cast(str(field.dataType).replace("Type", ""))
                )
        return df
        # There is a schema mismatch, provide insight upon error throw.
    else:
        df_cols = set(df.columns)
        schema_cols = set(schema.names)
        df_sh_diff = df_cols - schema_cols
        sh_df_diff = schema_cols - df_cols
        if (len(df_sh_diff) == 0) & (len(sh_df_diff) != 0):
            raise Exception(f"Column mismatch: {sh_df_diff} on schema but not in df")
        elif (len(df_sh_diff) != 0) & (len(sh_df_diff) == 0):
            raise Exception(f"Column mismatch: {df_sh_diff} in df but not on schema")
        else:
            raise Exception(f"Column name difference between df and schema. {df_sh_diff}")