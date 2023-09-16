import vaex
import json
import pandas as pd 
import boto3


df  = pd.read_csv('s3_path_of your file')
df = vaex.from_pandas(df)

#printing column names
# print(df.column_names)

col_names = df.column_names

col_counts = df.column_count

# Specify the column name and operation
col = '<<col_name>>'
operation = '<<operation>>'

col_dtype = df[col].dtype

# Initializing a dictionary to store the results
result_dict = {}
result_dict['column_name'] = col
result_dict['col_dtype'] = str(col_dtype)


# Calculating statistics based on the operation
if operation == 'count':
    total_count = len(df)
    null_count = df[col].isna().sum().item()
    null_count_percentage = (null_count * 100) / total_count
    non_null_count = total_count - null_count
    distinct_count = len(df[col].dropna().unique())

    result_dict["totalCount"] = total_count
    result_dict["nullCount"] = null_count
    result_dict["nullCountPercentage"] = null_count_percentage
    result_dict["nonNullCount"] = non_null_count
    result_dict["distinctCount"] = distinct_count

elif operation == 'minimum':
    minimum = df[col].min().item()
    result_dict["minimumValue"] = minimum

elif operation == 'maximum':
    maximum = df[col].max().item()
    result_dict["maximumValue"] = maximum

elif operation == 'mean_val':
    mean_val = df[col].mean().item()
    result_dict["meanValue"] = mean_val

elif operation == 'stddev_val':
    stddev_val = df[col].std()
    result_dict["stddevValue"] = stddev_val

elif operation == 'kurtosis_val':
    kurtosis_val = df[col].kurtosis().item()
    result_dict["kurtosisValue"] = kurtosis_val

elif operation == 'skewness_val':
    skewness_val = df[col].skew().item()
    result_dict["skewnessValue"] = skewness_val

elif operation == 'distinct_list':
    distinct_list = df[col].dropna().unique().tolist()
    result_dict["distinctItems"] = distinct_list

# Print the results
print(result_dict)

print(type(result_dict))

json_dict = json.dumps(result_dict,indent=4)

s3 = boto3.client('s3')

s3.put_object(
    Bucket='bucket_name',
    Key='data_profiler/temp/output.json',
    Body=json.dumps(result_dict),
    ContentType='application/json'
)

print(json_dict)
