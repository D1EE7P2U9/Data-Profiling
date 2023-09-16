
from flask import Flask, jsonify, request
import boto3
import json
import time


app = Flask(__name__)

@app.route('/create_glue_job', methods=['POST'])
def create_glue_job():
    try:
        # Read request JSON data
        req_data = request.get_json()

        col_name = req_data['col_name']
        operation = req_data['operation']
        jobname = req_data['jobname']
        aws_access_key = req_data['aws_access_key']
        aws_secret_key = req_data['aws_secret_key']

        region = req_data['aws_region']
        bucket_name = req_data['bucket_name']


        local_file_path = r'C:/***/***/***//script.py'

        
        

        # Read the content of the target script
        with open(local_file_path, "r") as f:
            content = f.read()

        # Replace the variable value

        updated_script = content.replace('<<col_name>>', col_name )
        updated_script = updated_script.replace('<<operation>>', operation )

        

        # Write the updated content back to the target script
        with open("script.py", "w") as f:
            f.write(updated_script)

        print("Variables replaced successfully.")
        print(updated_script)

        #creating boto3 session
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

        # S3 client
        s3 = session.client('s3')


        # Upload the updated script to S3
        s3.upload_file("path of locally generated hardcoded script_path", bucket_name, s3_key)  # define s3_key

        print("Script uploaded to S3 successfully!")



        ###creating new glue job to run the script 
        glue  = session.client('glue')

        script_loc = 'path of script in s3 bucket'

        
        command = {
            'Name': 'glueetl',
            'ScriptLocation':script_loc,
        }
        default_arguments = {
            '--job-language': 'python',
             '--additional-python-modules' : 's3://bucketnames and foder/vaex-4.17.0-py3-none-any.whl'  # used as we are using vaex lib
        }
        allocated_capacity = 2

        response = glue.create_job(
            Name=jobname,
            Role='your_role',
            Command=command,
            DefaultArguments=default_arguments,
            AllocatedCapacity=allocated_capacity,
            GlueVersion='4.0',
            MaxRetries=0
        )

        print(response)

        print(f"Created Glue job {jobname} with version {allocated_capacity}.")

        ############# Run glue job 

        response = glue.start_job_run(JobName=jobname)
        

        while True:
            job_run = glue.get_job_run(JobName=jobname, RunId=response['JobRunId'])
            status = job_run['JobRun']['JobRunState']
    
            if status in ('FAILED', 'SUCCEEDED'):
                break
    
            time.sleep(10)  # Wait for 10 seconds before checking the status again

        # Check if the job succeeded
        if status == 'SUCCEEDED':
            response = s3.get_object(Bucket='bucket_name', Key='data_profiler/temp/output.json')
            output_content = response['Body'].read().decode('utf-8')
            
            # Process and print the content
            print("Output Data:")
            print([status,{"result":{'result':[output_content]}}])
            print(output_content)

        else:
            print(f"Job failed with status: {status}")



        
        # return json.dumps({'status': 'Job completed', 'job_log_uri': job_log_uri})
    

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=False)
