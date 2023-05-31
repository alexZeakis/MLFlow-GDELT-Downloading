import mlflow
import argparse
import json

parser = argparse.ArgumentParser(description='An mlflow project for downloading \
                                 GDELT articles from specific date.')

parser.add_argument('-d', '--date', required=True, help='Date to download GDELT articles in the form of YYYYMMDD.')
parser.add_argument('-e', '--experiment', required=True, help='Experiment Name of MLFlow')
parser.add_argument('-a', '--airflow', help='Airflow run_id for connection')
parser.add_argument('-o', '--output', default="../data/", help='Path to the output file.')
parser.add_argument('-l', '--log', default="../logs/ingest/", help='Path to the log file.')
parser.add_argument('-t', '--tracking', help='URL of MLFlow tracking server')
parser.add_argument('-u', '--user', help='Name of MLFlow User.')

args = parser.parse_args()

if args.tracking is not None:
    mlflow.set_tracking_uri(args.tracking)
if args.user is not None:    
    mlflow.set_tag("user", args.user)

log_file = args.log+args.date+'.json'

# Set the parameters
parameters = {"date": args.date, "out": args.output, "logs": log_file,
              "airflow": args.airflow}

# Run the project
info = mlflow.run('./', parameters=parameters, experiment_name=args.experiment)
mlflow.end_run()

with open(log_file) as o:
    j = json.load(o)
    mlflow.start_run(run_id=info.run_id)
    for key, val in j.items():
        mlflow.log_metric(key, val)
    mlflow.end_run()

