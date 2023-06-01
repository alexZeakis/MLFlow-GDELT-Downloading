import mlflow
import argparse
import json

def run(**kwargs):
    
    date = kwargs.get('date')
    experiment = kwargs.get('experiment')
    airflow = kwargs.get('airflow')
    output = kwargs.get('output')
    log = kwargs.get('log')
    tracking = kwargs.get('tracking')
    user = kwargs.get('user')
    
    if tracking is not None:
        mlflow.set_tracking_uri(tracking)
    if user is not None:    
        mlflow.set_tag("user", user)
    
    log_file = log+date+'.json'
    
    # Set the parameters
    parameters = {"date": date, "out": output, "logs": log_file,
              "airflow": airflow}
    
    # Run the project
    # info = mlflow.run('./', parameters=parameters, experiment_name=experiment)
    info = mlflow.run('https://github.com/alexZeakis/MLFlow-GDELT-Downloading.git',
                      parameters=parameters, experiment_name=experiment)
    # info = mlflow.run('./', parameters=parameters, experiment_name=args.experiment, backend='docker')
    mlflow.end_run()
    
    with open(log_file) as o:
        j = json.load(o)
        mlflow.start_run(run_id=info.run_id)
        for key, val in j.items():
            mlflow.log_metric(key, val)
        mlflow.end_run()    


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='An mlflow project for downloading \
                                     GDELT articles from specific date.')
    
    parser.add_argument('-d', '--date', required=True, help='Date to download GDELT articles in the form of YYYYMMDD.')
    
    
    parser.add_argument('-e', '--experiment', required=True, help='Experiment Name of MLFlow')
    parser.add_argument('-a', '--airflow', help='Airflow run_id for connection')
    parser.add_argument('-o', '--output', default="../data/", help='Path to the output file.')
    parser.add_argument('-l', '--log', default="../logs/deduplicate/", help='Path to the log file.')
    parser.add_argument('-t', '--tracking', help='URL of MLFlow tracking server')
    parser.add_argument('-u', '--user', help='Name of MLFlow User.')
    
    args = parser.parse_args()
    
    parameters = {"date": args.date, 'experiment': args.experiment, 'airflow': args.airflow,
                  'output': args.output, 'log': args.log, 'tracking': args.tracking, 'user': args.user}
    
    run(**parameters)
