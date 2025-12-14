from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import os
import sys



default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='video_creator_stats_dag',
    default_args=default_args,
    description='Compute video creator stats every 10 minutes',
    schedule='*/10 * * * *',
    start_date=datetime(2025, 12, 15),
    catchup=False,
) as dag:

    logging.info("Starting video creator stats dag")
    # Add include/VideoCreatorStats to sys.path to allow imports of local modules (config, utils, etc)
    include_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '/usr/local/airflow'), 'include', 'VideoCreatorStats')
    if include_dir not in sys.path:
        sys.path.append(include_dir)

    from airflow.decorators import task

    @task(task_id='creator_stats')
    def run_stats_task():
        try:
            # Import here to avoid top-level failures if paths aren't set yet during parsing
            from process import main as process_main
            
            logging.info("Imported process successfully. Running main function...")
            process_main()
            logging.info("Process main function completed.")
            
        except ImportError as e:
            logging.error(f"Failed to import process module. Sys path is: {sys.path}")
            raise e
        except Exception as e:
            logging.error(f"Error executing stats process: {e}")
            raise e
    
    run_stats_task()
