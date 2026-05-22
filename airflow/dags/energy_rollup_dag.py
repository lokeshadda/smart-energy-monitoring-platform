# airflow/dags/energy_rollup_10min.py
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

TZ = pendulum.timezone("America/New_York")

with DAG(
    dag_id="energy_rollup_10min",
    description="10-minute city-level energy rollups from Redis minute windows",
    # Use a static past start_date so the DAG appears immediately
    start_date=datetime(2025, 1, 1, tzinfo=TZ),
    schedule_interval="*/10 * * * *",   # every 10 minutes
    catchup=False,
    tags=["energy","rollup"],
    default_args={"depends_on_past": False},
) as dag:

    # Compute last completed 10-minute bucket (script handles windowing)
    compute_bucket = BashOperator(
        task_id="compute_last_10min_rollup",
        bash_command=(
            "ROLLUP_MINUTES=10 ROLLUP_PREFIX=tenmin "
            "python /opt/app/spark/batch_rollup.py"
        ),
        do_xcom_push=True,
    )

    # Optional: quick glance at recent Miami keys (non-fatal if none yet)
    verify = BashOperator(
        task_id="verify_recent_keys",
        bash_command=(
            "echo 'Recent Miami rollup keys:' && "
            "redis-cli -h redis KEYS 'tenmin:*:Miami' | head -n 10 || true"
        ),
    )

    compute_bucket >> verify
