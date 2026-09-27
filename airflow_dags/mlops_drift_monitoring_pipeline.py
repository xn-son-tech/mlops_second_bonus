"""
=============================================================================
DAG: MLOps Drift Monitoring & Automated Retraining Trigger
=============================================================================
Automates:
1. Health check of Evidently AI Drift Detection service
2. Triggering automated drift analysis comparing production vs reference data
3. Evaluating drift metrics (drift score, drifted features)
4. Alerting via Telegram when data drift is detected
5. Automatically triggering the Model Retraining DAG upon drift detection
"""

import os
import sys
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Ensure utils can be imported in Airflow container
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)

from utils.telegram_notifier import (
    on_task_failure_callback,
    notify_drift_alert,
    notify_drift_healthy,
    send_telegram_message,
)

EVIDENTLY_URL = os.getenv("EVIDENTLY_URL", "http://evidently:8001")
DRIFT_THRESHOLD = float(os.getenv("EVIDENTLY_DRIFT_THRESHOLD", "0.1"))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_task_failure_callback,
}

# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def check_evidently_health(**context):
    """Check if Evidently service is up and has reference data."""
    print(f"🔍 Checking Evidently AI service at {EVIDENTLY_URL}/health...")
    res = requests.get(f"{EVIDENTLY_URL}/health", timeout=10)
    if res.status_code != 200:
        raise ConnectionError(f"Evidently service returned unhealthy status: {res.status_code}")

    health_data = res.json()
    print(f"✅ Evidently Health Status: {health_data}")

    if not health_data.get("reference_data_loaded"):
        print("⚠️ Warning: Reference data not loaded in Evidently. Analysis may require reference data upload.")

    prod_count = health_data.get("production_data_count", 0)
    print(f"📊 Current production samples collected: {prod_count}")
    context["ti"].xcom_push(key="production_count", value=prod_count)
    return health_data


def trigger_drift_analysis(**context):
    """Call Evidently /analyze endpoint to evaluate feature drift."""
    print(f"🚀 Triggering drift analysis via {EVIDENTLY_URL}/analyze...")
    payload = {"threshold": DRIFT_THRESHOLD}

    res = requests.post(f"{EVIDENTLY_URL}/analyze", json=payload, timeout=30)
    if res.status_code != 200:
        error_detail = res.text
        # If no production data is available yet, handle gracefully
        if "No production data available" in error_detail or "Reference data not loaded" in error_detail:
            print(f"ℹ️ Evidently notice: {error_detail}")
            return {
                "drift_detected": False,
                "drift_score": 0.0,
                "drifted_features": [],
                "total_features": 0,
                "notice": error_detail
            }
        raise RuntimeError(f"Evidently /analyze failed with code {res.status_code}: {error_detail}")

    result = res.json()
    print(f"📊 Drift Analysis Result: {result}")

    context["ti"].xcom_push(key="drift_result", value=result)
    return result


def branch_drift_decision(**context):
    """Branch execution depending on whether data drift was detected."""
    ti = context["ti"]
    result = ti.xcom_pull(key="drift_result", task_ids="trigger_drift_analysis") or {}

    drift_detected = result.get("drift_detected", False)
    drifted_count = result.get("drifted_count", 0)
    drift_score = result.get("drift_score", 0.0)

    print(f"⚖️ Evaluating Drift Result: detected={drift_detected}, count={drifted_count}, score={drift_score}")

    if drift_detected or drifted_count > 0:
        print("⚠️ DRIFT DETECTED -> Routing to drift alert & retraining trigger!")
        return "handle_drift_alert"
    else:
        print("🟢 NO DRIFT DETECTED -> System healthy.")
        return "handle_drift_healthy"


def handle_drift_alert(**context):
    """Send high-priority Telegram alert when drift is confirmed."""
    ti = context["ti"]
    result = ti.xcom_pull(key="drift_result", task_ids="trigger_drift_analysis") or {}

    drift_score = result.get("drift_score", 0.0)
    drifted_features = result.get("drifted_features", [])
    total_features = result.get("total_features", 13)
    report_filename = result.get("report_filename")

    print(f"📢 Sending Telegram Alert: {len(drifted_features)} features drifted!")
    notify_drift_alert(
        drift_score=drift_score,
        drifted_features=drifted_features,
        total_features=total_features,
        report_name=report_filename
    )
    return "Drift alert dispatched"


def handle_drift_healthy(**context):
    """Log and optionally notify when distribution is normal."""
    ti = context["ti"]
    result = ti.xcom_pull(key="drift_result", task_ids="trigger_drift_analysis") or {}

    drift_score = result.get("drift_score", 0.0)
    total_features = result.get("total_features", 13)

    print(f"✅ Data distribution is stable. Drift score: {drift_score:.4f}")
    
    notify_on_success = os.getenv("TELEGRAM_NOTIFY_ON_SUCCESS", "false").lower() == "true"
    if notify_on_success:
        notify_drift_healthy(drift_score=drift_score, total_features=total_features)

    return "System healthy"


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="mlops_drift_monitoring_pipeline",
    default_args=default_args,
    description="Automated Data Drift Observability, Telegram Alerting & Retraining Trigger",
    schedule_interval=None,  # Or "@hourly" in production
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "evidently", "drift-monitoring", "telegram"],
) as dag:

    t_health = PythonOperator(
        task_id="check_evidently_health",
        python_callable=check_evidently_health,
    )

    t_analyze = PythonOperator(
        task_id="trigger_drift_analysis",
        python_callable=trigger_drift_analysis,
    )

    t_branch = BranchPythonOperator(
        task_id="branch_drift_decision",
        python_callable=branch_drift_decision,
    )

    t_alert = PythonOperator(
        task_id="handle_drift_alert",
        python_callable=handle_drift_alert,
    )

    t_trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retraining_dag",
        trigger_dag_id="mlops_wine_quality_pipeline",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_healthy = PythonOperator(
        task_id="handle_drift_healthy",
        python_callable=handle_drift_healthy,
    )

    t_end = EmptyOperator(
        task_id="drift_pipeline_completed",
        trigger_rule="none_failed_min_one_success",
    )

    # DAG Dependency Flow
    t_health >> t_analyze >> t_branch
    t_branch >> t_alert >> t_trigger_retrain >> t_end
    t_branch >> t_healthy >> t_end
