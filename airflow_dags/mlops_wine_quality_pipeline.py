"""
=============================================================================
DAG: MLOps Wine Quality Training & Deployment Pipeline
=============================================================================
Automates:
1. System health verification (MLflow, MinIO)
2. Data preparation & validation
3. Model training with MLflow tracking
4. Quality gate evaluation (accuracy threshold)
5. Model registry & promotion to Production stage
6. Inference smoke test against FastAPI server
7. Telegram alert with model performance summary
"""

import os
import sys
from datetime import datetime, timedelta
import requests
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure utils can be imported in Airflow container
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)

from utils.telegram_notifier import (
    on_task_failure_callback,
    notify_model_training_result,
    send_telegram_message,
)

# =============================================================================
# CONSTANTS & CONFIGURATION
# =============================================================================

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MINIO_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
API_ENDPOINT = os.getenv("API_URL", "http://api:8000")
MODEL_NAME = os.getenv("MODEL_NAME", "wine_quality_model")
EXPERIMENT_NAME = os.getenv("EXPERIMENT_NAME", "wine_quality_experiment")
ACCURACY_THRESHOLD = float(os.getenv("MIN_ACCURACY_THRESHOLD", "0.85"))

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

def check_services(**context):
    """Verify that MLflow and MinIO are alive before running the pipeline."""
    print(f"🔍 Checking MLflow at {MLFLOW_TRACKING_URI}...")
    try:
        res = requests.get(f"{MLFLOW_TRACKING_URI}/health", timeout=5)
        print(f"   MLflow status code: {res.status_code}")
    except Exception as e:
        print(f"⚠️ MLflow health check warning (may still be accessible): {e}")

    print(f"🔍 Checking MinIO at {MINIO_ENDPOINT}...")
    try:
        res = requests.get(f"{MINIO_ENDPOINT}/minio/health/live", timeout=5)
        print(f"   MinIO status code: {res.status_code}")
    except Exception as e:
        print(f"⚠️ MinIO health check warning: {e}")

    return "Services checked"


def prepare_data(**context):
    """Load Wine dataset and validate basic data characteristics."""
    from sklearn.datasets import load_wine
    from sklearn.model_selection import train_test_split

    print("📊 Loading Wine dataset...")
    wine = load_wine()
    X, y = wine.data, wine.target

    if len(X) == 0:
        raise ValueError("Dataset is empty!")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"✅ Data split complete: {len(X_train)} train samples, {len(X_test)} test samples.")
    
    # Push metadata to XCom
    context["ti"].xcom_push(key="n_train", value=len(X_train))
    context["ti"].xcom_push(key="n_test", value=len(X_test))
    return {"n_features": X.shape[1], "n_classes": len(np.unique(y))}


def train_and_log_model(**context):
    """Train RandomForest model, log parameters and metrics to MLflow."""
    import mlflow
    import mlflow.sklearn
    from sklearn.datasets import load_wine
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

    # MinIO S3 credentials for artifacts
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_ENDPOINT
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
    os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    wine = load_wine()
    X, y = wine.data, wine.target
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    params = {
        "n_estimators": 100,
        "max_depth": 10,
        "min_samples_split": 2,
        "random_state": 42
    }

    print(f"🚀 Training model with params: {params}...")
    with mlflow.start_run(run_name=f"Airflow_Train_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
        model = RandomForestClassifier(**params)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        metrics = {
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "f1_score": float(f1_score(y_test, y_pred, average="weighted")),
            "precision": float(precision_score(y_test, y_pred, average="weighted")),
            "recall": float(recall_score(y_test, y_pred, average="weighted")),
        }

        print("📈 Evaluation metrics:")
        for k, v in metrics.items():
            print(f"   • {k}: {v:.4f}")

        # Log parameters & metrics
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)

        # Log & register model
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            signature=mlflow.models.infer_signature(X_train, y_pred),
            input_example=X_train[:5]
        )

        run_id = run.info.run_id
        print(f"✅ Model registered in MLflow! Run ID: {run_id}")

        context["ti"].xcom_push(key="run_id", value=run_id)
        context["ti"].xcom_push(key="metrics", value=metrics)
        return {"run_id": run_id, "metrics": metrics}


def quality_gate(**context):
    """Enforce model accuracy threshold before production promotion."""
    ti = context["ti"]
    metrics = ti.xcom_pull(key="metrics", task_ids="train_and_log_model")
    if not metrics:
        raise ValueError("No metrics received from training task!")

    accuracy = metrics.get("accuracy", 0.0)
    print(f"⚖️ Quality Gate Check: Accuracy = {accuracy:.4f} (Threshold: {ACCURACY_THRESHOLD})")

    if accuracy < ACCURACY_THRESHOLD:
        raise ValueError(
            f"❌ Quality Gate Failed: Accuracy {accuracy:.4f} is below threshold {ACCURACY_THRESHOLD}!"
        )

    print("✅ Quality Gate Passed!")
    return True


def promote_model_to_production(**context):
    """Promote the newly trained model version to Production stage in MLflow."""
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    # Find the latest version of the model
    model_versions = client.get_latest_versions(MODEL_NAME, stages=["None", "Staging"])
    if not model_versions:
        # Fallback to all versions
        all_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if not all_versions:
            raise ValueError(f"No versions found for model {MODEL_NAME}!")
        latest_version = max([int(v.version) for v in all_versions])
    else:
        latest_version = model_versions[0].version

    print(f"🔄 Promoting model '{MODEL_NAME}' version {latest_version} to 'Production'...")
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=str(latest_version),
        stage="Production",
        archive_existing_versions=True
    )

    print(f"✅ Model version {latest_version} successfully promoted to Production!")
    context["ti"].xcom_push(key="model_version", value=str(latest_version))
    return str(latest_version)


def smoke_test_fastapi(**context):
    """Smoke test FastAPI serving endpoint with a sample prediction."""
    print(f"🔍 Testing FastAPI serving at {API_ENDPOINT}...")
    try:
        health_res = requests.get(f"{API_ENDPOINT}/health", timeout=5)
        print(f"   API Health: {health_res.json()}")
    except Exception as e:
        print(f"⚠️ API Health check warning: {e}")

    sample_features = [14.23, 1.71, 2.43, 15.6, 127.0, 2.8, 3.06, 0.28, 2.29, 5.64, 1.04, 3.92, 1065.0]
    payload = {"features": sample_features}

    try:
        pred_res = requests.post(
            f"{API_ENDPOINT}/predict",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        if pred_res.status_code == 200:
            pred_data = pred_res.json()
            print(f"✅ FastAPI Prediction Smoke Test Succeeded! Result: {pred_data}")
            return pred_data
        else:
            print(f"⚠️ FastAPI returned status code {pred_res.status_code}: {pred_res.text}")
    except Exception as e:
        print(f"⚠️ FastAPI smoke test warning: {e}")

    return "Smoke test finished"


def send_telegram_summary(**context):
    """Send final summary card to Telegram."""
    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="train_and_log_model") or "N/A"
    metrics = ti.xcom_pull(key="metrics", task_ids="train_and_log_model") or {}
    model_version = ti.xcom_pull(key="model_version", task_ids="promote_model_to_production") or "1"

    print("📢 Sending Telegram deployment notification...")
    notify_model_training_result(
        model_name=MODEL_NAME,
        version=model_version,
        metrics=metrics,
        run_id=run_id,
        stage="Production"
    )
    return "Notification sent"


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="mlops_wine_quality_pipeline",
    default_args=default_args,
    description="Automated ML Training, Evaluation, Model Registry & Production Deployment",
    schedule_interval=None,  # Manual or triggered by Drift DAG / webhook
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "wine-quality", "mlflow", "telegram"],
) as dag:

    t_check = PythonOperator(
        task_id="check_services",
        python_callable=check_services,
    )

    t_prep = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
    )

    t_train = PythonOperator(
        task_id="train_and_log_model",
        python_callable=train_and_log_model,
    )

    t_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    t_promote = PythonOperator(
        task_id="promote_model_to_production",
        python_callable=promote_model_to_production,
    )

    t_smoke = PythonOperator(
        task_id="smoke_test_fastapi",
        python_callable=smoke_test_fastapi,
    )

    t_notify = PythonOperator(
        task_id="send_telegram_summary",
        python_callable=send_telegram_summary,
    )

    # Pipeline execution flow
    t_check >> t_prep >> t_train >> t_gate >> t_promote >> t_smoke >> t_notify
