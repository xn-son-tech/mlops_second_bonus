"""
=============================================================================
TELEGRAM NOTIFIER MODULE FOR AIRFLOW & MLOPS MONITORING
=============================================================================
Provides automated alerts to Telegram for:
- Task & DAG failures
- Model training & evaluation results
- Data drift detection & Evidently analysis reports
- Pipeline completion status
"""

import os
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Fallback Airflow Variable import (works both inside and outside Airflow)
try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    Variable = None
    AIRFLOW_AVAILABLE = False


def get_telegram_config() -> tuple[Optional[str], Optional[str]]:
    """
    Get Telegram Bot Token and Chat ID from Airflow Variables or Environment Variables.
    Priority: Airflow Variables > Environment Variables
    """
    bot_token = None
    chat_id = None

    if AIRFLOW_AVAILABLE and Variable:
        try:
            bot_token = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
            chat_id = Variable.get("TELEGRAM_CHAT_ID", default_var=None)
        except Exception:
            pass

    if not bot_token:
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not chat_id:
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

    return bot_token, chat_id


def send_telegram_message(
    message: str,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True
) -> bool:
    """
    Send a message to the configured Telegram chat via Telegram Bot API.
    
    Args:
        message: The message body (HTML formatted)
        parse_mode: 'HTML' or 'Markdown'
        disable_web_page_preview: Whether to disable URL previews
    
    Returns:
        bool: True if sent successfully, False otherwise
    """
    bot_token, chat_id = get_telegram_config()

    if not bot_token or not chat_id:
        logger.warning(
            "⚠️ Telegram notification skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not configured."
        )
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview
    }

    try:
        import requests
        response = requests.post(url, json=payload, timeout=10)
        result = response.json()
        if response.status_code == 200 and result.get("ok"):
            logger.info("✅ Telegram notification sent successfully.")
            return True
        else:
            logger.error(f"❌ Telegram API error: {result.get('description', response.text)}")
            return False
    except Exception as e:
        logger.error(f"❌ Failed to send Telegram notification: {e}")
        return False


# =============================================================================
# AIRFLOW CALLBACK HANDLERS
# =============================================================================

def on_task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Callback invoked automatically by Airflow when any task fails.
    """
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else (ti.dag_id if ti else "Unknown DAG")
    task_id = ti.task_id if ti else "Unknown Task"
    exec_date = context.get("logical_date") or context.get("execution_date") or datetime.now()
    exception = context.get("exception")
    log_url = ti.log_url if ti else "N/A"
    try_number = ti.try_number if ti else 1

    error_snippet = ""
    if exception:
        error_lines = str(exception).strip().splitlines()
        error_snippet = error_lines[-1] if error_lines else str(exception)
        if len(error_snippet) > 300:
            error_snippet = error_snippet[:297] + "..."

    message = (
        f"🚨 <b>[AIRFLOW TASK FAILED]</b> 🚨\n\n"
        f"▫️ <b>DAG:</b> <code>{dag_id}</code>\n"
        f"▫️ <b>Task:</b> <code>{task_id}</code> (Attempt #{try_number})\n"
        f"▫️ <b>Execution Date:</b> {exec_date.strftime('%Y-%m-%d %H:%M:%S UTC') if hasattr(exec_date, 'strftime') else exec_date}\n"
        f"▫️ <b>Log URL:</b> <a href=\"{log_url}\">View Logs</a>\n"
    )

    if error_snippet:
        message += f"\n❌ <b>Error:</b>\n<pre>{error_snippet}</pre>\n"

    message += f"\n<i>System: ML Monitoring Platform</i>"
    send_telegram_message(message)


def on_task_success_callback(context: Dict[str, Any]) -> None:
    """
    Callback invoked automatically by Airflow when a task succeeds (if enabled).
    """
    notify_on_success = os.getenv("TELEGRAM_NOTIFY_ON_SUCCESS", "false").lower() == "true"
    if not notify_on_success:
        return

    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else (ti.dag_id if ti else "Unknown DAG")
    task_id = ti.task_id if ti else "Unknown Task"
    duration = f"{ti.duration:.2f}s" if (ti and ti.duration) else "N/A"

    message = (
        f"✅ <b>[AIRFLOW TASK SUCCESS]</b>\n\n"
        f"▫️ <b>DAG:</b> <code>{dag_id}</code>\n"
        f"▫️ <b>Task:</b> <code>{task_id}</code>\n"
        f"▫️ <b>Duration:</b> {duration}\n"
        f"▫️ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    send_telegram_message(message)


# =============================================================================
# MLOPS NOTIFICATION HELPERS
# =============================================================================

def notify_model_training_result(
    model_name: str,
    version: Any,
    metrics: Dict[str, float],
    run_id: str,
    stage: str = "Production"
) -> bool:
    """
    Send rich notification when a new model has been trained and promoted to MLFlow.
    """
    metrics_str = ""
    for k, v in metrics.items():
        formatted_val = f"{v:.4f}" if isinstance(v, (float, int)) else str(v)
        metrics_str += f"   • <b>{k.capitalize()}:</b> <code>{formatted_val}</code>\n"

    message = (
        f"🚀 <b>[NEW MODEL DEPLOYED TO {stage.upper()}]</b>\n\n"
        f"📦 <b>Model Name:</b> <code>{model_name}</code>\n"
        f"🏷 <b>Version:</b> <code>v{version}</code>\n"
        f"🎯 <b>Stage:</b> <b>{stage}</b>\n"
        f"🆔 <b>MLflow Run ID:</b> <code>{run_id[:10]}...</code>\n\n"
        f"📊 <b>Evaluation Metrics:</b>\n{metrics_str}\n"
        f"⏰ <b>Timestamp:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🌐 <b>MLflow UI:</b> <a href=\"http://localhost:5000\">Open Registry</a>"
    )
    return send_telegram_message(message)


def notify_drift_alert(
    drift_score: float,
    drifted_features: List[str],
    total_features: int,
    report_name: Optional[str] = None
) -> bool:
    """
    Send high-priority warning when Data Drift is detected by Evidently AI.
    """
    features_list = "\n".join([f"   ⚠️ <code>{f}</code>" for f in drifted_features])
    
    report_link = f"http://localhost:8001/reports/{report_name}" if report_name else "http://localhost:8001/reports"

    message = (
        f"⚠️ <b>[ALERT: DATA DRIFT DETECTED]</b> ⚠️\n\n"
        f"Evidently AI has detected significant feature distribution drift in production!\n\n"
        f"▫️ <b>Drift Score:</b> <code>{drift_score:.4f}</code>\n"
        f"▫️ <b>Drifted Features:</b> {len(drifted_features)} / {total_features}\n"
        f"▫️ <b>Drifted List:</b>\n{features_list}\n\n"
        f"📊 <b>Full HTML Report:</b> <a href=\"{report_link}\">View Report</a>\n"
        f"🔄 <i>Auto-retraining pipeline is being triggered...</i>\n"
        f"⏰ <b>Timestamp:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    return send_telegram_message(message)


def notify_drift_healthy(drift_score: float, total_features: int) -> bool:
    """
    Send informational status when drift check passes without drift.
    """
    message = (
        f"🟢 <b>[DATA DRIFT CHECK: HEALTHY]</b>\n\n"
        f"▫️ <b>Drift Score:</b> <code>{drift_score:.4f}</code> (Below Threshold)\n"
        f"▫️ <b>Total Features Analyzed:</b> {total_features}\n"
        f"▫️ <b>Status:</b> All feature distributions are stable.\n"
        f"⏰ <b>Timestamp:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    return send_telegram_message(message)
