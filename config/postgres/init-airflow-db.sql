-- ============================================
-- INITIALIZE AIRFLOW DATABASE IN POSTGRESQL
-- ============================================

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

GRANT ALL PRIVILEGES ON DATABASE airflow TO mlflow;
