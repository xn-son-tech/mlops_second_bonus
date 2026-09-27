# 🚀 Hướng Dẫn Tích Hợp Airflow & Telegram Notification Cho ML Monitoring

Tài liệu này hướng dẫn chi tiết cách cấu hình và vận hành hệ thống điều phối **Apache Airflow** kết hợp cảnh báo tự động qua **Telegram** trong dự án ML Monitoring.

---

## 📋 1. Tổng Quan Kiến Trúc MLOps

```
               ┌────────────────────────────────────────────────────────┐
               │              Apache Airflow Orchestration              │
               │                                                        │
               │  ┌───────────────────────┐  ┌───────────────────────┐  │
               │  │  Wine Quality Train   │  │   Drift Monitoring    │  │
               │  │       Pipeline        │  │       Pipeline        │  │
               │  └──────────┬────────────┘  └───────────┬───────────┘  │
               └─────────────┼───────────────────────────┼──────────────┘
                             │                           │
              ┌──────────────┴───────────────┐           │ (Trigger /analyze)
              │                              │           ▼
              ▼                              ▼   ┌───────────────┐
       ┌──────────────┐              ┌───────────┤ Evidently AI  │
       │ MLFlow Server│              │  FastAPI  │    :8001      │
       │    :5000     │              │   :8000   └───────┬───────┘
       └──────┬───────┘              └─────┬─────┘       │ (Drift detected?)
              │                            │             │
              ▼                            ▼             ▼
       ┌──────────────┐              ┌───────────┐ ┌─────────────┐
       │ MinIO S3     │              │Prometheus │ │  Telegram   │
       │    :9000     │              │   :9090   │ │  Alert Bot  │
       └──────────────┘              └─────┬─────┘ └─────────────┘
                                           │
                                           ▼
                                     ┌───────────┐
                                     │  Grafana  │
                                     │   :3000   │
                                     └───────────┘
```

### Các DAG MLOps Được Cung Cấp:
1. **`mlops_wine_quality_pipeline`**:
   - Kiểm tra trạng thái MLflow và MinIO.
   - Tải dataset Wine, kiểm tra schema và phân chia tập dữ liệu.
   - Huấn luyện mô hình RandomForest, tính toán Accuracy, F1, Precision, Recall và ghi log vào MLflow Tracking.
   - **Quality Gate**: Kiểm tra ngưỡng Accuracy >= 0.85 (tự động báo lỗi nếu không đạt).
   - **Model Promotion**: Đăng ký và chuyển model sang stage `Production` trong MLflow Model Registry.
   - Smoke test kiểm tra suy luận qua API FastAPI `/predict`.
   - **Gửi tin nhắn tổng kết sang Telegram** (kèm Run ID, Version, Accuracy, F1 score).

2. **`mlops_drift_monitoring_pipeline`**:
   - Kiểm tra trạng thái dịch vụ Evidently AI.
   - Kích hoạt phân tích Data Drift (`POST /analyze`).
   - Đọc kết quả drift score và danh sách feature bị lệch phân phối.
   - **Nếu phát hiện Data Drift**:
     - Bắn cảnh báo khẩn cấp sang Telegram kèm danh sách feature bị lệch và link xem báo cáo HTML.
     - **Tự động kích hoạt (Auto-trigger)** DAG `mlops_wine_quality_pipeline` để huấn luyện lại mô hình!
   - **Nếu bình thường**: Ghi nhận trạng thái ổn định.

---

## 🤖 2. Hướng Dẫn Cấu Hình Telegram Bot (Trong 2 Phút)

### Bước 2.1: Tạo Bot Telegram
1. Mở ứng dụng Telegram, tìm kiếm user: **`@BotFather`**.
2. Gửi lệnh `/start`, sau đó gửi `/newbot`.
3. Đặt tên hiển thị cho bot và username (kết thúc bằng chữ `bot`, ví dụ: `my_mlops_alert_bot`).
4. **Copy mã Token API** được cấp (dạng: `7123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ`).

### Bước 2.2: Lấy Chat ID của Bạn (hoặc Group)
1. Trong Telegram, tìm kiếm bot: **`@userinfobot`** (hoặc **`@GetMyChatID_Bot`**).
2. Nhấn `/start`, bot sẽ phản hồi thông tin của bạn kèm dòng **`Id: 123456789`** (đây chính là `CHAT_ID`).
3. **QUAN TRỌNG:** Quay lại cuộc trò chuyện với **Bot mà bạn vừa tạo ở Bước 2.1** và nhấn nút **`/start`** (hoặc gửi 1 tin nhắn bất kỳ cho nó) để cho phép bot gửi tin nhắn tới bạn!

### Bước 2.3: Điền Vào File `.env`
Mở file `.env` và cập nhật 2 dòng:
```ini
TELEGRAM_BOT_TOKEN=7123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ
TELEGRAM_CHAT_ID=123456789
TELEGRAM_NOTIFY_ON_SUCCESS=false
TELEGRAM_NOTIFY_ON_FAILURE=true
```

### Bước 2.4: Kiểm Tra Kết Nối Ngay Lập Tức
Chạy lệnh test có sẵn:
```bash
python scripts/test_telegram.py
```
Nếu cấu hình đúng, bạn sẽ nhận được một tin nhắn kiểm tra màu xanh gửi thẳng về Telegram của mình! 🎉

---

## 🐳 3. Khởi Động Toàn Bộ Hệ Thống Với Docker Compose

```bash
# 1. Khởi động Docker Desktop trên máy nếu chưa chạy

# 2. Build và khởi động toàn bộ cụm dịch vụ (bao gồm Airflow):
docker compose up -d --build
```

### Danh Sách Cổng Truy Cập:
| Dịch vụ | Địa chỉ Web UI | Tài khoản mặc định | Mô tả |
| :--- | :--- | :--- | :--- |
| **Airflow Webserver** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` | Giao diện điều phối pipeline MLOps |
| **MLflow UI** | [http://localhost:5000](http://localhost:5000) | *Không cần* | Quản lý thí nghiệm và Model Registry |
| **FastAPI Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | *Không cần* | API phục vụ suy luận mô hình |
| **Evidently Reports** | [http://localhost:8001/reports](http://localhost:8001/reports) | *Không cần* | Báo cáo kiểm định Data Drift |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | Dashboard giám sát thời gian thực |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | *Không cần* | Lưu trữ số liệu giám sát |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minio` / `minio123` | Quản trị lưu trữ S3 Artifacts |

---

## 🎮 4. Thao Tác Chạy DAG Trên Airflow

1. Mở trình duyệt vào [http://localhost:8080](http://localhost:8080) và đăng nhập với `admin` / `admin`.
2. Bật công tắc (Toggle) xanh cho DAG:
   - `mlops_wine_quality_pipeline`
   - `mlops_drift_monitoring_pipeline`
3. Nhấn nút **Trigger DAG** (biểu tượng Play ▶️) ở góc phải của `mlops_wine_quality_pipeline`.
4. Quan sát các task chạy tuần tự:
   `check_services` ➔ `prepare_data` ➔ `train_and_log_model` ➔ `quality_gate` ➔ `promote_model_to_production` ➔ `smoke_test_fastapi` ➔ `send_telegram_summary`.
5. Ngay khi hoàn tất, mở điện thoại / Telegram lên để nhận thông báo kết quả huấn luyện kèm các chỉ số F1, Accuracy, Run ID!

---

## 🛠 5. Khắc Phục Sự Cố Thường Gặp (Troubleshooting)

- **Lỗi Telegram: "Unauthorized / Bot Token is invalid"**:
  - Kiểm tra lại xem mã token trong `.env` có bị thừa khoảng trắng hoặc ký tự ngoặc kép hay không.
- **Lỗi Telegram: "Chat not found / Forbidden: bot was blocked by the user"**:
  - Bạn chưa bấm `/start` với bot của mình. Hãy tìm bot trên Telegram và gửi `/start`.
- **Airflow Webserver báo lỗi kết nối Database**:
  - Service `airflow-init` sẽ tự động khởi tạo database `airflow` và chạy migrate. Bạn có thể kiểm tra log bằng:
    `docker compose logs -f airflow-init`
