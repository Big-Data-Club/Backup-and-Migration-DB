# Neon PostgreSQL Migration Suite

Migrate all databases from a Neon host to another Neon host, local PostgreSQL, or any target — with Airflow orchestration and a live dashboard.

## One-command production cutover (v2)

Luồng được khuyến nghị cho BDC production:

```bash
./start_migrate
```

Tool chỉ hỏi hai giá trị và ẩn nội dung khi nhập:

```text
Source URL:
Destination URL:
```

Sau đó tool tự động:

1. Kiểm tra Docker, SSH, CoreApplication và hai PostgreSQL endpoint.
2. Detect major version PostgreSQL để dùng đúng image client.
3. SSH vào K3s và scale các service ghi DB về `0`, đồng thời lưu trạng thái
   rollback của replicas, ConfigMap, Secret và production `.env`.
4. Discover **tất cả** user database từ `pg_database`; không dùng danh sách cũ
   hoặc database name nằm cuối URL làm phạm vi migration.
5. Dump dạng directory và restore song song theo từng database.
6. Verify số user table nguồn/đích cho từng database.
7. Reset `search_path`, thiết lập mặc định cho owner role và recycle backend
   session của pooler để tránh session giữ config cũ.
8. Cập nhật `CoreApplication/.env` và `CoreApplication/k3s/base/configmap.yaml`.
9. Cập nhật production `.env`, `bdc-config`, `bdc-secrets`, restart Redis và
   rollout các database consumer.
10. Chờ toàn bộ rollout Ready, chạy K3s maintenance theo ngưỡng và giữ ba bộ
    dump gần nhất.

Nếu bất kỳ bước nào lỗi sau khi production đã được quiesce, tool tự động khôi
phục ConfigMap, Secret, `.env` và replica count cũ để service quay lại source.

### Yêu cầu

- Docker đang chạy trên máy gọi lệnh.
- Có `python3` và `ssh`.
- SSH alias `bdc` truy cập được production K3s.
- `CoreApplication` nằm cạnh repository này, hoặc đặt `CORE_DIR`.
- Runner/user trên server đọc được kubeconfig và có quyền quản trị các workload
  BDC trong namespace đích.

URL phải có dạng đầy đủ và password nên được URL-encode nếu chứa ký tự đặc biệt:

```text
postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require
```

Credential không được ghi vào log hay command line của PostgreSQL client. Tool
dùng env file tạm có mode `0600`, rồi xóa sau khi kết thúc.

### Tùy chọn

```bash
./start_migrate --help
./start_migrate --version
./start_migrate --jobs 4
./start_migrate --no-cutover
./start_migrate --no-quiesce
```

- `--jobs N`: số job song song bên trong mỗi `pg_dump`/`pg_restore`.
- `--no-cutover`: chỉ migrate và cập nhật Core local, không sửa production.
- `--no-quiesce`: không dừng writer. Chỉ dùng khi chấp nhận dữ liệu có thể thay
  đổi giữa các dump.
- `--keep-runs N`: số bộ dump mới nhất được giữ lại, mặc định là `3`.

Các override dành cho môi trường khác:

| Variable | Default | Purpose |
|---|---|---|
| `CORE_DIR` | `../CoreApplication` | Đường dẫn CoreApplication |
| `K3S_SSH_TARGET` | `bdc` | SSH host/alias production |
| `K3S_REMOTE_ENV_FILE` | `/home/bdc_web/codespace/core/.env` | Production env source |
| `K3S_NAMESPACE` | `default` | Namespace application |
| `MIGRATION_JOBS` | `2` | Parallel dump/restore jobs |
| `KEEP_MIGRATION_RUNS` | `3` | Số dump run được giữ |
| `K3S_PRUNE_DISK_THRESHOLD` | `82` | Chỉ prune image khi disk đạt ngưỡng |

### Artifacts và báo cáo

Mỗi run tạo thư mục:

```text
dumps/cutover-<UTC timestamp>-<random>/
├── databases.txt
├── <database>.directory/
└── report.tsv
```

`report.tsv` chứa database, table count nguồn/đích, dung lượng dump và trạng
thái. Toàn bộ `dumps/` đã được git-ignore.

### K3s cleanup tối ưu

Core deploy gọi `scripts/k3s-maintenance.sh` trước và sau rollout:

- luôn xóa Kubernetes pod object đã `Succeeded`;
- giữ pod `Failed` để còn dữ liệu chẩn đoán;
- không prune image khi disk còn đủ;
- chỉ chạy `k3s crictl rmi --prune` sau deploy nếu disk usage đạt ngưỡng;
- không xóa PVC, K3s storage, model cache đang dùng hoặc dữ liệu database.

Điều này tránh kiểu prune unconditional khiến image vừa xóa phải được tải lại
ngay trong cùng rollout.

---

Các phần bên dưới mô tả CLI Python và Airflow cũ, vẫn được giữ để chạy migration
không gắn với production cutover.

---

## Project Structure

```
neon_migration/
├── migrate.py        ← Core migration script (standalone, no Airflow needed)
├── airflow_dag.py         ← Two Airflow DAGs: migration + hourly health-check
├── dashboard.html         ← Live migration dashboard (auto-polls every 10s)
├── dashboard_server.py    ← Flask server for dashboard API
├── docker-compose.yml     ← Full Airflow + local PostgreSQL stack
├── config.example.yaml    ← Migration config template
└── requirements.txt
```

---

## Option A: Run Script Directly (No Airflow)

### 1. Install dependencies
```bash
pip install psycopg2-binary pyyaml
# Also requires pg_dump / pg_restore (PostgreSQL client tools)
# Mac:   brew install postgresql
# Linux: apt install postgresql-client
```

### 2. Configure
```bash
cp config.example.yaml config.local.yaml
# Edit config.local.yaml — fill in source_url and target URLs
```

### 3. Run migration
```bash
# Using config file
python migrate.py --config config.local.yaml

# Or inline
python migrate.py \
  --source "postgresql://user:pass@ep-xxx.us-east-1.aws.neon.tech/postgres?sslmode=require" \
  --target "postgresql://postgres:postgres@localhost:5432/postgres" \
  --target-type local \
  --workers 3

# Migrate only specific databases
python migrate.py --config config.local.yaml --include-dbs app_db analytics

# Resume a failed run
python migrate.py --config config.local.yaml --resume
```

### 4. View dashboard
```bash
pip install flask flask-cors
python dashboard_server.py
# Open http://localhost:8081
```

---

## Option B: Run with Airflow (Recommended for Production)

### 1. Start the full stack
```bash
# Copy and edit env
cp .env.example .env
# Edit .env with your real NEON_SOURCE_URL and NEON_TARGET_URLS

docker-compose up -d

# Wait ~2 min for Airflow to initialize
docker-compose logs -f airflow-webserver
```

### 2. Access services
| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Flower (Celery) | http://localhost:5555 | — |
| Dashboard | http://localhost:8081 | — |
| Local PostgreSQL | localhost:5432 | postgres / postgres |

### 3. Configure Airflow Variables
In Airflow UI -> Admin -> Variables, add:

| Key | Value |
|---|---|
| `NEON_SOURCE_URL` | `postgresql://user:pass@ep-xxx.neon.tech/postgres?sslmode=require` |
| `NEON_TARGET_URLS` | `{"local": "postgresql://postgres:postgres@local-postgres:5432/postgres", "neon_prod": "postgresql://..."}` |
| `NEON_EXCLUDE_DBS` | `["template0", "template1", "postgres"]` |
| `NEON_INCLUDE_DBS` | `[]` (empty = all databases) |
| `NEON_PARALLEL_WORKERS` | `2` |
| `NEON_KNOWN_DBS` | `["app_db", "analytics", "users"]` (for static DAG) |
| `SLACK_WEBHOOK_URL` | `https://hooks.slack.com/...` (optional) |

### 4. Trigger migration
```bash
# Via CLI
docker-compose exec airflow-webserver \
  airflow dags trigger neon_db_migration

# Or click "Trigger DAG" in the Airflow UI
```

### 5. Copy DAGs into Airflow
```bash
mkdir -p dags
cp airflow_dag.py dags/neon_migration_dag.py
```

---

## .env.example

```env
NEON_SOURCE_URL=postgresql://user:pass@ep-quiet-moon-123456.us-east-1.aws.neon.tech/postgres?sslmode=require
NEON_TARGET_URLS={"neon_prod":"postgresql://user:pass@ep-prod.us-west-2.aws.neon.tech/postgres?sslmode=require","local":"postgresql://postgres:postgres@local-postgres:5432/postgres"}
SLACK_WEBHOOK_URL=
AIRFLOW_UID=50000
```

---

## Dashboard Features

The dashboard (`dashboard.html`) auto-refreshes every 10 seconds and shows:

- **Summary cards** — total / success / running / failed / pending database counts
- **Database table** — status badge, size, table count, duration, per-target progress bars, error messages
- **Status donut chart** — visual distribution of migration states
- **Airflow run history** — last 8 DAG runs with state and duration
- **Live log stream** — last 100 lines from the migration log file
- **Connection info panel** — source host, target host, run ID

Dashboard works in two modes:
1. **Server mode** — run `dashboard_server.py`, polls `/api/state`, `/api/runs`, `/api/logs`
2. **Static mode** — open `dashboard.html` directly, reads `migration_state.json` from same folder

---

## Migration Flow

```
Source Neon host
    │
    ├── discover all databases
    │
    ├── for each database (parallel, N workers):
    │       │
    │       ├── pg_dump --format=custom -> /tmp/neon_dumps/*.dump
    │       │
    │       ├── for each target:
    │       │       ├── CREATE DATABASE IF NOT EXISTS
    │       │       └── pg_restore --clean --if-exists
    │       │
    │       └── verify: table count source == target
    │
    └── report (console + Slack webhook + Airflow XCom)
```

---

## Troubleshooting

**`pg_dump: error: connection to server failed: SSL required`**
-> Add `?sslmode=require` to your Neon URL.

**`pg_restore: error: could not connect to server`**
-> Check local PostgreSQL is running: `docker-compose ps local-postgres`

**Verification fails (table count mismatch)**
-> Usually caused by views or materialized views counted differently.
  Use `--no-verify` flag or check if source has pending migrations.

**Airflow DAG not appearing**
-> Ensure `airflow_dag.py` is in the `dags/` volume and syntax is valid:
  `python -c "import ast; ast.parse(open('airflow_dag.py').read()); print('OK')"`
