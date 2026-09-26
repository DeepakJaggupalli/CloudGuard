# ☁ CloudGuard AI - Autonomous Anomaly Detection & AWS Auto-Scaling Infrastructure

> **⏱ Estimated Read Time**: ~4 minutes  
> **Repository Owner**: Deepak Jaggupalli  
> **Status**: 🟢 Production Ready | Docker & AWS Integrated

---

## 📖 Overview

**CloudGuard AI** is an autonomous cloud infrastructure telemetry monitoring and remediation system. It ingests live virtual machine and container telemetry via **Apache Kafka**, classifies real-time performance anomalies using **Isolation Forest (Model 1)**, prioritizes incidents (`P1`–`P4`), recommends remediations with **Random Forest (Model 2)**, and connects to **AWS EC2 & Auto Scaling** to dynamically scale CPU and RAM upon operator approval.

---

## 🔄 System Architecture & Decision Pipeline

```
        Live VM & App Data (Docker / AWS EC2 VM)
                        │
                        ▼
                Data Processing
                        │
                        ▼
            Model 1 (Isolation Forest)
                        │
                        ▼
              Incident Engine (P1–P4)
                        │
                        ▼
              Is it a Known Anomaly?
                        │
            ┌───────────┴───────────┐
            │                       │
           Yes                     No
            │                       │
            ▼                       ▼
         Model 2            Human Intervention
     (Random Forest)        (Operator resolves
            │                the new anomaly)
            ▼                       │
   Show Recommended Remedy          │
            │                       │
            ▼                       │
    User Approval (Yes / No)        │
            │                       │
      ┌─────┴─────┐                 │
      │           │                 │
     Yes         No                 │
      │           │                 │
      ▼           ▼                 │
   AI Executes   Human Intervention │
   Remediation   (Manual resolution)│
   (AWS CPU/RAM) \       /          │
                  \     /           │
                   ▼               ▼
     Store Feedback → Update Future Model Behavior
```

---

## ✨ Key Features

1. **⚡ Kafka Real-Time Telemetry Stream**: Continuous ingestion of CPU, RAM, Disk, Latency, and Packet Loss metrics over Kafka topic `cloudguard-telemetry`.
2. **🤖 Dual Machine Learning Architecture**:
   - **Model 1 (Isolation Forest)**: Unsupervised anomaly detection.
   - **Model 2 (Random Forest)**: Supervised remedy prediction & confidence estimation.
3. **🚨 Priority & Incident Engine**: Categorizes anomalies into `P1 Critical`, `P2 High`, `P3 Medium`, and `P4 Low` severity levels.
4. **☁ Live AWS EC2 & Auto-Scaling**: Integrates with AWS `boto3` SDK to dynamically resize EC2 instance types (e.g. `t3.micro` ➔ `t3.medium`) or Auto Scaling Group capacities.
5. **🎛 Human-in-the-Loop Operator Dashboard**: Modern web interface displaying real-time predictions, approval buttons (**✓ Approve & Scale** / **✕ Reject**), and high CPU load simulation tools.
6. **🔄 Continuous Model Retraining**: Stores operator feedback into SQLite database (`feedback.db`) to retrain and improve future model behavior on demand.

---

## 🛠 Technology Stack

- **Core Logic & ML**: Python 3.12/3.13, Scikit-Learn, Pandas, Joblib
- **Messaging Broker**: Apache Kafka 4.0 (Docker KRaft cluster)
- **Cloud Infrastructure**: AWS SDK (`boto3`), EC2 Instance Scaling, AWS ASG
- **Web Dashboard**: Flask, HTML5, CSS3 Glassmorphism UI
- **Containerization**: Docker, Docker Compose

---

## 🚀 Quick Start Guide

### 1. Clone & Navigate to Project Directory
```bash
git clone https://github.com/DeepakJaggupalli/CloudGuard.git
cd CloudGuard
```

### 2. Start Services with Docker Compose
```bash
docker compose up -d
```
*Spins up Apache Kafka on port `9092`, Demo Workload App on port `8080`, and Dashboard on port `5000`.*

### 3. Launch ML Pipeline & Telemetry Stream
```bash
# Terminal 1: Run AI Anomaly Pipeline
python -u ml/kafka_anomaly_pipeline.py

# Terminal 2: Run Telemetry Stream Producer
python -u simulator/telemetry_producer.py
```

### 4. Access Web Dashboard
Open **`http://localhost:5000`** in your web browser.

---

## ☁ Connecting AWS Account for Live Scaling

1. Open Dashboard at `http://localhost:5000`.
2. Click **"⚙ AWS Setup"** in the top navbar.
3. Enter your AWS Credentials:
   - **AWS Access Key ID**
   - **AWS Secret Access Key**
   - **AWS Region** (e.g., `eu-north-1` or `us-east-1`)
   - **Target EC2 Instance ID** (e.g., `i-02c4b1ea02502212e`)
4. Click **Save AWS Settings**.

---

## 📁 Repository Sitemap

```
CloudGuard/
├── aws/
│   ├── README_AWS.md             # AWS Integration & IAM Setup Guide
│   └── deploy_ec2_demo_app.sh   # Bash script to deploy workload on AWS EC2
├── dashboard/
│   ├── app.py                    # Flask Web API Server
│   └── index.html                # Real-Time Dashboard UI
├── data/
│   └── Data set/                 # Telemetry & Remedy Training Datasets
├── live-app/
│   ├── app.py                    # Demo Flask Workload App
│   └── Dockerfile                # Workload Container Dockerfile
├── ml/
│   ├── kafka_anomaly_pipeline.py # Core Anomaly & Remediation Pipeline
│   ├── features.py               # Feature vector definitions
│   ├── retrain_remedy_from_feedback.py # Retraining script
│   └── models/                   # Pre-trained Joblib ML Models
├── remediation/
│   ├── aws_scaling.py            # AWS EC2 & ASG Scaling Module
│   ├── engine.py                 # Incident Decision Engine
│   └── feedback.py               # SQLite Feedback Database Manager
├── docker-compose.yml            # Multi-container Docker configuration
├── Dockerfile.dashboard          # Dashboard Dockerfile
├── .gitignore                    # Git exclusion rules
└── README.md                     # Project documentation
```

---

## 📄 License

This project is open-source under the MIT License.
