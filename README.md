# ☁ CloudGuard AI - Autonomous Telemetry Anomaly Detection & AWS Scaling Engine

> **⏱ Estimated Read Time**: ~6 minutes  
> **Author / Maintainer**: Deepak Jaggupalli  
> **Repository**: [https://github.com/DeepakJaggupalli/CloudGuard](https://github.com/DeepakJaggupalli/CloudGuard)  
> **License**: MIT  
> **Status**: 🟢 Production Ready | Docker Containerized | AWS EC2 & ASG Integrated

---

## 📋 Table of Contents
1. [Executive Summary](#-executive-summary)
2. [End-to-End Decision Flowchart](#-end-to-end-decision-flowchart)
3. [System Architecture](#-system-architecture)
4. [Machine Learning Pipeline](#-machine-learning-pipeline)
5. [AWS Cloud Integration & Auto-Scaling](#-aws-cloud-integration--auto-scaling)
6. [Kafka Messaging Architecture](#-kafka-messaging-architecture)
7. [REST API Documentation](#-rest-api-documentation)
8. [Quick Start & Installation Guide](#-quick-start--installation-guide)
9. [Directory Structure & Sitemap](#-directory-structure--sitemap)
10. [Troubleshooting & FAQ](#-troubleshooting--faq)

---

## 📖 Executive Summary

**CloudGuard AI** is an autonomous cloud infrastructure monitoring, anomaly classification, and automated remediation platform. It ingests live telemetry metrics (CPU, RAM, Disk, Latency, Packet Loss) from containerized workloads and AWS Virtual Machines via **Apache Kafka**.

The platform features a **Dual-Model Machine Learning Pipeline**:
- **Model 1 (Isolation Forest)**: Unsupervised classification of real-time telemetry into Normal vs. Anomaly.
- **Incident Engine**: Categorizes anomalies into priority tiers (`P1 Critical`, `P2 High`, `P3 Medium`, `P4 Low`).
- **Known Anomaly Evaluator**: Verifies pattern confidence before recommendation.
- **Model 2 (Random Forest)**: Recommends precise remediation actions (e.g. `scale_compute`, `scale_memory`).
- **Human-in-the-Loop Approval Workflow**: Operator reviews recommendations on an interactive web dashboard.
- **AWS Scaling Engine**: Executes live AWS EC2 instance type modifications (`t3.micro` ➔ `t3.medium`) or Auto Scaling Group (ASG) capacity adjustments via AWS `boto3` SDK upon operator approval.
- **Continuous Feedback Loop**: Stores operator decisions in an SQLite database (`feedback.db`) to retrain and update future model behavior.

---

## 🔄 End-to-End Decision Flowchart

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

## 🏗 System Architecture

```
 ┌─────────────────────────────────────────────────────────────┐
 │ 🐳 LOCAL DOCKER CONTAINER ENVIRONMENT                        │
 │  • Apache Kafka Broker (Port 9092)                         │
 │  • Demo Workload Application (Port 8080)                    │
 │  • CloudGuard Web Dashboard (Port 5000)                     │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                                │ (Consumes cloudguard-telemetry)
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ 🧠 MACHINE LEARNING PIPELINE                                │
 │  • Feature Scaler & Normalizer                              │
 │  • Model 1: Isolation Forest (Anomaly Score & Flag)         │
 │  • Priority Engine (P1 Critical to P4 Normal)               │
 │  • Model 2: Random Forest (Remedy Prediction & Confidence)  │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                                │ (AWS API Call on Approval)
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ ☁ AWS CLOUD INFRASTRUCTURE (eu-north-1 / us-east-1)         │
 │  • EC2 Instance: i-02c4b1ea02502212e                       │
 │  • Action: Dynamically scales CPU/RAM (t3.micro ➔ t3.medium)│
 └─────────────────────────────────────────────────────────────┘
```

---

## 🧠 Machine Learning Pipeline

### Model 1: Isolation Forest (Anomaly Detection)
- **Algorithm**: `sklearn.ensemble.IsolationForest`
- **Features Used**: `cpu_utilization_pct`, `memory_utilization_pct`, `disk_utilization_pct`, `network_in_mbps`, `network_out_mbps`, `disk_iops`, `latency_ms`, `packet_loss_pct`, `temperature_c`, `uptime_minutes`.
- **Output**: `-1` (Anomaly) or `1` (Normal telemetry), plus continuous decision function anomaly score.

### Incident Priority Classifier
- **P1 Critical**: CPU >= 90% OR Memory >= 95% OR Packet Loss >= 10% OR Latency >= 250ms.
- **P2 High**: CPU >= 75% OR Memory >= 85% OR Packet Loss >= 3% OR Latency >= 150ms.
- **P3 Medium**: Anomaly flagged with moderate metric spikes.
- **P4 Low / Normal**: Standard baseline performance.

### Model 2: Random Forest Classifier (Remedy Recommendation)
- **Algorithm**: `sklearn.ensemble.RandomForestClassifier` (300 Estimators)
- **Input Vector**: `event_type`, `event_category`, `severity`
- **Target Label**: Recommended action (`scale_compute`, `scale_memory`, `restart_service`, `clear_cache`, `reroute_traffic`, etc.)
- **Confidence Cutoff**: >= 50% confidence classifies as **Known Anomaly**; below 50% classifies as **Unknown Anomaly** requiring direct **Human Intervention**.

---

## ☁ AWS Cloud Integration & Auto-Scaling

CloudGuard AI integrates directly with **AWS EC2** and **AWS Auto Scaling Groups (ASG)** via the `boto3` SDK.

### Required IAM Policy Permissions

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudGuardEC2Scaling",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeInstances",
                "ec2:ModifyInstanceAttribute",
                "ec2:StartInstances",
                "ec2:StopInstances"
            ],
            "Resource": "*"
        },
        {
            "Sid": "CloudGuardASGScaling",
            "Effect": "Allow",
            "Action": [
                "autoscaling:DescribeAutoScalingGroups",
                "autoscaling:SetDesiredCapacity"
            ],
            "Resource": "*"
        }
    ]
}
```

### AWS Credentials Configuration
Credentials can be set dynamically in the Web UI (`http://localhost:5000` ➔ **"⚙ AWS Setup"**) or configured in `remediation/aws_config.json`:

```json
{
  "enabled": true,
  "aws_access_key_id": "YOUR_AWS_ACCESS_KEY",
  "aws_secret_access_key": "YOUR_AWS_SECRET_KEY",
  "aws_region": "eu-north-1",
  "instance_id": "i-02c4b1ea02502212e",
  "current_instance_type": "t3.micro",
  "scaled_instance_type": "t3.medium"
}
```

---

## 📬 Kafka Messaging Architecture

| Topic Name | Purpose | Producer | Consumer |
| :--- | :--- | :--- | :--- |
| **`cloudguard-telemetry`** | Ingests real-time container/VM metrics | `live_telemetry.py` | `kafka_anomaly_pipeline.py` |
| **`cloudguard-predictions`** | Transmits AI model outputs and decisions | `kafka_anomaly_pipeline.py` | `dashboard/app.py` |
| **`cloudguard-remediation`** | Transmits anomaly alerts for user action | `kafka_anomaly_pipeline.py` | Dashboard UI |

---

## 🔌 REST API Documentation

### 1. Telemetry Stream & Predictions
* **`GET /api/predictions`**
  - **Description**: Returns chronological list of evaluated telemetry events.
  - **Response**: Array of prediction objects (Event ID, VM ID, CPU, Memory, Model 1 Status, Priority, Remedy, Confidence, Approval State).

### 2. Dashboard Statistics
* **`GET /api/stats`**
  - **Description**: Summary metrics of total events, anomalies, known vs. unknown counts, and priority distribution (`P1`–`P4`).

### 3. Operator Approval (Execute AI Remediation)
* **`POST /api/remediation/approve`**
  - **Body**: `{"event_id": "UUID", "operator_note": "Approved"}`
  - **Description**: Executes AWS EC2 CPU/RAM scaling or container scaling, logs feedback to database.

### 4. Operator Rejection (Human Intervention)
* **`POST /api/remediation/reject`**
  - **Body**: `{"event_id": "UUID", "operator_note": "Rejected"}`
  - **Description**: Marks incident for manual human intervention, logs rejection to database.

### 5. Retrain Machine Learning Model
* **`POST /api/retrain`**
  - **Description**: Triggers online model retraining of Model 2 (Random Forest) incorporating newly stored operator feedback.

### 6. AWS Configuration
* **`GET /api/aws/config`** & **`POST /api/aws/config`**
  - **Description**: View or update AWS Access Keys, Region, EC2 Instance ID, and Scaled Instance Type.

### 7. Trigger Load Simulation
* **`POST /api/demo/trigger-load`**
  - **Description**: Generates an artificial CPU load spike on the target workload for live anomaly testing.

---

## 🚀 Quick Start & Installation Guide

### Prerequisites
- Python 3.12+
- Docker & Docker Desktop
- AWS Account with an EC2 Instance

### Step 1: Clone Repository & Navigate
```bash
git clone https://github.com/DeepakJaggupalli/CloudGuard.git
cd CloudGuard
```

### Step 2: Start Containers with Docker Compose
```bash
docker compose up -d
```

### Step 3: Run the Machine Learning Pipeline
```bash
python -u ml/kafka_anomaly_pipeline.py
```

### Step 4: Run Live Real-Time Telemetry Streaming
```bash
python -u simulator/telemetry_producer.py
```

### Step 5: Open Web Dashboard
Open **[`http://localhost:5000`](http://localhost:5000)** in your browser.

---

## 📁 Directory Structure & Sitemap

```
CloudGuard/
├── aws/
│   ├── README_AWS.md             # AWS Integration & IAM Setup Guide
│   └── deploy_ec2_demo_app.sh   # Bash deployment script for AWS EC2 instance
├── dashboard/
│   ├── app.py                    # Flask Web API & Kafka listener server
│   └── index.html                # Real-Time Glassmorphism UI Dashboard
├── data/
│   └── Data set/                 # Datasets for model training & evaluation
├── live-app/
│   ├── app.py                    # Demo Workload Flask Application
│   └── Dockerfile                # Docker container build script for workload
├── ml/
│   ├── kafka_anomaly_pipeline.py # Core Dual-Model Anomaly & Remediation Pipeline
│   ├── features.py               # ML feature definitions
│   ├── retrain_remedy_from_feedback.py # Online model retraining module
│   └── models/                   # Joblib serializations of ML models
├── remediation/
│   ├── aws_scaling.py            # AWS EC2 & ASG Boto3 Scaling Engine
│   ├── engine.py                 # Incident Decision Engine
│   └── feedback.py               # SQLite Feedback Database Operations
├── docker-compose.yml            # Multi-container Docker configuration
├── Dockerfile.dashboard          # Dashboard Docker container configuration
├── .gitignore                    # Git version control ignore rules
└── README.md                     # Comprehensive project documentation
```

---

## ❓ Troubleshooting & FAQ

#### Q: How do I know if Kafka is running?
Run `docker ps`. You should see `cloudguard-kafka` running on port `9092`.

#### Q: What happens if I haven't entered my AWS Keys yet?
CloudGuard AI automatically runs in **AWS Simulation Mode**. You can view full audit logs, and when you enter your keys on the UI, it seamlessly switches to **Live AWS Mode**.

#### Q: How do I reset the Docker environment cleanly?
Run:
```bash
docker compose down
docker compose up -d
```

---

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.
