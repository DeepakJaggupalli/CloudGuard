# ☁ CloudGuard AI - AWS Account Integration & Scaling Setup Guide

CloudGuard AI connects to your **AWS Account** to monitor EC2 instances and automatically execute **CPU and RAM scaling** upon user approval when anomalies are detected.

---

## 🚀 Quick Setup Instructions

### 1. Requirements on AWS
- An AWS Account.
- An EC2 Instance (e.g., `t3.micro`) running the demo application or workload.
- IAM User / Credentials with permissions to modify EC2 attributes and scale Auto Scaling Groups.

---

## 🔑 IAM Policy Permissions Required

Attach the following policy to your IAM User or IAM Role for CloudGuard AI:

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

---

## ⚙ Connecting AWS Account in CloudGuard AI

1. Open the CloudGuard Dashboard at `http://localhost:5000`.
2. Click the **"⚙ AWS Setup"** button in the header navbar.
3. Enter your AWS Details:
   - **AWS Access Key ID**
   - **AWS Secret Access Key**
   - **AWS Region** (e.g. `us-east-1` or `ap-south-1`)
   - **Target EC2 Instance ID** (e.g. `i-0a1b2c3d4e5f67890`)
   - **Scaled Instance Type** (e.g. `t3.medium` - 2 vCPU, 4GB RAM)
4. Click **"Save AWS Settings"**.
5. CloudGuard AI will now automatically route remediation approvals to real **AWS EC2 Scaling**!

---

## 🔄 Automated Workflow Diagram

```
Live VM & App Data
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
 ┌──────┴──────┐
 │             │
Yes           No
 │             │
 ▼             ▼
Model 2      Human Intervention
(Random      (Operator resolves
 Forest)      the new anomaly)
 │             │
 ▼             │
Show Recommended Remedy
 │
 ▼
User Approval (Yes / No)
 │
 ┌──────┴──────┐
 │             │
Yes           No
 │             │
 ▼             ▼
AI Executes   Human Intervention
Remediation   (Manual resolution)
      \       /
       \     /
        ▼
 Store Feedback → Update Future Model Behavior
```
