#!/bin/bash
# ==============================================================================
# CloudGuard AI - AWS EC2 VM Demo App & Telemetry Agent Installer
# ==============================================================================
# Run this script on an AWS EC2 instance (Amazon Linux 2023 or Ubuntu 22.04/24.04)
# to deploy the demo application and telemetry monitoring.
# ==============================================================================

set -e

echo "=================================================================="
echo "Installing CloudGuard Demo Application on AWS EC2 Instance"
echo "=================================================================="

# Update packages
if command -v apt-get &> /dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y python3 python3-pip git docker.io
    sudo systemctl start docker
    sudo systemctl enable docker
elif command -v dnf &> /dev/null; then
    sudo dnf update -y
    sudo dnf install -y python3 python3-pip git docker
    sudo systemctl start docker
    sudo systemctl enable docker
fi

# Clone or prepare application directory
APP_DIR="/opt/cloudguard-demo"
sudo mkdir -p $APP_DIR
sudo chown -R $USER:$USER $APP_DIR

cat << 'EOF' > $APP_DIR/app.py
from flask import Flask, jsonify
import os, socket, time

app = Flask(__name__)

@app.get("/")
def home():
    return jsonify({
        "application": "CloudGuard AWS EC2 Demo Application",
        "status": "running",
        "hostname": socket.gethostname(),
        "message": "Live workload monitored by CloudGuard AI on AWS EC2"
    })

@app.get("/health")
def health():
    return jsonify({"status": "healthy", "timestamp": time.time()})

@app.get("/load")
def load():
    start = time.time()
    while time.time() - start < 5:
        pass
    return jsonify({"message": "CPU load generated on AWS EC2", "duration_seconds": 5})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
EOF

pip3 install flask requests

# Create systemd service for app
cat << EOF | sudo tee /etc/systemd/system/cloudguard-demo.service
[Unit]
Description=CloudGuard Demo Application Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/python3 $APP_DIR/app.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable cloudguard-demo
sudo systemctl start cloudguard-demo

echo "=================================================================="
echo "CloudGuard Demo App successfully deployed on AWS EC2!"
echo "HTTP Port: 8080"
echo "Health Check: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):8080/health"
echo "=================================================================="
