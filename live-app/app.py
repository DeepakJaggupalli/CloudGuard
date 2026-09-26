from flask import Flask, jsonify
import os
import socket
import time

app = Flask(__name__)


@app.get("/")
def home():
    return jsonify({
        "application": "CloudGuard Demo Application",
        "status": "running",
        "hostname": socket.gethostname(),
        "message": "Live workload monitored by CloudGuard"
    })


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": time.time()
    })


@app.get("/api/data")
def data():
    return jsonify({
        "service": "demo-api",
        "status": "running",
        "environment": "docker"
    })


@app.get("/load")
def load():
    """
    CPU-load endpoint for CloudGuard testing.
    It intentionally consumes CPU for a short period.
    """
    start = time.time()

    while time.time() - start < 5:
        pass

    return jsonify({
        "message": "CPU load generated",
        "duration_seconds": 5
    })


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=8080
    )
