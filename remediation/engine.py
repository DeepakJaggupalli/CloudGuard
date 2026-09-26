from datetime import datetime
from remediation.aws_scaling import execute_aws_remediation


AUTO_ACTIONS = {
    "reroute_traffic",
    "inspect_network",
    "restart_service",
    "clear_cache",
    "scale_compute",
    "archive_data",
    "rotate_logs",
    "optimize_query",
    "throttle_process",
    "increase_memory",
    "cleanup_disk",
    "expand_volume",
    "failover_network",
    "failover_service",
    "failover_zone",
    "migrate_vm",
    "rate_limit",
    "reduce_workload",
    "restart_auth",
    "restart_connection_pool",
    "restart_network",
    "review_rules",
    "rollback_deployment",
    "scale_memory",
    "shift_traffic",
    "terminate_process",
    "tune_connection_pool",
}


def decide(remedy, confidence, requires_human_approval, priority):
    """
    Incident Engine Decision Logic:
    Evaluates whether an anomaly is a known anomaly with an approved remediation candidate.
    """
    if requires_human_approval:
        return (
            "human_intervention",
            "Model indicates human approval is required."
        )

    if confidence < 0.50:
        return (
            "human_intervention",
            "Unknown anomaly or confidence below safety threshold (50%)."
        )

    if priority == "P1":
        return (
            "pending_approval",
            "P1 Critical incident requiring explicit operator approval before remediation."
        )

    if remedy not in AUTO_ACTIONS:
        return (
            "human_intervention",
            "Remediation action is not on the automatic allow-list."
        )

    return (
        "pending_approval",
        "Known anomaly with an approved remediation candidate."
    )


def apply_remediation(action, instance_id=None):
    """
    Executes remediation action. Triggers AWS CPU/RAM scaling or fallback simulation.
    """
    aws_result = execute_aws_remediation(action, instance_id=instance_id)

    return {
        "status": "executed",
        "action": action,
        "mode": aws_result.get("mode", "simulated_success"),
        "aws_details": aws_result.get("details", ""),
        "instance_id": aws_result.get("instance_id", "local-docker"),
        "executed_at": datetime.utcnow().isoformat() + "Z",
    }


def apply_simulated(action):
    """Backward compatibility wrapper."""
    return apply_remediation(action)
