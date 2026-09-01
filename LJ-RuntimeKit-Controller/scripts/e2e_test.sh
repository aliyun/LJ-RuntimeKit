#!/bin/bash
# E2E test for LJ-RuntimeKit-Controller
# Tests enable/disable via node labels on two nodes.
set -o pipefail

NAMESPACE="lj-runtimekit-system"
EVENT_NS="default"
LABEL_KEY="lj_runtimekit_enable"
TARGET_LINK="/usr/bin/nvidia-container-runtime"
TARGET_DEST="/usr/local/harp/kitC/xpu-container-runtime"
SSH_PASS="001027Aa@"
SSH_USER="zhangdanyang.zdy"

NODE1="t09c06224.cloud.sqa.na131"
NODE1_IP="33.254.24.200"
NODE2="w32g01140.cloud.sqa.na131"
NODE2_IP="33.254.64.76"

JOB_IMAGE="docker.io/library/lj-runtimekit-controller-job-agent:v0.1.9"
TAR_FILE="/home/zhangdanyang.zdy/lj-runtimekit-controller-job-agent.tar"

TOTAL=0
PASS=0
FAIL=0

RESULT_LOG="/home/zhangdanyang.zdy/amp_test_results.log"
DETAIL_LOG="/home/zhangdanyang.zdy/amp_test_details.log"

echo "=== AMP E2E Test Started at $(date -Iseconds) ===" | tee -a "$RESULT_LOG" "$DETAIL_LOG"

log_detail() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$DETAIL_LOG"
}

log_result() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$RESULT_LOG"
}

node_ip() {
    local node="$1"
    if [ "$node" = "$NODE1" ]; then echo "$NODE1_IP"; else echo "$NODE2_IP"; fi
}

ensure_image() {
    local node="$1"
    local ip
    ip=$(node_ip "$node")
    log_detail "Ensuring job-agent image on $node ($ip)"
    local has_img
    has_img=$(sshpass -p "$SSH_PASS" ssh -T -o StrictHostKeyChecking=no "${SSH_USER}@${ip}" "echo '$SSH_PASS' | sudo -S ctr -n k8s.io images ls | grep '${JOB_IMAGE}' | wc -l" 2>/dev/null)
    if [ "${has_img:-0}" -eq 0 ]; then
        log_detail "Image not present on $node, importing $TAR_FILE"
        sshpass -p "$SSH_PASS" ssh -T -o StrictHostKeyChecking=no "${SSH_USER}@${ip}" "echo '$SSH_PASS' | sudo -S ctr -n k8s.io image import '$TAR_FILE'" >> "$DETAIL_LOG" 2>&1
    else
        log_detail "Image already present on $node"
    fi
}

get_label() {
    local node="$1"
    kubectl get node "$node" -o jsonpath="{.metadata.labels.${LABEL_KEY}}" 2>/dev/null
}

set_label() {
    local node="$1"
    local value="$2"
    if [ -z "$value" ]; then
        kubectl label node "$node" "${LABEL_KEY}-" --overwrite
    else
        kubectl label node "$node" "${LABEL_KEY}=${value}" --overwrite
    fi
}

wait_for_event() {
    local node="$1"
    local action="$2"
    local timeout=300
    local elapsed=0
    log_detail "Waiting for ActionSucceeded event on $node action=$action"
    while [ "$elapsed" -lt "$timeout" ]; do
        local found
        found=$(kubectl get events -n "$EVENT_NS" --field-selector reason=ActionSucceeded \
            -o custom-columns=NODE:.involvedObject.name,MSG:.message --no-headers 2>/dev/null | \
            grep "${node}.*AMP action \"${action}\"" | tail -1)
        if [ -n "$found" ]; then
            echo "$found"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
        log_detail "  waited ${elapsed}s for event..."
    done
    return 1
}

check_symlink() {
    local node="$1"
    local ip
    ip=$(node_ip "$node")
    local out
    out=$(sshpass -p "$SSH_PASS" ssh -T -o StrictHostKeyChecking=no "${SSH_USER}@${ip}" "echo '$SSH_PASS' | sudo -S ls -la '$TARGET_LINK'" 2>/dev/null)
    echo "$out"
}

is_symlink_correct() {
    local out="$1"
    echo "$out" | grep -q "${TARGET_LINK} -> ${TARGET_DEST}"
}

is_symlink_absent() {
    local out="$1"
    if echo "$out" | grep -q "No such file"; then
        return 0
    fi
    if echo "$out" | grep -q "${TARGET_LINK} ->"; then
        return 1
    fi
    return 0
}

wait_for_job_gone() {
    local node="$1"
    local action="$2"
    local timeout=300
    local elapsed=0
    log_detail "Waiting for job completion on $node action=$action"
    while [ "$elapsed" -lt "$timeout" ]; do
        local count
        count=$(kubectl get jobs -n "$NAMESPACE" -l "app.kubernetes.io/managed-by=lj-runtimekit-controller,lj-runtimekit-controller/target-node=${node},lj-runtimekit-controller/action=${action}" -o name 2>/dev/null | wc -l)
        if [ "$count" -eq 0 ]; then
            log_detail "Job gone after ${elapsed}s"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    return 1
}

run_case() {
    local case_no="$1"
    local node="$2"
    local action="$3"
    local desc="$4"

    TOTAL=$((TOTAL + 1))
    log_detail ""
    log_detail "========== CASE ${case_no}: ${desc} (node=${node}, action=${action}) =========="

    # Pre-load image
    ensure_image "$node"

    local before_label
    before_label=$(get_label "$node")
    log_detail "Before label: ${LABEL_KEY}=${before_label}"

    # Apply label
    if [ "$action" = "enable" ]; then
        set_label "$node" "true"
    else
        set_label "$node" ""
    fi
    log_detail "Applied label for action=$action"

    # Wait for event
    local event
    event=$(wait_for_event "$node" "$action")
    local event_rc=$?

    if [ "$event_rc" -ne 0 ]; then
        log_result "CASE ${case_no} FAIL: event not received (node=${node}, action=${action})"
        FAIL=$((FAIL + 1))
        return
    fi

    local event_node
    event_node=$(echo "$event" | awk '{print $1}')
    local event_msg
    event_msg=$(echo "$event" | awk '{$1=""; sub(/^[ \t]+/, ""); print}')
    log_detail "Event received: node=${event_node}, message=${event_msg}"

    # Verify event node matches
    if [ "$event_node" != "$node" ]; then
        log_result "CASE ${case_no} FAIL: event node mismatch (expected=${node}, got=${event_node})"
        FAIL=$((FAIL + 1))
        return
    fi

    # Verify event message contains node name
    if ! echo "$event_msg" | grep -q "succeeded on node \"${node}\""; then
        log_result "CASE ${case_no} FAIL: event message node mismatch (msg=${event_msg})"
        FAIL=$((FAIL + 1))
        return
    fi

    # Wait for job to be cleaned up
    wait_for_job_gone "$node" "$action"

    # Check symlink
    local symlink_out
    symlink_out=$(check_symlink "$node")
    log_detail "Symlink state: $symlink_out"

    if [ "$action" = "enable" ]; then
        if is_symlink_correct "$symlink_out"; then
            log_result "CASE ${case_no} PASS: enable succeeded on ${node}"
            PASS=$((PASS + 1))
        else
            log_result "CASE ${case_no} FAIL: enable symlink incorrect on ${node} (got=${symlink_out})"
            FAIL=$((FAIL + 1))
        fi
    else
        if is_symlink_absent "$symlink_out"; then
            log_result "CASE ${case_no} PASS: disable succeeded on ${node}"
            PASS=$((PASS + 1))
        else
            log_result "CASE ${case_no} FAIL: disable symlink still present on ${node} (got=${symlink_out})"
            FAIL=$((FAIL + 1))
        fi
    fi
}

# Ensure python3 is available
if ! command -v python3 >/dev/null 2>&1; then
    log_result "ERROR: python3 is required but not installed"
    exit 1
fi

# Normalize initial state: disable both nodes
log_detail "Normalizing initial state: removing labels from both nodes"
set_label "$NODE1" ""
set_label "$NODE2" ""
sleep 10

# Run test cases
case_num=0
for round in $(seq 1 10); do
    log_detail ""
    log_detail "#################### ROUND ${round} ####################"

    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "enable" "enable node1"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "disable" "disable node1"

    case_num=$((case_num + 1)); run_case "$case_num" "$NODE2" "enable" "enable node2"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE2" "disable" "disable node2"

    # Idempotent cases
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "enable" "idempotent enable node1"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "enable" "idempotent enable node1 again"

    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "disable" "idempotent disable node1"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "disable" "idempotent disable node1 again"

    # Rapid toggle
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "enable" "rapid enable node1"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "disable" "rapid disable node1"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "enable" "rapid enable node1 again"

    # Alternate nodes
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE2" "enable" "alternate enable node2"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "enable" "alternate enable node1"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE2" "disable" "alternate disable node2"
    case_num=$((case_num + 1)); run_case "$case_num" "$NODE1" "disable" "alternate disable node1"
done

log_detail ""
log_result "=== AMP E2E Test Completed at $(date -Iseconds) ==="
log_result "TOTAL: ${TOTAL}, PASS: ${PASS}, FAIL: ${FAIL}"
