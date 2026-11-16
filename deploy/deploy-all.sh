#!/usr/bin/env bash
#
# Copyright (c) 2024-2026 Alibaba Cloud.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# kubectl is only needed for deploy/undeploy, so the dependency check is
# deferred to require_kubectl() and 'help' works without kubectl installed.
require_kubectl() {
  command -v kubectl >/dev/null 2>&1 || { echo "[ERROR] kubectl not found in PATH"; exit 1; }
}

# ==================== Output formatting ====================
# Colored output with status symbols; auto-disabled when stdout is not a
# TTY or NO_COLOR is set, so piped logs stay plain and ASCII-safe.
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
  C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'; C_CYAN=$'\033[36m'
  S_OK="✓"; S_FAIL="✗"; S_WARN="⚠"; S_WAIT="○"
else
  C_RESET=""; C_BOLD=""; C_DIM=""
  C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_CYAN=""
  S_OK="[OK]"; S_FAIL="[FAIL]"; S_WARN="[WARN]"; S_WAIT="[..]"
fi

banner() {
  local line="============================================================"
  echo ""
  echo "${C_BOLD}${C_CYAN}${line}${C_RESET}"
  echo "${C_BOLD}${C_CYAN}  $*${C_RESET}"
  echo "${C_BOLD}${C_CYAN}${line}${C_RESET}"
}

section() {
  echo ""
  echo "${C_BOLD}${C_BLUE}── $*${C_RESET}"
}

log_info()  { echo "${C_BLUE}[INFO]${C_RESET} $*"; }
log_ok()    { echo "${C_GREEN}[ OK ]${C_RESET} $*"; }
log_warn()  { echo "${C_YELLOW}[WARN]${C_RESET} $*"; }
log_error() { echo "${C_RED}[FAIL]${C_RESET} $*"; }

# Print one check result line. Args: status(0=ok 1=pending 2=fail) text
print_check() {
  case "$1" in
    0) echo "  ${C_GREEN}${S_OK}${C_RESET} $2" ;;
    1) echo "  ${C_YELLOW}${S_WAIT}${C_RESET} $2" ;;
    *) echo "  ${C_RED}${S_FAIL}${C_RESET} $2" ;;
  esac
}

# All available components and their corresponding directories.
# ALL_COMPONENTS order is the deployment order and the default selection:
# job-controller ships the ComputeDomain CRDs that device-plugin relies on,
# so it is applied first and removed last.
declare -A COMPONENT_DIRS=(
  ["job-controller"]="${SCRIPT_DIR}/job-controller"
  ["device-plugin"]="${SCRIPT_DIR}/deviceplugin"
  ["scheduler"]="${SCRIPT_DIR}/scheduler"
  ["exporter"]="${SCRIPT_DIR}/exporter"
  ["controller"]="${SCRIPT_DIR}/controller"
)

ALL_COMPONENTS=("job-controller" "device-plugin" "scheduler" "exporter" "controller")

# ==================== Post-deploy verification config ====================
RUNTIMEKIT_NS="kube-system"
VERIFY_TIMEOUT="${VERIFY_TIMEOUT:-180}"
VERIFY_POLL_INTERVAL="${VERIFY_POLL_INTERVAL:-5}"
EXPORTER_METRICS_PORT="${EXPORTER_METRICS_PORT:-9501}"
NODE_RUNTIMEKIT_LABEL="lj_runtimekit_enable=true"

usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  deploy      Deploy components to Kubernetes
  undeploy    Remove components from Kubernetes
  help        Show this help text (does not need kubectl)

Options:
  --components=<list>   Comma-separated list of components to deploy/undeploy.
                        Available: job-controller, device-plugin, scheduler,
                        exporter, controller
                        Default: all components in the order above
  --skip-verify         Skip the post-deployment verification (deploy only)

Notes:
  Each component applies the regular *.yaml/*.yml files of its own directory
  in ascending order (and deletes them in reverse order). Subdirectories are
  not scanned, so deploy/job-controller/examples is never applied
  automatically; apply those examples by hand once the CRDs exist.

Environment variables:
  SKIP_VERIFY=1                     Skip the post-deployment verification (same as --skip-verify)
  VERIFY_TIMEOUT=<sec>              Max wait time for post-deploy verification (default 180)
  VERIFY_POLL_INTERVAL=<sec>        Interval between status poll rounds (default 5)

Examples:
  $(basename "$0") deploy                                     # Deploy all components
  $(basename "$0") deploy --components=job-controller          # Deploy job-controller only
  $(basename "$0") deploy --components=device-plugin,exporter # Deploy multiple
  $(basename "$0") undeploy                                   # Undeploy all components
  $(basename "$0") undeploy --components=controller           # Undeploy controller only
  $(basename "$0") help                                        # Show this help text
EOF
}

validate_components() {
  local -a requested=("$@")
  for comp in "${requested[@]}"; do
    if [[ -z "${COMPONENT_DIRS[$comp]+_}" ]]; then
      log_error "Unknown component: '$comp'"
      echo "Available components: ${ALL_COMPONENTS[*]}"
      exit 1
    fi
  done
}

deploy_component() {
  local comp="$1"
  local dir="${COMPONENT_DIRS[$comp]}"

  banner "Deploying: ${comp}"

  if [[ ! -d "$dir" ]]; then
    log_warn "Directory not found: $dir, skipping."
    return
  fi

  local yamls
  yamls=$(find "$dir" -maxdepth 1 \( -name "*.yaml" -o -name "*.yml" \) 2>/dev/null | sort)

  if [[ -z "$yamls" ]]; then
    log_warn "No YAML files found in $dir, skipping."
    return
  fi

  while IFS= read -r f; do
    log_info "Applying: $f"
    kubectl apply -f "$f"
  done <<< "$yamls"

  log_ok "Component '${comp}' deployed successfully."
}

undeploy_component() {
  local comp="$1"
  local dir="${COMPONENT_DIRS[$comp]}"

  banner "Undeploying: ${comp}"

  if [[ ! -d "$dir" ]]; then
    log_warn "Directory not found: $dir, skipping."
    return
  fi

  local yamls
  yamls=$(find "$dir" -maxdepth 1 \( -name "*.yaml" -o -name "*.yml" \) 2>/dev/null | sort -r)

  if [[ -z "$yamls" ]]; then
    log_warn "No YAML files found in $dir, skipping."
    return
  fi

  while IFS= read -r f; do
    log_info "Deleting: $f"
    kubectl delete -f "$f" --ignore-not-found=true
  done <<< "$yamls"

  log_ok "Component '${comp}' undeployed successfully."
}

# ==================== Post-deploy verification ====================
VERIFY_ISSUES=()
VERIFY_WARNINGS=()

add_verify_issue() {
  VERIFY_ISSUES+=("$1")
}

add_verify_warning() {
  VERIFY_WARNINGS+=("$1")
}

# Non-blocking status checks used by the polling loop.
# Each check echoes a one-line status summary and returns 0 when ready.

# Deployment status detail: ready / updated / available / failed.
# Return 0 if all desired replicas are ready, up-to-date, available, and none
# of the selected pods are in a failure state (e.g. ImagePullBackOff).
check_deployment_status() {
  local name="$1" ns="$2"
  local ready desired updated available
  ready=$(kubectl get deployment "$name" -n "$ns" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  desired=$(kubectl get deployment "$name" -n "$ns" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)
  updated=$(kubectl get deployment "$name" -n "$ns" -o jsonpath='{.status.updatedReplicas}' 2>/dev/null || true)
  available=$(kubectl get deployment "$name" -n "$ns" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || true)
  ready=${ready:-0}; desired=${desired:-0}; updated=${updated:-0}; available=${available:-0}

  # Count pods in failure states. During a rolling update, old pods may still
  # be Ready/Available while new pods fail (e.g. image pull failure). Status
  # counters alone can therefore falsely report success.
  local failed=0 selector states
  selector=$(kubectl get deployment "$name" -n "$ns" -o go-template='{{range $k,$v := .spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}' 2>/dev/null | sed 's/,$//')
  if [[ -n "$selector" ]]; then
    states=$(kubectl get pods -n "$ns" -l "$selector" -o jsonpath='{range .items[*]}{.status.phase} {.status.containerStatuses[0].state.waiting.reason}{"\n"}{end}' 2>/dev/null || true)
    failed=$(echo "$states" | grep -c -E 'CrashLoopBackOff|ImagePullBackOff|ErrImagePull|CreateContainerConfigError|InvalidImageName|RegistryUnavailable|^Failed ' || true)
  fi

  echo "Deployment ${name} ready=${ready}/${desired}, updated=${updated}/${desired}, available=${available}/${desired}, failed=${failed}"
  [[ "$desired" -gt 0 && "$ready" -eq "$desired" && "$updated" -eq "$desired" && "$available" -eq "$desired" && "$failed" -eq 0 ]]
}

# DaemonSet status detail: total / ready / updated / creating / failed.
# Return 0 if all desired pods are ready AND up-to-date (important during rolling updates).
check_daemonset_status() {
  local name="$1" ns="$2"
  local desired ready updated update_strategy
  desired=$(kubectl get daemonset "$name" -n "$ns" -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || true)
  ready=$(kubectl get daemonset "$name" -n "$ns" -o jsonpath='{.status.numberReady}' 2>/dev/null || true)
  updated=$(kubectl get daemonset "$name" -n "$ns" -o jsonpath='{.status.updatedNumberScheduled}' 2>/dev/null || true)
  update_strategy=$(kubectl get daemonset "$name" -n "$ns" -o jsonpath='{.spec.updateStrategy.type}' 2>/dev/null || true)
  desired=${desired:-0}; ready=${ready:-0}
  # For RollingUpdate, updatedNumberScheduled must be present; if it is temporarily
  # empty (controller has not updated status yet), default to 0 so we wait.
  # For OnDelete or unknown strategies, skip the up-to-date check.
  if [[ "$update_strategy" == "RollingUpdate" ]]; then
    updated=${updated:-0}
  else
    updated=${updated:-$desired}
  fi
  local failed=0 selector states
  selector=$(kubectl get daemonset "$name" -n "$ns" -o go-template='{{range $k,$v := .spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}' 2>/dev/null | sed 's/,$//')
  if [[ -n "$selector" ]]; then
    states=$(kubectl get pods -n "$ns" -l "$selector" -o jsonpath='{range .items[*]}{.status.phase} {.status.containerStatuses[0].state.waiting.reason}{"\n"}{end}' 2>/dev/null || true)
    failed=$(echo "$states" | grep -c -E 'CrashLoopBackOff|ImagePullBackOff|ErrImagePull|CreateContainerConfigError|InvalidImageName|RegistryUnavailable|^Failed ' || true)
  fi
  local creating=$((desired - ready - failed))
  [[ $creating -lt 0 ]] && creating=0
  echo "DaemonSet ${name} total=${desired}, ready=${ready}, updated=${updated}, creating=${creating}, failed=${failed}"
  [[ "$desired" -gt 0 && "$ready" -eq "$desired" && "$updated" -eq "$desired" ]]
}

# Doc 3.6: 节点资源上报 (aliyun.com/*); return 0 if all labeled nodes report
check_node_resources() {
  local nodes
  nodes=$(kubectl get nodes -l "$NODE_RUNTIMEKIT_LABEL" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  if [[ -z "$nodes" ]]; then
    echo "node resources: no node labeled ${NODE_RUNTIMEKIT_LABEL}"
    return 1
  fi
  local total=0 reported=0 node res
  for node in $nodes; do
    total=$((total+1))
    res=$(kubectl get node "$node" -o json 2>/dev/null | grep -o '"aliyun\.com/[^"]*"' | sort -u | tr -d '"' | tr '\n' ' ' || true)
    [[ -n "$res" ]] && reported=$((reported+1))
  done
  echo "node resources: ${reported}/${total} labeled node(s) report aliyun.com/*"
  [[ $total -gt 0 && $reported -eq $total ]]
}

# Doc 3.6: Exporter 指标暴露 (curl <node-ip>:9501/metrics); return 0 if all labeled
# nodes expose metrics data, return 2 if the check cannot be performed (skip).
check_exporter_metrics() {
  local nodes
  nodes=$(kubectl get nodes -l "$NODE_RUNTIMEKIT_LABEL" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  if [[ -z "$nodes" ]]; then
    echo "metrics: no node labeled ${NODE_RUNTIMEKIT_LABEL}, cannot probe"
    return 1
  fi
  local has_curl=0 has_wget=0
  command -v curl >/dev/null 2>&1 && has_curl=1
  command -v wget >/dev/null 2>&1 && has_wget=1
  if [[ $has_curl -eq 0 && $has_wget -eq 0 ]]; then
    echo "metrics: neither curl nor wget available on this machine, skipped"
    return 2
  fi
  local total=0 ok=0 node ip body
  for node in $nodes; do
    total=$((total+1))
    ip=$(kubectl get node "$node" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)
    [[ -z "$ip" ]] && continue
    body=""
    if [[ $has_curl -eq 1 ]]; then
      body=$(curl -fsS -m 5 "http://${ip}:${EXPORTER_METRICS_PORT}/metrics" 2>/dev/null || true)
    else
      body=$(wget -q -T 5 -O - "http://${ip}:${EXPORTER_METRICS_PORT}/metrics" 2>/dev/null || true)
    fi
    if [[ -n "$body" ]] && echo "$body" | grep -q '^[a-zA-Z_:]'; then
      ok=$((ok+1))
    fi
  done
  echo "metrics: ${ok}/${total} node(s) expose data on :${EXPORTER_METRICS_PORT}/metrics"
  [[ $total -gt 0 && $ok -eq $total ]]
}

# 组件运行状态由轮询循环中的工作负载状态检查覆盖。

# Concise failure diagnosis: pod phase/reason + last warning event + filtered log lines.

# Print log lines that look like failure messages from a container.
# Args: pod namespace [current|previous]
print_failure_log_lines() {
  local pod="$1" ns="$2" mode="${3:-current}"
  local logs=""
  if [[ "$mode" == "previous" ]]; then
    logs=$(kubectl logs "$pod" -n "$ns" --previous --tail=50 2>/dev/null || true)
  else
    logs=$(kubectl logs "$pod" -n "$ns" --tail=50 2>/dev/null || true)
  fi
  [[ -z "$logs" ]] && return 1

  local filtered
  filtered=$(echo "$logs" | grep -i -E 'error|fatal|fail|panic|bind: address already in use|cannot assign requested address|listen tcp .* failed|permission denied|no such file or directory' || true)
  [[ -z "$filtered" ]] && return 1

  echo "      ${C_DIM}Relevant log lines (${mode}):${C_RESET}"
  echo "$filtered" | head -5 | sed "s/^/        ${C_DIM}/; s/$/${C_RESET}/"
  return 0
}

# Check a single exporter pod for 9501 port occupancy by scanning its logs.
# Prints a message if detected.
check_exporter_pod_port_occupancy() {
  local pod="$1" ns="$2"
  local hint node
  hint=$( { kubectl logs "$pod" -n "$ns" --tail=30 2>/dev/null || true; kubectl logs "$pod" -n "$ns" --previous --tail=30 2>/dev/null || true; } | grep -i 'bind: address already in use' | head -1 || true)
  if [[ -n "$hint" ]]; then
    node=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo "unknown")
    echo "      ${C_RED}Detected 9501 port occupancy on node ${node}.${C_RESET}"
    echo "      ${C_DIM}Hint: run 'ss -ltnp | grep 9501' on node ${node} to find the conflicting process.${C_RESET}"
  fi
}

diagnose_pod_failure_concise() {
  local pod="$1" ns="$2"
  local phase reason node
  phase=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  reason=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || true)
  node=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo "unknown")
  echo "      ${C_DIM}Pod ${pod} (node=${node}): phase=${phase}${reason:+, reason=${reason}}${C_RESET}"
  local ev
  ev=$(kubectl get events -n "$ns" --field-selector involvedObject.name="$pod" --sort-by='.lastTimestamp' --no-headers 2>/dev/null | grep -i 'warning' | tail -1 || true)
  if [[ -n "$ev" ]]; then
    ev=$(echo "$ev" | awk '{for(i=5;i<=NF;i++) printf "%s ", $i; print ""}' | sed 's/ *$//')
    echo "      ${C_DIM}Last warning: ${ev:0:200}${C_RESET}"
  fi
  print_failure_log_lines "$pod" "$ns" "current" || print_failure_log_lines "$pod" "$ns" "previous"
  # For exporter pods, check 9501 port occupancy immediately after log diagnosis.
  local app_label
  app_label=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/name}' 2>/dev/null || true)
  if [[ "$app_label" == "lj-runtimekit-exporter" ]]; then
    check_exporter_pod_port_occupancy "$pod" "$ns"
  fi
}

diagnose_workload_failure_concise() {
  local kind="$1" name="$2" ns="$3"
  local selector pods pod
  selector=$(kubectl get "$kind" "$name" -n "$ns" -o go-template='{{range $k,$v := .spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}' 2>/dev/null | sed 's/,$//')
  [[ -z "$selector" ]] && return
  pods=$(kubectl get pods -n "$ns" -l "$selector" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  [[ -z "$pods" ]] && return
  for pod in $pods; do
    diagnose_pod_failure_concise "$pod" "$ns"
  done
}

print_verify_result() {
  local w
  for w in "${VERIFY_WARNINGS[@]}"; do
    log_warn "${w}"
  done
  local line="============================================================"
  if [[ ${#VERIFY_ISSUES[@]} -gt 0 ]]; then
    echo ""
    echo "${C_BOLD}${C_RED}${line}${C_RESET}"
    echo "${C_BOLD}${C_RED}  ${S_FAIL} Post-deployment verification FAILED: ${#VERIFY_ISSUES[@]} issue(s) found${C_RESET}"
    echo "${C_BOLD}${C_RED}${line}${C_RESET}"
    local i=1
    local issue
    for issue in "${VERIFY_ISSUES[@]}"; do
      echo "  ${C_RED}${i}.${C_RESET} ${issue}"
      i=$((i + 1))
    done
    echo ""
    echo "Please fix the issues above, then re-verify."
    exit 1
  fi
  echo ""
  echo "${C_BOLD}${C_GREEN}${line}${C_RESET}"
  echo "${C_BOLD}${C_GREEN}  ${S_OK} Post-deployment verification PASSED${C_RESET}"
  echo "${C_BOLD}${C_GREEN}      All deployed components are ready${C_RESET}"
  echo "${C_BOLD}${C_GREEN}${line}${C_RESET}"
}

run_post_deploy_verify() {
  banner "Post-deployment verification"

  local deadline=$(( $(date +%s) + VERIFY_TIMEOUT ))
  local poll=0
  declare -A ready_map

  echo "  Timeout: ${C_BOLD}${VERIFY_TIMEOUT}s${C_RESET}    Poll interval: ${C_BOLD}${VERIFY_POLL_INTERVAL}s${C_RESET}"

  local comp detail
  while :; do
    poll=$((poll+1))
    section "Round ${poll}  ${C_DIM}$(date '+%H:%M:%S')${C_RESET}"
    local all_ready=1

    for comp in "${SELECTED_COMPONENTS[@]}"; do
      if [[ "${ready_map[$comp]:-0}" == "1" ]]; then
        print_check 0 "${comp}: ${C_GREEN}READY${C_RESET}"
        continue
      fi

      local comp_ok=1 rc
      case "$comp" in
        scheduler)
          rc=0
          detail=$(check_deployment_status "lj-runtimekit-scheduler-framework" "$RUNTIMEKIT_NS") || rc=$?
          print_check $((rc == 0 ? 0 : 1)) "${comp}: ${detail}"
          [[ $rc -ne 0 ]] && comp_ok=0
          ;;
        controller)
          rc=0
          detail=$(check_deployment_status "lj-runtimekit-controller-manager" "$RUNTIMEKIT_NS") || rc=$?
          print_check $((rc == 0 ? 0 : 1)) "${comp}: ${detail}"
          [[ $rc -ne 0 ]] && comp_ok=0
          ;;
        device-plugin)
          rc=0
          detail=$(check_daemonset_status "lj-runtimekit-device-plugin-ds" "$RUNTIMEKIT_NS") || rc=$?
          print_check $((rc == 0 ? 0 : 1)) "${comp}: ${detail}"
          [[ $rc -ne 0 ]] && comp_ok=0
          local nr_detail nr_rc=0
          nr_detail=$(check_node_resources) || nr_rc=$?
          print_check $((nr_rc == 0 ? 0 : 1)) "${comp}: ${nr_detail}"
          [[ $nr_rc -ne 0 ]] && comp_ok=0
          ;;
        exporter)
          local ds_rc=0 m_detail m_rc
          detail=$(check_daemonset_status "lj-runtimekit-exporter-ds" "$RUNTIMEKIT_NS") || ds_rc=$?
          print_check $((ds_rc == 0 ? 0 : 1)) "${comp}: ${detail}"
          if [[ $ds_rc -eq 0 ]]; then
            # Only probe /metrics after all exporter pods are ready;
            # metrics data is the final readiness condition.
            m_rc=0
            m_detail=$(check_exporter_metrics) || m_rc=$?
            print_check $((m_rc == 0 ? 0 : 1)) "${comp}: ${m_detail}"
            if [[ $m_rc -eq 2 ]]; then
              add_verify_warning "[exporter] ${m_detail}; verify manually on a GPU node: curl localhost:${EXPORTER_METRICS_PORT}/metrics"
            elif [[ $m_rc -ne 0 ]]; then
              comp_ok=0
            fi
          else
            comp_ok=0
          fi
          ;;
      esac

      if [[ $comp_ok -eq 1 ]]; then
        ready_map[$comp]=1
      else
        all_ready=0
      fi
    done

    [[ $all_ready -eq 1 ]] && break
    [[ $(date +%s) -ge $deadline ]] && break

    sleep "$VERIFY_POLL_INTERVAL"
  done

  # Diagnose any not-ready components (concise) and collect issues
  local has_failure=0
  for comp in "${SELECTED_COMPONENTS[@]}"; do
    [[ "${ready_map[$comp]:-0}" == "1" ]] && continue
    has_failure=1
    case "$comp" in
      scheduler)
        log_error "scheduler: deployment not ready within ${VERIFY_TIMEOUT}s"
        diagnose_workload_failure_concise deployment lj-runtimekit-scheduler-framework "$RUNTIMEKIT_NS"
        add_verify_issue "[scheduler] deployment lj-runtimekit-scheduler-framework not ready within ${VERIFY_TIMEOUT}s"
        ;;
      controller)
        log_error "controller: deployment not ready within ${VERIFY_TIMEOUT}s"
        diagnose_workload_failure_concise deployment lj-runtimekit-controller-manager "$RUNTIMEKIT_NS"
        add_verify_issue "[controller] deployment lj-runtimekit-controller-manager not ready within ${VERIFY_TIMEOUT}s"
        ;;
      device-plugin)
        log_error "device-plugin: not ready within ${VERIFY_TIMEOUT}s"
        diagnose_workload_failure_concise daemonset lj-runtimekit-device-plugin-ds "$RUNTIMEKIT_NS"
        add_verify_issue "[device-plugin] daemonset lj-runtimekit-device-plugin-ds not ready within ${VERIFY_TIMEOUT}s"
        local nr_detail
        if ! nr_detail=$(check_node_resources); then
          log_error "device-plugin: ${nr_detail}"
          add_verify_issue "[device-plugin] ${nr_detail}. Fix: kubectl label node <node-name> ${NODE_RUNTIMEKIT_LABEL}; check device-plugin pod logs (kubectl logs -n ${RUNTIMEKIT_NS} -l app=lj-runtimekit-device-plugin)"
        fi
        ;;
      exporter)
        log_error "exporter: not ready within ${VERIFY_TIMEOUT}s"
        local ds_ok=1 m_detail m_rc
        detail=$(check_daemonset_status "lj-runtimekit-exporter-ds" "$RUNTIMEKIT_NS") || ds_ok=0
        print_check $((ds_ok == 1 ? 0 : 2)) "${comp}: ${detail}"
        if [[ $ds_ok -eq 0 ]]; then
          diagnose_workload_failure_concise daemonset lj-runtimekit-exporter-ds "$RUNTIMEKIT_NS"
          add_verify_issue "[exporter] daemonset lj-runtimekit-exporter-ds not ready within ${VERIFY_TIMEOUT}s"
        else
          m_rc=0
          m_detail=$(check_exporter_metrics) || m_rc=$?
          print_check $((m_rc == 0 ? 0 : 2)) "${comp}: ${m_detail}"
          if [[ $m_rc -eq 1 ]]; then
            add_verify_issue "[exporter] ${m_detail}. Pods are ready but /metrics returns no data; check exporter pod logs or run 'curl localhost:${EXPORTER_METRICS_PORT}/metrics' on the node itself"
          fi
        fi
        ;;
      job-controller)
        log_error "job-controller: deployment not ready within ${VERIFY_TIMEOUT}s"
        diagnose_workload_failure_concise deployment lj-runtimekit-job-controller "$RUNTIMEKIT_NS"
        add_verify_issue "[job-controller] deployment lj-runtimekit-job-controller not ready within ${VERIFY_TIMEOUT}s"
        ;;
    esac
  done

  [[ $has_failure -eq 1 ]] && echo ""
  print_verify_result
}

# ---- Main ----

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

COMMAND="$1"
shift

case "$COMMAND" in
  deploy|undeploy) ;;
  help|-h|--help)
    usage
    exit 0
    ;;
  *)
    log_error "Unknown command: '$COMMAND'"
    usage
    exit 1
    ;;
esac

# Parse --components flag
SELECTED_COMPONENTS=()
SKIP_VERIFY_FLAG=0
for arg in "$@"; do
  case "$arg" in
    --components=*)
      raw="${arg#--components=}"
      # Reject empty items ("", "a,", ",a", "a,,b") instead of letting the
      # split drop them, which would silently turn a typo into a partial run.
      if [[ -z "$raw" || "$raw" == ,* || "$raw" == *, || "$raw" == *,,* ]]; then
        log_error "Invalid --components list: '${raw}' (empty component name)"
        echo "Available components: ${ALL_COMPONENTS[*]}"
        exit 1
      fi
      IFS=',' read -ra SELECTED_COMPONENTS <<< "$raw"
      ;;
    --skip-verify)
      SKIP_VERIFY_FLAG=1
      ;;
    *)
      log_error "Unknown option: '$arg'"
      usage
      exit 1
      ;;
  esac
done

# Default to all components if none specified
if [[ ${#SELECTED_COMPONENTS[@]} -eq 0 ]]; then
  SELECTED_COMPONENTS=("${ALL_COMPONENTS[@]}")
fi

# Validate
validate_components "${SELECTED_COMPONENTS[@]}"

# Only deploy/undeploy reach this point, so 'help' works without kubectl.
require_kubectl

# Resolve skip-verify switch: command-line flag or env var
SKIP_VERIFY=0
if [[ $SKIP_VERIFY_FLAG -eq 1 ]]; then
  SKIP_VERIFY=1
else
  case "$(echo "${SKIP_VERIFY:-0}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) SKIP_VERIFY=1 ;;
  esac
fi

banner "LJ Runtime Kit - ${COMMAND}"
echo "  Command   : ${C_BOLD}${COMMAND}${C_RESET}"
echo "  Components: ${C_BOLD}${SELECTED_COMPONENTS[*]}${C_RESET}"

case "$COMMAND" in
  deploy)
    for comp in "${SELECTED_COMPONENTS[@]}"; do
      deploy_component "$comp"
    done
    log_ok "All specified components deployed."
    if [[ $SKIP_VERIFY -eq 1 ]]; then
      log_info "Post-deployment verification skipped (--skip-verify)."
    else
      run_post_deploy_verify
    fi
    ;;
  undeploy)
    # Reverse order for clean teardown
    reversed=()
    for (( i=${#SELECTED_COMPONENTS[@]}-1; i>=0; i-- )); do
      reversed+=("${SELECTED_COMPONENTS[$i]}")
    done
    for comp in "${reversed[@]}"; do
      undeploy_component "$comp"
    done
    log_ok "All specified components undeployed."
    ;;
esac
