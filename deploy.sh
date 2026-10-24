#!/usr/bin/env bash
# deploy.sh — build and push images to the OpenShift internal registry, then roll out
#
# Usage:
#   ./deploy.sh               # build + push all images, then rollout
#   ./deploy.sh ui api delete # build + push specific images, then rollout each
#   ./deploy.sh --rollout-only # skip builds, just restart all deployments

set -euo pipefail

NAMESPACE="cp4i"
REGISTRY="default-route-openshift-image-registry.apps.itz-hnqzef.infra01-lb.dal14.techzone.ibm.com"
# Cluster nodes are amd64 — always build for linux/amd64 regardless of local machine arch
PLATFORM="linux/amd64"

# ── Helpers ───────────────────────────────────────────────────────────────────
log() { echo "[deploy] $*"; }
die() { echo "[deploy] ERROR: $*" >&2; exit 1; }

image_tag()    {
  case "$1" in
    ui)     echo "${REGISTRY}/${NAMESPACE}/mq-kafka-bridge-ui:latest" ;;
    api)    echo "${REGISTRY}/${NAMESPACE}/mq-kafka-bridge-api:latest" ;;
    browse) echo "${REGISTRY}/${NAMESPACE}/mq-kafka-browse-connector:latest" ;;
    delete) echo "${REGISTRY}/${NAMESPACE}/mq-kafka-delete-connector:latest" ;;
  esac
}

build_context() {
  case "$1" in
    ui)     echo "src/ui" ;;
    api)    echo "src/api" ;;
    browse) echo "src/browse_connector" ;;
    delete) echo "src/delete_connector" ;;
  esac
}

deployment_name() {
  case "$1" in
    ui)     echo "mq-kafka-bridge-ui" ;;
    api)    echo "mq-kafka-bridge-api" ;;
    browse) echo "mq-kafka-browse-connector" ;;
    delete) echo "mq-kafka-delete-connector" ;;
  esac
}

registry_login() {
  log "Logging in to OpenShift registry..."
  # oc whoami returns "kube:admin" which docker login rejects — use "kubeadmin" directly
  oc whoami -t | docker login "${REGISTRY}" -u kubeadmin --password-stdin
}

build_and_push() {
  local name="$1"
  local ctx; ctx=$(build_context "$name")
  local tag; tag=$(image_tag "$name")
  log "Building ${name} (${ctx}) for ${PLATFORM}..."
  docker build --platform "${PLATFORM}" -t "${tag}" "${ctx}"
  log "Pushing ${name}..."
  docker push "${tag}"
}

rollout() {
  local name="$1"
  local dep; dep=$(deployment_name "$name")
  log "Restarting deployment/${dep}..."
  oc rollout restart "deployment/${dep}" -n "${NAMESPACE}"
  oc rollout status "deployment/${dep}" -n "${NAMESPACE}" --timeout=120s
}

# ── Argument parsing ──────────────────────────────────────────────────────────
TARGETS=()
ROLLOUT_ONLY=false

if [[ "${1:-}" == "--rollout-only" ]]; then
  ROLLOUT_ONLY=true
  TARGETS=(ui api browse delete)
elif [[ $# -eq 0 || "${1:-}" == "all" ]]; then
  TARGETS=(ui api browse delete)
else
  for arg in "$@"; do
    case "$arg" in
      ui|api|browse|delete) TARGETS+=("$arg") ;;
      *) die "Unknown target '${arg}'. Valid: ui api browse delete all --rollout-only" ;;
    esac
  done
fi

# ── Main ──────────────────────────────────────────────────────────────────────
oc whoami &>/dev/null || die "Not logged in to OpenShift — run 'oc login' first"

if ! $ROLLOUT_ONLY; then
  registry_login
  for t in "${TARGETS[@]}"; do
    build_and_push "$t"
  done
fi

for t in "${TARGETS[@]}"; do
  rollout "$t"
done

log "Done."
if oc get route mq-kafka-bridge-ui -n "${NAMESPACE}" &>/dev/null; then
  HOST=$(oc get route mq-kafka-bridge-ui -n "${NAMESPACE}" -o jsonpath='{.spec.host}')
  log "UI → https://${HOST}"
fi
