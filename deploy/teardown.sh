#!/usr/bin/env bash
#
# teardown.sh — destroy every billable AWS resource this project creates.
#
#   ./deploy/teardown.sh           delete everything, then report what is left
#   ./deploy/teardown.sh --check   report only. Deletes nothing.
#
# Run this at the END OF EVERY WORKING SESSION. Nothing triggers it automatically:
# the budget alerts in 25a are a backstop for the night you forget, not a trigger.
# An alert tells you money was spent; this script is what stops it being spent.
#
# ---------------------------------------------------------------------------------
# WHY THE ORDER IS WHAT IT IS
#
# Deletion order is the REVERSE of creation order, because AWS refuses to delete a
# thing that something else still points at ("DependencyViolation"). Concretely:
#
#   1. The Ingress goes first, WHILE THE CLUSTER IS STILL UP. The ALB was created by
#      a controller inside the cluster, not by us and not by eksctl — so it is in
#      nobody's config. Delete the cluster first and the ALB is STRANDED: running,
#      billing, and no longer attached to anything that would remind you it exists.
#   2. The certificate can only be deleted once no load balancer is using it.
#   3. ElastiCache and RDS live in the VPC that eksctl created, so they must be gone
#      before eksctl can remove that VPC — and their deletes are SLOW and async, so
#      the script waits rather than racing.
#   4. eksctl delete cluster then removes the nodegroup, the VPC, the subnets, the
#      internet gateway, the OIDC provider AND THE NAT GATEWAY (~$1.10/day — the
#      single most expensive thing to leave behind).
#   5. ECR is independent of the VPC and can go any time.
#
# The script never trusts a delete. It ends by LISTING what still exists, because a
# delete that silently did nothing and a delete that worked look identical.
# ---------------------------------------------------------------------------------

# NOT `set -e`, deliberately. This script exists to be run when you are unsure what
# is still up, so "that was already deleted" must be a success and must not abort the
# sweep. With -e the first already-gone resource would kill the run before it ever
# reached the NAT Gateway. Errors are handled per-step instead.
set -uo pipefail

# --- configuration --------------------------------------------------------------
# These names are the contract between this script and every task that creates
# something. This file was written BEFORE anything existed, which is what forced the
# names to be decided up front rather than improvised per task.
REGION="${AWS_REGION:-us-east-1}"
PREFIX="webhook"

CLUSTER="${PREFIX}-eks"
REDIS_ID="${PREFIX}-redis"
DB_ID="${PREFIX}-db"
ECR_REPOS=("${PREFIX}-producer" "${PREFIX}-worker")
CACHE_SUBNET_GROUP="${PREFIX}-cache-subnets"
DB_SUBNET_GROUP="${PREFIX}-db-subnets"
K8S_NAMESPACE="webhooks"
TAG_KEY="Project"
TAG_VALUE="webhook-delivery"
CERT_DOMAIN="webhooks.sndiaye.com"

# 🔴 DELIBERATELY NOT DELETED: the Route 53 hosted zone for sndiaye.com ($0.50/month).
# Deleting it stops the domain resolving AND breaks ACM's ability to re-validate the
# certificate on renewal, so the next `terraform`-free rebuild would need the whole
# domain wired up again. It is the one thing here that is cheaper to keep than to
# recreate. This is an exclusion, not an oversight — which is why it is written down.
#
# 🔴 ALSO NOT DELETED: the AWS Budgets from 25a. They are account-level settings, not
# project resources, they cost nothing, and they must outlive every teardown.

# --- helpers --------------------------------------------------------------------
CHECK_ONLY=0
[[ "${1:-}" == "--check" ]] && CHECK_ONLY=1

log()  { printf '\n\033[1m▶ %s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m✓ %s\033[0m\n' "$*"; }
warn() { printf '  \033[33m! %s\033[0m\n' "$*"; }

# Runs a delete and treats "it wasn't there" as success. This is what makes the
# script idempotent: running it twice must be safe, and running it on a half-torn-down
# account must continue past whatever is already gone.
try() {
  local what="$1"; shift
  if out=$("$@" 2>&1); then
    ok "$what — deleted"
  elif grep -qiE 'not.?found|does not exist|NoSuchEntity|ResourceNotFoundException|ValidationError.*not found' <<<"$out"; then
    info "$what — already gone"
  else
    warn "$what — FAILED: $(head -1 <<<"$out")"
  fi
}

# --- the inventory --------------------------------------------------------------
# This is the real deliverable. Every count below is a thing that bills by the hour.
# It runs standalone via --check, and Task 31b uses it as the proof of teardown.
inventory() {
  log "INVENTORY — what still exists in $REGION"
  local total=0 n

  n=$(aws eks list-clusters --region "$REGION" --query "length(clusters[?contains(@,'$PREFIX')])" --output text 2>/dev/null || echo 0)
  info "EKS clusters ............ $n"; total=$((total + n))

  n=$(aws ec2 describe-instances --region "$REGION" \
        --filters "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query "length(Reservations[].Instances[])" --output text 2>/dev/null || echo 0)
  info "EC2 instances ........... $n"; total=$((total + n))

  # The expensive surprise. `available` and `pending` bill; `deleted` does not, so the
  # filter matters — without it a torn-down account reports NAT Gateways forever.
  n=$(aws ec2 describe-nat-gateways --region "$REGION" \
        --filter "Name=state,Values=available,pending" \
        --query "length(NatGateways)" --output text 2>/dev/null || echo 0)
  info "NAT Gateways ............ $n   <-- ~\$1.10/day each"; total=$((total + n))

  # Found at 26c by enumerating the CloudFormation stack rather than by trusting
  # the mental inventory: eksctl allocates an Elastic IP for the NAT Gateway.
  #
  # 🔴 ONLY THE UNATTACHED ONES ARE COUNTED, and that is the whole point. While
  # the NAT Gateway exists its EIP is attached and already represented by the
  # line above -- counting it too would double-count. The failure this catches is
  # the NAT Gateway going away and LEAVING THE ADDRESS BEHIND, which since 2024
  # bills on its own (~$0.005/hr, ~$0.12/day) for an address doing nothing.
  #
  # Same shape as the NAT Gateway itself, one level deeper: nobody asked for this
  # resource, so nobody remembers to look for it.
  n=$(aws ec2 describe-addresses --region "$REGION" \
        --query "length(Addresses[?AssociationId==null])" --output text 2>/dev/null || echo 0)
  info "Unattached Elastic IPs .. $n   <-- ~\$0.12/day each"; total=$((total + n))

  n=$(aws elbv2 describe-load-balancers --region "$REGION" \
        --query "length(LoadBalancers)" --output text 2>/dev/null || echo 0)
  info "Load balancers .......... $n"; total=$((total + n))

  n=$(aws elasticache describe-cache-clusters --region "$REGION" \
        --query "length(CacheClusters)" --output text 2>/dev/null || echo 0)
  info "ElastiCache clusters .... $n"; total=$((total + n))

  n=$(aws rds describe-db-instances --region "$REGION" \
        --query "length(DBInstances)" --output text 2>/dev/null || echo 0)
  info "RDS instances ........... $n"; total=$((total + n))

  # Storage-priced rather than hourly (~$0.10/GB/month), so this is pennies — listed
  # because "nothing left" should mean nothing, not "nothing I consider expensive".
  n=$(aws ecr describe-repositories --region "$REGION" \
        --query "length(repositories[?contains(repositoryName,'$PREFIX')])" --output text 2>/dev/null || echo 0)
  info "ECR repositories ........ $n   (storage-priced, not hourly)"

  printf '\n'
  if [[ "$total" -eq 0 ]]; then
    ok "TOTAL HOURLY-BILLED RESOURCES: 0"
    info "Still billing on purpose: the Route 53 hosted zone (\$0.50/month)."
  else
    warn "TOTAL HOURLY-BILLED RESOURCES: $total  <-- STILL COSTING MONEY"
  fi
  printf '\n'
  info "Cost Explorer lags ~24h, so today's spend will not appear until tomorrow."
  info "Verify with: aws ce get-cost-and-usage --time-period Start=\$(date -u +%Y-%m-01),End=\$(date -u +%Y-%m-%d) --granularity MONTHLY --metrics UnblendedCost"
}

# --- main -----------------------------------------------------------------------
log "Account: $(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo UNKNOWN)   Region: $REGION"

if [[ "$CHECK_ONLY" -eq 1 ]]; then
  info "--check: reporting only, deleting nothing."
  inventory
  exit 0
fi

# STEP 1 — the Ingress, while the cluster still exists.
# This is the step whose ORDER actually matters. The ALB is created by a controller
# reacting to the Ingress object; removing the Ingress is what tells it to remove the
# ALB. Skipping this leaves a load balancer nothing will ever clean up.
log "1/7  Ingress (releases the ALB) — must happen while the cluster is UP"
if kubectl --request-timeout=10s get ns "$K8S_NAMESPACE" >/dev/null 2>&1; then
  kubectl delete ingress --all -n "$K8S_NAMESPACE" --ignore-not-found --timeout=120s 2>&1 | sed 's/^/  /'
  info "waiting 60s for the controller to delete the ALB..."
  sleep 60
  ok "ingress removed"
else
  info "no reachable cluster / namespace — skipping"
fi

# STEP 2 — the certificate, now that nothing is using it.
log "2/7  ACM certificate for $CERT_DOMAIN"
CERT_ARN=$(aws acm list-certificates --region "$REGION" \
             --query "CertificateSummaryList[?DomainName=='$CERT_DOMAIN'].CertificateArn | [0]" \
             --output text 2>/dev/null)
if [[ -n "$CERT_ARN" && "$CERT_ARN" != "None" ]]; then
  try "certificate" aws acm delete-certificate --region "$REGION" --certificate-arn "$CERT_ARN"
else
  info "no certificate for $CERT_DOMAIN — already gone"
fi

# STEP 3 — the datastores. These live INSIDE the VPC eksctl created, so they must be
# fully deleted before step 5 can remove that VPC. Both deletes are asynchronous and
# take several minutes, which is why the script waits instead of racing ahead.
log "3/7  ElastiCache + RDS (must finish before the VPC can go)"
try "elasticache $REDIS_ID" aws elasticache delete-cache-cluster --region "$REGION" --cache-cluster-id "$REDIS_ID"
try "rds $DB_ID" aws rds delete-db-instance --region "$REGION" --db-instance-identifier "$DB_ID" \
      --skip-final-snapshot --delete-automated-backups

info "waiting for both to finish deleting (several minutes)..."
aws elasticache wait cache-cluster-deleted --region "$REGION" --cache-cluster-id "$REDIS_ID" 2>/dev/null && ok "elasticache gone"
aws rds wait db-instance-deleted --region "$REGION" --db-instance-identifier "$DB_ID" 2>/dev/null && ok "rds gone"

# STEP 4 — the subnet groups, which reference the subnets and so must precede the VPC.
log "4/7  Subnet groups"
try "cache subnet group" aws elasticache delete-cache-subnet-group --region "$REGION" --cache-subnet-group-name "$CACHE_SUBNET_GROUP"
try "db subnet group"    aws rds delete-db-subnet-group --region "$REGION" --db-subnet-group-name "$DB_SUBNET_GROUP"

# STEP 5 — stray EC2 instances carrying our project tag.
#
# The cluster's own nodes are NOT handled here — eksctl removes those with the
# nodegroup in step 6. This step exists for anything created outside that: a scratch
# instance, a test box, anything started by hand and forgotten.
#
# 🔴 SCOPED BY TAG, NEVER BY REGION. "Terminate every instance in us-east-1" would be
# one line shorter and would destroy unrelated things this account may hold later. A
# teardown script that can damage something outside its own project is worse than no
# teardown script, because it will eventually be run by someone in a hurry.
log "5/7  Stray EC2 instances tagged $TAG_KEY=$TAG_VALUE"
STRAY=$(aws ec2 describe-instances --region "$REGION" \
          --filters "Name=tag:$TAG_KEY,Values=$TAG_VALUE" \
                    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
          --query "Reservations[].Instances[].InstanceId" --output text 2>/dev/null)
if [[ -n "$STRAY" ]]; then
  info "terminating: $STRAY"
  aws ec2 terminate-instances --region "$REGION" --instance-ids $STRAY >/dev/null 2>&1
  info "waiting for termination..."
  aws ec2 wait instance-terminated --region "$REGION" --instance-ids $STRAY 2>/dev/null && ok "instances terminated"
else
  info "no tagged instances — already gone"
fi

# STEP 6 — the cluster. This one command removes the most money: the nodegroup, the
# VPC, its subnets, the internet gateway, the IAM OIDC provider, and THE NAT GATEWAY.
# --wait makes it block until AWS reports the CloudFormation stacks actually deleted,
# rather than until the request was accepted.
log "6/7  EKS cluster $CLUSTER  (takes the VPC and the NAT Gateway with it)"
if aws eks describe-cluster --region "$REGION" --name "$CLUSTER" >/dev/null 2>&1; then
  eksctl delete cluster --name "$CLUSTER" --region "$REGION" --wait 2>&1 | sed 's/^/  /'
  ok "cluster deleted"
else
  info "cluster $CLUSTER not found — already gone"
fi

# STEP 6 — ECR. No VPC dependency, so it could go anywhere; --force is required
# because a repository containing images refuses to delete without it.
log "7/7  ECR repositories"
for repo in "${ECR_REPOS[@]}"; do
  try "ecr $repo" aws ecr delete-repository --region "$REGION" --repository-name "$repo" --force
done

# --- prove it, rather than trust it ---------------------------------------------
# Every step above reported success on its own terms. That is not evidence: a delete
# that silently did nothing prints the same as one that worked, and the ALB in step 1
# is deleted by a controller whose success this script never observed.
inventory
