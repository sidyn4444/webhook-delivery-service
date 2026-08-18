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
log "1/10  Ingress (releases the ALB) — must happen while the cluster is UP"
if kubectl --request-timeout=10s get ns "$K8S_NAMESPACE" >/dev/null 2>&1; then
  kubectl delete ingress --all -n "$K8S_NAMESPACE" --ignore-not-found --timeout=120s 2>&1 | sed 's/^/  /'
  info "waiting 60s for the controller to delete the ALB..."
  sleep 60
  ok "ingress removed"
else
  info "no reachable cluster / namespace — skipping"
fi

# STEP 2 — the DNS records, before the things they point at are gone.
#
# 🔴 WHY THIS STEP EXISTS AT ALL: nothing here costs money, and that is exactly why
# it was missing. Two records were created in Task 30 and neither is deleted by any
# other step:
#   * an ALIAS A record  webhooks.sndiaye.com -> the ALB
#   * an ACM validation CNAME  _<hash>.webhooks.sndiaye.com
# Step 1 deletes the ALB and step 3 deletes the certificate, leaving BOTH records
# pointing at things that no longer exist -- a "dangling" record. The alias one is
# the one that matters: it names an ELB by name in a known region, and an alias to a
# load balancer someone else could later create is the classic subdomain-takeover
# shape. The hosted zone deliberately SURVIVES teardown, so these records would
# otherwise outlive every rebuild and accumulate.
#
# ⚠️ Route 53 DELETE requires the record's EXACT current value, so each record is
# read back and echoed into the change batch rather than reconstructed from memory.
log "2/10  Route 53 records for $CERT_DOMAIN (dangling DNS, not a cost)"
ZONE_ID=$(aws route53 list-hosted-zones \
            --query "HostedZones[?Name=='${CERT_DOMAIN#*.}.'].Id | [0]" --output text 2>/dev/null | sed 's#/hostedzone/##')
if [[ -n "$ZONE_ID" && "$ZONE_ID" != "None" ]]; then
  RECS=$(aws route53 list-resource-record-sets --hosted-zone-id "$ZONE_ID" --output json 2>/dev/null)
  BATCH=$(CERT_DOMAIN="$CERT_DOMAIN" python3 -c '
import json,sys,os
d=json.load(sys.stdin); dom=os.environ["CERT_DOMAIN"]
ch=[{"Action":"DELETE","ResourceRecordSet":r} for r in d["ResourceRecordSets"]
    if r["Name"].rstrip(".").endswith(dom) and r["Type"] not in ("NS","SOA")]
print(json.dumps({"Changes":ch}) if ch else "")' <<<"$RECS")
  if [[ -n "$BATCH" ]]; then
    try "dns records under $CERT_DOMAIN" aws route53 change-resource-record-sets \
        --hosted-zone-id "$ZONE_ID" --change-batch "$BATCH"
  else
    info "no records under $CERT_DOMAIN — already gone"
  fi
else
  info "no hosted zone for ${CERT_DOMAIN#*.} — nothing to clean"
fi

# STEP 3 — the certificate, now that nothing is using it.
log "3/10  ACM certificate for $CERT_DOMAIN"
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
log "4/10  ElastiCache + RDS (must finish before the VPC can go)"
# 🔴 A REPLICATION GROUP, NOT A CACHE CLUSTER — and `delete-cache-cluster` will NOT
# remove one. Changed at 27a, after the API refused `--transit-encryption-enabled` on
# `create-cache-cluster` with "Encryption feature is not supported for engine REDIS".
# Encrypted, password-protected Redis on ElastiCache only exists as a replication
# group, even when it is a single node with no replica. So the shape of the resource
# was forced by the security requirement, not chosen.
#
# ⚠️ The failure this line prevents is a quiet one: the old command returns a
# not-found error, `try` treats not-found as success and prints a green line, and the
# Redis node keeps running. A teardown that reports success while the thing survives
# is worse than one that fails loudly.
#
# --no-retain-primary-cluster deletes the primary too. Without it AWS keeps the node
# as a standalone cache cluster — still billing, and now under a different name.
try "elasticache $REDIS_ID" aws elasticache delete-replication-group --region "$REGION" \
      --replication-group-id "$REDIS_ID" --no-retain-primary-cluster
try "rds $DB_ID" aws rds delete-db-instance --region "$REGION" --db-instance-identifier "$DB_ID" \
      --skip-final-snapshot --delete-automated-backups

info "waiting for both to finish deleting (several minutes)..."
aws elasticache wait replication-group-deleted --region "$REGION" --replication-group-id "$REDIS_ID" 2>/dev/null && ok "elasticache gone"
aws rds wait db-instance-deleted --region "$REGION" --db-instance-identifier "$DB_ID" 2>/dev/null && ok "rds gone"

# STEP 4 — the subnet groups, which reference the subnets and so must precede the VPC.
log "5/10  Subnet groups"
try "cache subnet group" aws elasticache delete-cache-subnet-group --region "$REGION" --cache-subnet-group-name "$CACHE_SUBNET_GROUP"
try "db subnet group"    aws rds delete-db-subnet-group --region "$REGION" --db-subnet-group-name "$DB_SUBNET_GROUP"

# --- our own security groups (added at 27a) ---------------------------------------
#
# 🔴 THESE COST NOTHING AND ARE STILL LOAD-BEARING FOR THE TEARDOWN. A security group
# we created lives in the VPC that eksctl owns, but eksctl's CloudFormation stack has
# no record of it. Step 6 therefore tries to delete a VPC that still has a member it
# does not know about, the delete FAILS, and the stack rolls back with the VPC intact
# — and the NAT Gateway inside it still billing at ~$1.10/day.
#
# The failure is not silent, but it is misleading: the error names the VPC, not the
# security group, so the thing that must be removed is never mentioned by the thing
# that broke. Same shape as the ALB in step 1 — a resource created outside the tool
# that owns the environment is exactly what a cleanup built from memory misses.
#
# ORDERING: this must run AFTER step 3, because a security group attached to a live
# ElastiCache or RDS network interface refuses to delete, and BEFORE step 6.
#
# 🔴 SCOPED BY NAME, NOT BY TAG — and the first version of this step got it wrong.
# `cluster.yaml` sets `tags: {Project: webhook-delivery}`, so eksctl stamps OUR tag
# onto every resource IT creates. Filtering on that tag returned three security
# groups: ours plus eksctl's ClusterSharedNodeSecurityGroup and its
# ControlPlaneSecurityGroup — CloudFormation-owned objects this script must not
# touch. The tag says which project a thing belongs to; it does NOT say who made it,
# and a cleanup needs the second question answered.
#
# The name filter uses the same PREFIX contract as $REDIS_ID and $DB_ID, which is how
# the rest of this script identifies its own resources. Verified read-only before
# being relied on: `webhook-*-sg` matched exactly ours, and a deliberately bogus
# pattern returned 0, so the filter discriminates rather than always matching.
log "6/10  Our security groups (free, but they block the VPC delete)"
OUR_SGS=$(aws ec2 describe-security-groups --region "$REGION" \
            --filters "Name=group-name,Values=${PREFIX}-*-sg" \
            --query "SecurityGroups[].GroupId" --output text 2>/dev/null)
if [[ -n "$OUR_SGS" ]]; then
  for sg in $OUR_SGS; do
    try "security group $sg" aws ec2 delete-security-group --region "$REGION" --group-id "$sg"
  done
else
  info "no tagged security groups — already gone"
fi

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
# ⚠️ MEASURED CONSEQUENCE OF THIS STEP RUNNING BEFORE STEP 8: the cluster's own
# nodes carry this tag, so this terminates them out from under the VPC CNI and
# orphans the secondary ENIs it attached for pod IPs — which then blocks a subnet
# delete and fails the stack in step 8. Step 8 now detects and repairs that. Left in
# this order deliberately: its real job is catching instances eksctl does NOT know
# about, and those must go before the VPC delete is attempted, not after.
log "7/10  Stray EC2 instances tagged $TAG_KEY=$TAG_VALUE"
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
log "8/10  EKS cluster $CLUSTER  (takes the VPC and the NAT Gateway with it)"

# 🔴 THE VPC ID MUST BE CAPTURED BEFORE THE CLUSTER GOES. The recovery block below
# needs it, and once the cluster is deleted there is nothing left to ask.
CLUSTER_VPC=$(aws eks describe-cluster --region "$REGION" --name "$CLUSTER" \
                --query 'cluster.resourcesVpcConfig.vpcId' --output text 2>/dev/null)

if aws eks describe-cluster --region "$REGION" --name "$CLUSTER" >/dev/null 2>&1; then
  # ⚠️ `set -o pipefail` is on, so this captures eksctl's status, not sed's.
  eksctl delete cluster --name "$CLUSTER" --region "$REGION" --wait 2>&1 | sed 's/^/  /'
  EKSCTL_RC=${PIPESTATUS[0]}

  # ---------------------------------------------------------------------------
  # 🔴 THIS BLOCK EXISTS BECAUSE THE SCRIPT ONCE PRINTED "cluster deleted" WHILE
  # eksctl WAS PRINTING "Error: failed to delete cluster with nodegroup(s)".
  #
  # The old line was an UNCONDITIONAL `ok` — it reported the outcome of the step
  # rather than the outcome of the command, so the single most expensive delete in
  # the script could fail silently and the final inventory (which counts only
  # HOURLY-BILLED things) would still read 0 and look like success.
  #
  # THE UNDERLYING CAUSE, measured: step 7 terminates the nodes directly, and the
  # AWS VPC CNI attaches SECONDARY ENIs to each node to hand out pod IPs. Killing
  # the instance out from under the CNI leaves those ENIs behind in state
  # `available`, and an ENI in a subnet blocks that subnet from being deleted,
  # which fails the whole CloudFormation stack and orphans the VPC.
  #
  # Free — a VPC, a subnet and a detached ENI all cost nothing — which is exactly
  # why an inventory that only counts billable things cannot detect it.
  # ---------------------------------------------------------------------------
  if [[ "$EKSCTL_RC" -ne 0 ]]; then
    warn "eksctl exited $EKSCTL_RC — attempting the known CNI-ENI recovery"
    if [[ -n "$CLUSTER_VPC" && "$CLUSTER_VPC" != "None" ]]; then
      ORPHAN_ENIS=$(aws ec2 describe-network-interfaces --region "$REGION" \
                      --filters "Name=vpc-id,Values=$CLUSTER_VPC" "Name=status,Values=available" \
                      --query "NetworkInterfaces[?starts_with(Description,'aws-K8S-')].NetworkInterfaceId" \
                      --output text 2>/dev/null)
      for eni in $ORPHAN_ENIS; do
        try "orphaned CNI interface $eni" aws ec2 delete-network-interface \
            --region "$REGION" --network-interface-id "$eni"
      done

      # -----------------------------------------------------------------------
      # 🔴 THE SECOND BLOCKER, AND IT IS A DIFFERENT ONE. Clearing the ENIs let
      # the subnet delete and moved the failure UP a level, to the VPC itself:
      #   "The vpc 'vpc-...' has dependencies and cannot be deleted"
      # The dependency was `eks-cluster-sg-<cluster>-<id>` -- a security group
      # created by the EKS SERVICE, not by this CloudFormation stack, so the
      # stack has no record of it and will never remove it.
      #
      # ⚠️ It also does not match step 6's `${PREFIX}-*-sg` pattern, so that step
      # could not have caught it either. Matched here on the name EKS actually
      # uses, and `default` is excluded because a VPC's default group cannot be
      # deleted and disappears with the VPC.
      # -----------------------------------------------------------------------
      LEFTOVER_SGS=$(aws ec2 describe-security-groups --region "$REGION" \
                       --filters "Name=vpc-id,Values=$CLUSTER_VPC" \
                       --query "SecurityGroups[?GroupName!='default'].GroupId" \
                       --output text 2>/dev/null)
      for sg in $LEFTOVER_SGS; do
        try "leftover security group $sg" aws ec2 delete-security-group \
            --region "$REGION" --group-id "$sg"
      done
    fi
    # ⚠️ MEASURED: the first real recovery needed TWO passes, because clearing the
    # ENIs only moved the failure from the subnet up to the VPC. Each pass deletes
    # what the previous failure exposed, so the retry loops rather than assuming one
    # round is enough. Bounded at 3 -- a loop that cannot give up is its own outage.
    STACK_GONE=0
    for attempt in 1 2 3; do
      info "retrying the CloudFormation stack delete (pass $attempt)..."
      aws cloudformation delete-stack --region "$REGION" --stack-name "eksctl-${CLUSTER}-cluster" >/dev/null 2>&1
      if aws cloudformation wait stack-delete-complete --region "$REGION" \
           --stack-name "eksctl-${CLUSTER}-cluster" 2>/dev/null; then
        STACK_GONE=1; break
      fi
      info "pass $attempt did not finish — clearing whatever it exposed"
      for sg in $(aws ec2 describe-security-groups --region "$REGION" \
                    --filters "Name=vpc-id,Values=$CLUSTER_VPC" \
                    --query "SecurityGroups[?GroupName!='default'].GroupId" --output text 2>/dev/null); do
        try "leftover security group $sg" aws ec2 delete-security-group --region "$REGION" --group-id "$sg"
      done
    done
    if [[ "$STACK_GONE" -eq 1 ]]; then
      ok "cluster deleted (after ENI recovery)"
    else
      warn "CLUSTER STACK STILL NOT DELETED — the VPC is orphaned. Nothing is billing,"
      warn "but inspect: aws cloudformation describe-stack-events --stack-name eksctl-${CLUSTER}-cluster"
    fi
  else
    ok "cluster deleted"
  fi
else
  info "cluster $CLUSTER not found — already gone"
fi

# STEP 7 — ECR. No VPC dependency, so it could go anywhere; --force is required
# because a repository containing images refuses to delete without it.
# STEP 9 — the IAM policy created by hand for the load balancer controller.
#
# ⚠️ ORDER: this MUST come after the cluster delete. `eksctl delete cluster` removes
# the iamserviceaccount CloudFormation stack, and that stack owns the ROLE the policy
# is attached to. A policy with a live attachment cannot be deleted, so running this
# earlier fails with DeleteConflict.
#
# Not billable -- IAM is free. It is here because a policy nobody uses is one more
# thing granting 80 actions in this account, and because a rebuild recreates it and
# would otherwise hit EntityAlreadyExists.
log "9/10  IAM policy for the load balancer controller (free, but it accumulates)"
LBC_POLICY_ARN=$(aws iam list-policies --scope Local \
                   --query "Policies[?PolicyName=='AWSLoadBalancerControllerIAMPolicy'].Arn | [0]" \
                   --output text 2>/dev/null)
if [[ -n "$LBC_POLICY_ARN" && "$LBC_POLICY_ARN" != "None" ]]; then
  for r in $(aws iam list-entities-for-policy --policy-arn "$LBC_POLICY_ARN" \
               --query "PolicyRoles[].RoleName" --output text 2>/dev/null); do
    try "detach policy from role $r" aws iam detach-role-policy --role-name "$r" --policy-arn "$LBC_POLICY_ARN"
  done
  try "iam policy AWSLoadBalancerControllerIAMPolicy" aws iam delete-policy --policy-arn "$LBC_POLICY_ARN"
else
  info "no load balancer controller policy — already gone"
fi

log "10/10  ECR repositories"
for repo in "${ECR_REPOS[@]}"; do
  try "ecr $repo" aws ecr delete-repository --region "$REGION" --repository-name "$repo" --force
done

# --- prove it, rather than trust it ---------------------------------------------
# Every step above reported success on its own terms. That is not evidence: a delete
# that silently did nothing prints the same as one that worked, and the ALB in step 1
# is deleted by a controller whose success this script never observed.
inventory
