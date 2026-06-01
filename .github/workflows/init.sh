#!/bin/bash

set +e
docker buildx inspect "${GITHUB_ACTOR}" >/dev/null 2>&1
if test x"$?" = x"0"; then
	exit 0
fi

set -eux

reserved_space=$(echo -n "$reserved_space" | jq -r '."'"$runner"'" // "48GB"')
max_used_space=$(echo -n "$max_used_space" | jq -r '."'"$runner"'" // "64GB"')
min_free_space=$(echo -n "$min_free_space" | jq -r '."'"$runner"'" // "16GB"')
safety_free_space=$(echo -n "$safety_free_space" | jq -r '."'"$runner"'" // "8GB"')
trunk_max_used=$(echo -n "$trunk_max_used" | jq -r '."'"$runner"'" // "12GB"')
branch_max_used=$(echo -n "$branch_max_used" | jq -r '."'"$runner"'" // "16GB"')
unlabeled_max_used=$(echo -n "$unlabeled_max_used" | jq -r '."'"$runner"'" // "4GB"')
leaf_max_used=$(echo -n "$leaf_max_used" | jq -r '."'"$runner"'" // "20GB"')
cachemount_max_used=$(echo -n "$cachemount_max_used" | jq -r '."'"$runner"'" // "8GB"')

cat <<EOF > ./buildkitd.toml
[system]
  platformsCacheMaxAge = "504h"
[worker.oci]
  enabled = true
  rootless = false
  gc = true
  reservedSpace = "${reserved_space}"
  maxUsedSpace = "${max_used_space}"
  minFreeSpace = "${min_free_space}"

# Leaf: per-run outputs (build-bins, build-tests, install, complement,
# integ, smoke, packaging) plus the final image targets (static,
# docker, oci, complement-testee) we want to reuse across runs.
# Sized to hold ~1-2 full default-matrix runs of leaf images.
[[worker.oci.gcpolicy]]
  filters = ["label==cache.tier==leaf"]
  keepDuration = "1h"
  maxUsedSpace = "${leaf_max_used}"
  all = true

# Cache mounts: cargo registry, cargo git, rustup downloads. Slow-
# changing, expensive to refetch; given their own bucket so the
# leaf-tier churn doesn't evict them.
[[worker.oci.gcpolicy]]
  filters = ["type==cachemount"]
  keepDuration = "168h"
  maxUsedSpace = "${cachemount_max_used}"
  all = true

# Unlabeled records: intermediate exec layers, frontend bookkeeping,
# anything not explicitly tiered. Excludes cachemount via type filter
# so the cachemount bucket above is the sole evictor for those.
[[worker.oci.gcpolicy]]
  filters = ["label!=cache.tier==leaf,label!=cache.tier==branch,label!=cache.tier==trunk,type!=cachemount"]
  keepDuration = "24h"
  maxUsedSpace = "${unlabeled_max_used}"
  all = true

# Branch: cooked deps (deps-base), rocksdb compile, cargo. Expensive
# cargo_rust_feat_sys nodes. Day-scale age protection; size cap is
# the dominant trigger so small runners can churn this tier normally.
[[worker.oci.gcpolicy]]
  filters = ["label==cache.tier==branch"]
  keepDuration = "24h"
  maxUsedSpace = "${branch_max_used}"
  all = true

# Trunk: foundation layers (system, rust). Week-scale age protection
# so trunk survives any normal pressure; size cap is the backstop.
[[worker.oci.gcpolicy]]
  filters = ["label==cache.tier==trunk"]
  keepDuration = "168h"
  maxUsedSpace = "${trunk_max_used}"
  all = true

# Safety floor: under critical disk pressure, evict anything regardless
# of tier or age. Only relief valve when tier caps haven't sufficed.
[[worker.oci.gcpolicy]]
  minFreeSpace = "${safety_free_space}"
  all = true
EOF

docker buildx create \
	--bootstrap \
	--driver docker-container \
	--buildkitd-config ./buildkitd.toml \
	--name "${GITHUB_ACTOR}" \
	--buildkitd-flags "--allow-insecure-entitlement network.host"
