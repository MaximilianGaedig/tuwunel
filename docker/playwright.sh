#!/bin/bash
set -eo pipefail

BASEDIR=$(dirname "$0")

CI="${CI:-false}"
CI_VERBOSE="${CI_VERBOSE_ENV:-false}"
CI_VERBOSE_ENV="${CI_VERBOSE_ENV:-$CI_VERBOSE}"

default_cargo_profile="test"
default_feat_set="all"
default_rust_toolchain="nightly"
default_rust_target="x86_64-unknown-linux-gnu"
default_sys_name="debian"
default_sys_target="x86_64-v1-linux-gnu"
default_sys_version="testing-slim"

default_playwright_run=".*"
default_playwright_skip=""
default_playwright_shard="1/1"
default_playwright_count=1
default_playwright_workers=8
default_playwright_retries=1

run="${1:-$default_playwright_run}"

# Skip-list: alternation of substrings matched against test titles. Mirrors
# the equivalent block in docker/complement.sh. Anything added here is a
# permanent skip for the suite.
skip="${default_playwright_skip}"

set -a
cargo_profile="${cargo_profile:-$default_cargo_profile}"
feat_set="${feat_set:-$default_feat_set}"
rust_target="${rust_target:-$default_rust_target}"
rust_toolchain="${rust_toolchain:-$default_rust_toolchain}"
sys_name="${sys_name:-$default_sys_name}"
sys_target="${sys_target:-$default_sys_target}"
sys_version="${sys_version:-$default_sys_version}"
set +a

envs=""
envs="$envs -e CI=true"
envs="$envs -e playwright_run=${playwright_run:-$run}"
envs="$envs -e playwright_skip=${playwright_skip:-$skip}"
envs="$envs -e playwright_shard=${playwright_shard:-$default_playwright_shard}"
envs="$envs -e playwright_count=${playwright_count:-$default_playwright_count}"
envs="$envs -e playwright_workers=${playwright_workers:-$default_playwright_workers}"
envs="$envs -e playwright_retries=${playwright_retries:-$default_playwright_retries}"

set -x
tester_image="playwright-tester--${sys_name}--${sys_version}--${sys_target}"
testee_image="playwright-testee--${cargo_profile}--${rust_toolchain}--${rust_target}--${feat_set}--${sys_name}--${sys_version}--${sys_target}"
shard_slug=$(printf '%s' "${playwright_shard:-$default_playwright_shard}" | tr '/' '_')
name="playwright_tester__${sys_name}__${sys_version}__${sys_target}__${shard_slug}"
sock="/var/run/docker.sock"
arg="--name $name -v $sock:$sock --network=host $envs $tester_image"
set +x

if test "$CI_VERBOSE_ENV" = "true"; then
	date
	env
fi

docker rm -f "$name" 2>/dev/null || true

arg="-d $arg"
cid=$(docker run $arg)

if test "$CI" = "true"; then
	echo -n "$cid" > "$name"
fi

result_src="$cid:/playwright/out/results.json"
result_dst="tests/playwright/results.json"
output_src="$cid:/playwright/out/output.log"
output_dst="tests/playwright/output.log"
mkdir -p tests/playwright

extract_output() {
	docker cp "$output_src" "$output_dst" 2>/dev/null || true
}
extract_results() {
	docker cp "$result_src" "$result_dst" 2>/dev/null || true
}

trap 'extract_output; extract_results; set +x; date; echo -e "\033[1;41;37mERROR\033[0m"' ERR
trap 'docker container stop $cid; extract_output; extract_results' INT
docker logs -f "$cid"
docker wait "$cid" >/dev/null 2>&1 || true

extract_results
extract_output

echo -e "\033[1;42;30mACCEPT\033[0m"
