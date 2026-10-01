#!/usr/bin/env bash
# A build cache for mxg-static.yml that is kept in workflow artifacts.
#
# Why not actions/cache: GitHub only lets a run read caches written on its own branch or on the
# default branch. The default branch of this fork is upstream's `main`, where this workflow never
# runs, so every new `check/**` branch would start cold and every branch would keep its own copy
# until the 10 GB the repository gets is full. Artifacts can be read from any run, have no such
# quota in a public repository, and expire by themselves.
#
# The rule GitHub applies to caches is kept, with the deploy branch in the place of the default
# branch: a run takes a cache only from its own branch or from the deploy branch. So nothing a
# scratch branch uploads can end up in what the deploy branch builds.
#
#   mxg-cache.sh restore <exact-name> <fallback-prefix or ''> <path>...
#   mxg-cache.sh pack <path>...
#
# Paths are relative to $HOME. `restore` writes `hit=exact|partial|miss` to the step's outputs and
# never fails the job: a cache that cannot be had only costs time. `pack` writes
# $RUNNER_TEMP/mxg-cache.tar.zst, which actions/upload-artifact then stores under <exact-name>.

set -euo pipefail

archive="$RUNNER_TEMP/mxg-cache.tar.zst"

# Prints the id of the newest usable artifact whose name is, or starts with, $2.
find_artifact() {
	local mode=$1 name=$2 query='per_page=100'

	# Only an exact name can be asked of the API; for a prefix the newest hundred are looked through.
	if [ "$mode" = exact ]; then
		query+="&name=$name"
	fi

	gh api "repos/$GITHUB_REPOSITORY/actions/artifacts?$query" \
		| jq -r \
			--arg mode "$mode" \
			--arg name "$name" \
			--arg own "$GITHUB_REF_NAME" \
			--arg deploy "$DEPLOY_BRANCH" '
			[ .artifacts[]
			| select(.expired | not)
			| select(.workflow_run.head_branch == $own or .workflow_run.head_branch == $deploy)
			| select(if $mode == "exact" then .name == $name else (.name | startswith($name)) end)
			]
			| sort_by(.created_at) | last | .id // empty'
}

restore() {
	local exact=$1 prefix=$2 id hit=miss
	shift 2

	id=$(find_artifact exact "$exact" || true)
	if [ -n "$id" ]; then
		hit=exact
	elif [ -n "$prefix" ]; then
		id=$(find_artifact prefix "$prefix" || true)
		if [ -n "$id" ]; then
			hit=partial
		fi
	fi

	if [ -n "$id" ]; then
		echo "Restoring artifact $id ($hit)"
		# An artifact is always a zip; ours holds the one archive, stored uncompressed.
		if gh api "repos/$GITHUB_REPOSITORY/actions/artifacts/$id/zip" > "$archive.zip" \
			&& unzip -p "$archive.zip" | tar -C "$HOME" -I 'zstd -d -T0' -xf -; then
			ls -la "$archive.zip"
		else
			# Half a cache is worse than none: cargo trusts what it finds.
			echo "::warning::The cache could not be restored, building without it"
			hit=miss
			for path in "$@"; do
				sudo rm -rf "${HOME:?}/$path"
			done
		fi
		rm -f "$archive.zip"
	else
		echo "No cache named $exact${prefix:+ or starting with $prefix}"
	fi

	echo "hit=$hit" >> "$GITHUB_OUTPUT"
}

pack() {
	tar -C "$HOME" -I 'zstd -T0 -3' -cf "$archive" "$@"
	ls -la "$archive"
}

command=$1
shift
case "$command" in
	restore) restore "$@" ;;
	pack) pack "$@" ;;
	*)
		echo "unknown command: $command" >&2
		exit 2
		;;
esac
