#!/usr/bin/env bash
# fleet_par: run one task function over many items concurrently, with
# captured per-item output and single-threaded verdict collection.
# Source this; used by fleetrun.sh and orchestrate.sh for the pre/post
# stages (provision, listeners, haproxy, snapshot-pre, prune-post, gc,
# teardown) -- PROTOCOL.md §33.
#
#   fleet_par <max_jobs> <task_fn> <item>...
#
# Each item runs as "task_fn <item>" in a background subshell, stdout+
# stderr captured to a private per-item file; at most <max_jobs> run at
# once, throttled by reaping the OLDEST unfinished task first (simple,
# deterministic; with homogeneous ssh-bound tasks the loss vs. an
# any-order reap is noise -- and bash's `wait -n` would forget the
# specific pid's status anyway). When every task has finished, results
# land in three assoc arrays keyed by item:
#
#   fleet_par_rc[item]    task exit status
#   fleet_par_out[item]   full captured output (trailing newlines trimmed)
#   fleet_par_data[item]  contents of $FLEET_PAR_DATA, a per-task scratch
#                         file each task may write to -- a side channel
#                         for structured verdicts that must not ride the
#                         display output
#
# fleet_par prints NOTHING and returns 0 (nonzero only if its scratch
# dir cannot be made): replaying output, translating rcs into verdicts,
# and mutating shared state belong in the CALLER's collector loop,
# single-threaded on purpose. Task functions must be pure -- a background
# subshell's writes to globals die with it, so a task communicates by
# rc, output, and data file ONLY. Items must be unique (they key the
# result arrays).

declare -A fleet_par_rc=() fleet_par_out=() fleet_par_data=()

fleet_par() {
    local -                      # confine the set±e dance (§17 invariant)
    set +e
    local max="$1" fn="$2"
    shift 2
    fleet_par_rc=(); fleet_par_out=(); fleet_par_data=()
    if (( $# == 0 )); then
        return 0
    fi
    local tmp
    tmp=$(mktemp -d "${TMPDIR:-/tmp}/stevedore-par.XXXXXX") || {
        echo "ERROR: fleet_par: cannot create scratch dir" >&2
        return 1
    }
    local -a items=( "$@" ) pids=() rcs=()
    local i reap=0
    for (( i = 0; i < ${#items[@]}; i++ )); do
        if (( i - reap >= max )); then
            wait "${pids[reap]}"
            rcs[reap]=$?
            reap=$(( reap + 1 ))
        fi
        FLEET_PAR_DATA="$tmp/$i.data" "$fn" "${items[i]}" \
            > "$tmp/$i.out" 2>&1 &
        pids[i]=$!
    done
    for (( ; reap < ${#items[@]}; reap++ )); do
        wait "${pids[reap]}"
        rcs[reap]=$?
    done
    for (( i = 0; i < ${#items[@]}; i++ )); do
        fleet_par_rc[${items[i]}]=${rcs[i]}
        fleet_par_out[${items[i]}]=$(cat "$tmp/$i.out" 2>/dev/null)
        fleet_par_data[${items[i]}]=$(cat "$tmp/$i.data" 2>/dev/null)
    done
    rm -rf -- "$tmp"
    return 0
}
