"""Exercise fatal callbacks outside the parent gtest/userver process."""

import os
import subprocess
import sys


def main() -> int:
    task_pool, other_pools, operators = sys.argv[1:]
    cases = (
        (task_pool, "TaskPool.UnhandledFailureTerminatesProcess"),
        (other_pools, "PriorityTaskPool.UnhandledFailureTerminatesProcess"),
        (other_pools, "DelayPool.UnhandledFailureTerminatesProcess"),
        (operators, "GraphParityReview.UnhandledFailureTerminatesProcess"),
    )
    failures = []
    for executable, test in cases:
        for mode in ("error", "cancelled-error"):
            environment = dict(os.environ, SERVICELIB_FATAL_CALLBACK_CHILD=mode)
            result = subprocess.run(
                [executable, f"--gtest_filter={test}"],
                env=environment,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            diagnostic = (
                "servicelib: unhandled callback exception: "
                "callback-failure-process-probe"
            )
            if result.returncode != 2 or diagnostic not in result.stderr:
                failures.append(f"{test}/{mode}: exit={result.returncode}\n"
                                f"{result.stdout}\n{result.stderr}")
            else:
                print(f"PASS {test}/{mode}: process exit 2", flush=True)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
