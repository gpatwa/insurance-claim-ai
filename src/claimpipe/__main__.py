"""Single container image, four roles: `python -m claimpipe {api|worker|relay|notifier}`.

The same image runs every process; the role (and adapter config) is the only difference between
deployments — the crux of the portability story.
"""

from __future__ import annotations

import asyncio
import sys

VALID_ROLES = frozenset({"api", "worker", "relay", "notifier"})


def run(role: str) -> None:
    if role not in VALID_ROLES:
        raise SystemExit(f"unknown role {role!r}; expected one of {sorted(VALID_ROLES)}")

    if role == "api":
        from claimpipe.api.main import main as api_main

        api_main()
    elif role == "worker":
        from claimpipe.temporal.worker import main as worker_main

        asyncio.run(worker_main())
    elif role == "relay":
        from claimpipe.relay import main as relay_main

        asyncio.run(relay_main())
    elif role == "notifier":
        from claimpipe.notifier import main as notifier_main

        asyncio.run(notifier_main())


def main() -> None:
    role = sys.argv[1] if len(sys.argv) > 1 else "worker"
    run(role)


if __name__ == "__main__":
    main()
