"""Opt-in fault injection used by the crash-recovery tests and the demo.

Disabled unless ``IMPORTGUARD_FAULTS`` is set, e.g.
``IMPORTGUARD_FAULTS="before_commit=crash"`` or ``"after_staging=sleep:5"``.
Actions: ``crash`` (hard process exit, no cleanup), ``raise`` (exception), ``sleep:<seconds>``.
"""

from __future__ import annotations

import logging
import os
import time

ENV_VAR = "IMPORTGUARD_FAULTS"
CRASH_EXIT_CODE = 86
CHECKPOINTS = frozenset({"after_claim", "after_staging", "before_commit", "after_commit"})

log = logging.getLogger(__name__)


class InjectedFault(RuntimeError):
    pass


def _actions() -> dict[str, str]:
    spec = os.environ.get(ENV_VAR, "").strip()
    if not spec:
        return {}
    actions: dict[str, str] = {}
    for item in spec.split(","):
        name, _, action = item.strip().partition("=")
        if name not in CHECKPOINTS:
            raise ValueError(f"{ENV_VAR}: unknown checkpoint {name!r}")
        actions[name] = action
    return actions


def checkpoint(name: str) -> None:
    action = _actions().get(name)
    if action is None:
        return
    log.warning("fault injection: %s -> %s", name, action)
    if action == "crash":
        os._exit(CRASH_EXIT_CODE)
    if action == "raise":
        raise InjectedFault(f"injected fault at {name}")
    if action.startswith("sleep:"):
        time.sleep(float(action.removeprefix("sleep:")))
        return
    raise ValueError(f"{ENV_VAR}: unknown action {action!r}")
