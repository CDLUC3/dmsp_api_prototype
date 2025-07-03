import argparse
import pathlib
from importlib.util import find_spec
from pathlib import Path

from sqlmesh.core.console import configure_console
from sqlmesh.core.context import Context
from sqlmesh.core.plan import Plan
from sqlmesh.core.test import ModelTextTestResult
from sqlmesh.utils import Verbosity


def sqlmesh_dir(module_name: str = "dmpworks.sqlmesh") -> pathlib.Path:
    spec = find_spec(module_name)
    if spec is None or not spec.origin:
        raise ModuleNotFoundError(module_name)

    return Path(spec.origin).parent


def run_plan(args) -> Plan:
    configure_console(ignore_warnings=False)
    ctx = Context(
        paths=[sqlmesh_dir()],
        load=True,
    )
    plan = ctx.plan(environment="prod", no_prompts=True, auto_apply=True)
    return plan


def run_tests(args) -> ModelTextTestResult:
    configure_console(ignore_warnings=False)
    ctx = Context(
        paths=[sqlmesh_dir()],
        load=True,
    )
    test_results: ModelTextTestResult = ctx.test(verbosity=Verbosity.VERY_VERBOSE)
    return test_results


def setup_parser(subparsers):
    # Plan command
    plan_parser = subparsers.add_parser("plan", description="Run SQLMesh plan.")
    plan_parser.set_defaults(func=run_plan)

    # Test command
    test_parser = subparsers.add_parser("test", description="Run SQLMesh tests.")
    test_parser.set_defaults(func=run_tests)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="sqlmesh")
    subparsers = parser.add_subparsers(dest="command", required=True)
    setup_parser(subparsers)
    args = parser.parse_args()
    args.func(args)
