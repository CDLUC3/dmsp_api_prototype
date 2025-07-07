from cyclopts import App

from dmpworks.sqlmesh.sqlmesh import run_plan, run_tests

app = App(name="sqlmesh", help="SQLMesh utilities")


@app.command(name="test")
def test_cmd():
    """Run SQLMesh plan."""

    run_tests()


@app.command(name="plan")
def plan_cmd():
    """Run SQLMesh tests."""

    run_plan()
