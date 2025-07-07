from cyclopts import App


app = App(name="sqlmesh", help="SQLMesh utilities")


@app.command(name="test")
def test_cmd():
    """Run SQLMesh plan."""

    from dmpworks.sqlmesh.sqlmesh import run_tests

    run_tests()


@app.command(name="plan")
def plan_cmd():
    """Run SQLMesh tests."""

    from dmpworks.sqlmesh.sqlmesh import run_plan

    run_plan()
