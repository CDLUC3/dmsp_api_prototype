from cyclopts import App

from dmpworks.batch.cli import app as batch_app
from dmpworks.opensearch.cli import app as opensearch_app

from dmpworks.sqlmesh.cli import app as sqlmesh_app
from dmpworks.transform.cli import app as transform_app

cli = App(name="dmpworks", help="DMP Tool Related Works Command Line Tool.")

cli.command(opensearch_app)
cli.command(batch_app)
cli.command(sqlmesh_app)
cli.command(transform_app)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
