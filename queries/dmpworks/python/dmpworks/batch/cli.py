from cyclopts import App

from dmpworks.batch.datacite import app as datacite_app
from dmpworks.batch.openalex_funders import app as openalex_funders_app
from dmpworks.batch.openalex_works import app as openalex_works_app
from dmpworks.batch.crossref_metadata import app as crossref_metadata_app
from dmpworks.batch.ror import app as ror_app
from dmpworks.batch.sqlmesh import app as sqlmesh_app

app = App(name="aws-batch", help="AWS Batch pipelines.")

app.command(datacite_app)
app.command(openalex_funders_app)
app.command(openalex_works_app)
app.command(crossref_metadata_app)
app.command(ror_app)
app.command(sqlmesh_app)
