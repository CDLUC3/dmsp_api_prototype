from cyclopts import App

from dmpworks.batch.datacite import app as datacite_app
from dmpworks.batch.openalex_funders import app as openalex_funders_app
from dmpworks.batch.openalex_works import app as openalex_works_app

app = App(name="aws-batch", help="AWS Batch pipelines.")

app.command(datacite_app)
app.command(openalex_funders_app)
app.command(openalex_works_app)
