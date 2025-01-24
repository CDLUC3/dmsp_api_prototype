import os
import unittest
from unittest.mock import patch

import vcr

from dmptool_workflows.config import project_path
from dmptool_workflows.dmp_match_workflow.funder_apis import (
    _pubmed_ids_to_dois,
    nih_core_project_to_appl_ids,
    nih_fetch_award_publication_dois,
    nsf_fetch_award_publication_dois,
    pubmed_ids_to_dois,
)

FIXTURES_FOLDER = project_path("dmp_match_workflow", "tests", "fixtures")


class TestFunderAPIs(unittest.TestCase):
    def test_nsf_fetch_award_publications(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nsf_fetch_award_publications.yaml")):
            results = nsf_fetch_award_publication_dois("0932263")

    def test_nih_core_project_to_appl_ids(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_core_project_to_appl_ids.yaml")):
            results = nih_core_project_to_appl_ids("5P41GM108569-08")
            self.assertSetEqual({10438551, 10438555, 10438547, 10438553, 10438548, 10438552, 10438554}, set(results))

    def test_nih_fetch_award_publication_dois(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "nih_fetch_award_publication_dois.yaml")):
            results = nih_fetch_award_publication_dois("10808782")
            self.assertListEqual([{"doi": "10.3390/ph17101335", "pmcid": "PMC11509978", "pmid": "39458976"}], results)

    def test_pubmed_ids_to_dois(self):
        with patch("dmptool_workflows.dmp_match_workflow.funder_apis._pubmed_ids_to_dois") as func:
            func.return_value = []
            pubmed_ids_to_dois([], "pmid")
            self.assertEqual(0, func.call_count)

        with patch("dmptool_workflows.dmp_match_workflow.funder_apis._pubmed_ids_to_dois") as func:
            func.return_value = []
            pubmed_ids_to_dois([38096378] * 200, "pmid")
            self.assertEqual(1, func.call_count)

        with patch("dmptool_workflows.dmp_match_workflow.funder_apis._pubmed_ids_to_dois") as func:
            func.return_value = []
            pubmed_ids_to_dois([38096378] * 201, "pmid")
            self.assertEqual(2, func.call_count)

        with patch("dmptool_workflows.dmp_match_workflow.funder_apis._pubmed_ids_to_dois") as func:
            func.return_value = []
            pubmed_ids_to_dois([38096378] * 300, "pmid")
            self.assertEqual(2, func.call_count)

        with patch("dmptool_workflows.dmp_match_workflow.funder_apis._pubmed_ids_to_dois") as func:
            func.return_value = []
            pubmed_ids_to_dois([38096378] * 400, "pmid")
            self.assertEqual(2, func.call_count)

        with patch("dmptool_workflows.dmp_match_workflow.funder_apis._pubmed_ids_to_dois") as func:
            func.return_value = []
            pubmed_ids_to_dois([38096378] * 401, "pmid")
            self.assertEqual(3, func.call_count)

    def test__pubmed_ids_to_dois(self):
        with vcr.use_cassette(os.path.join(FIXTURES_FOLDER, "_pubmed_ids_to_dois.yaml")):
            # PubMed IDs
            results = _pubmed_ids_to_dois([39747675, 38286823, 38096378], "pmid")
            results.sort(key=lambda x: x["pmid"])
            self.assertListEqual(
                [
                    {
                        "pmid": "38096378",
                        "pmcid": "PMC10874502",
                        "doi": "10.1021/acs.jproteome.3c00430",
                    },
                    {
                        "pmid": "38286823",
                        "pmcid": "PMC10990768",
                        "doi": "10.1021/jasms.3c00435",
                    },
                    {
                        "pmid": "39747675",
                        "pmcid": None,
                        "doi": None,
                    },
                ],
                results,
            )

            # PubMed PMC IDs
            results = _pubmed_ids_to_dois([10990768, 10874502, 10908861], "pmcid")
            results.sort(key=lambda x: x["pmcid"])
            self.assertListEqual(
                [
                    {
                        "pmid": "38096378",
                        "pmcid": "PMC10874502",
                        "doi": "10.1021/acs.jproteome.3c00430",
                    },
                    {
                        "pmid": "38431639",
                        "pmcid": "PMC10908861",
                        "doi": "10.1038/s41467-024-46240-9",
                    },
                    {
                        "pmid": "38286823",
                        "pmcid": "PMC10990768",
                        "doi": "10.1021/jasms.3c00435",
                    },
                ],
                results,
            )
