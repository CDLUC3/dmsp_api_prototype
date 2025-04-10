import csv
import os
import unittest

from dmptool_workflows.config import project_path
from dmptool_workflows.dmp_match_workflow.funder_apis import nih_core_project_to_appl_ids
from dmptool_workflows.dmp_match_workflow.funder_ids import NIHAwardID, NSFAwardID

FIXTURES_FOLDER = project_path("dmp_match_workflow", "tests", "fixtures")


class TestNIHIdentifiers(unittest.TestCase):
    def test_parse_nih_award_id(self):
        # # print(f"input: {inp}")
        # # print(f"\texpected: {exp}")
        # parsed = NIHAwardID.parse("ZIA AI000483")
        # print(f"\tparsed: {parsed}")
        # # self.assertEqual(exp, parsed)
        #
        # # Convert award ID to application IDs
        # appl_ids = nih_core_project_to_appl_ids(
        #     appl_type_code=parsed.application_type,
        #     activity_code=parsed.activity_code,
        #     ic_code=parsed.institute_code,
        #     serial_num=parsed.serial_number,
        #     support_year=parsed.support_year,
        #     suffix_code=parsed.other_suffixes,
        # )
        # print(f"\tappl_ids: {appl_ids}")

        inputs = []
        expected = []
        data_path = os.path.join(FIXTURES_FOLDER, "nih_award_ids.csv")

        # Load test data
        convert = lambda s: s.strip() or None  # Convert empty strings to None
        with open(data_path, mode="r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                inputs.append(row["text"])
                expected.append(
                    NIHAwardID(
                        application_type=convert(row["application_type"]),
                        activity_code=convert(row["activity_code"]),
                        institute_code=convert(row["institute_code"]),
                        serial_number=convert(row["serial_number"]),
                        support_year=convert(row["support_year"]),
                        other_suffixes=convert(row["other_suffixes"]),
                    )
                )

        # Check that award IDs parse
        print(f"test_parse_nih_award_id")
        for inp, exp in zip(inputs, expected):
            parsed = NIHAwardID.parse(inp)
            self.assertEqual(exp, parsed)

            # Convert award ID to application IDs
            appl_ids = nih_core_project_to_appl_ids(
                appl_type_code=parsed.application_type,
                activity_code=parsed.activity_code,
                ic_code=parsed.institute_code,
                serial_num=parsed.serial_number,
                support_year=parsed.support_year,
                suffix_code=parsed.other_suffixes,
            )
            if len(appl_ids) == 0:
                print(f"input: {inp}")
                # print(f"\texpected: {exp}")
                print(f"\tparsed: {parsed}")
                print(f"\tappl_ids: {appl_ids}")


class TestNSFIdentifiers(unittest.TestCase):
    def test_parse_nsf_award_id(self):
        inputs = []
        expected = []
        data_path = os.path.join(FIXTURES_FOLDER, "nsf_award_ids.csv")

        # Load test data
        convert = lambda s: s.strip() or None  # Convert empty strings to None
        with open(data_path, mode="r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                inputs.append(row["text"])
                expected.append(NSFAwardID(org_id=convert(row["org_id"]), award_id=convert(row["award_id"])))

        # Check that award IDs parse
        print(f"test_parse_nsf_award_id:")
        for inp, exp in zip(inputs, expected):
            print(f"input: {inp}")
            print(f"\texpected: {exp}")
            parsed = NSFAwardID.parse(inp)
            print(f"\tparsed: {parsed}")
            self.assertEqual(exp.award_id, parsed.award_id)
