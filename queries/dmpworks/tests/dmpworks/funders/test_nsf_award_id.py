import csv
import os

from dmpworks.funders.nsf_award_id import NSFAwardID
from queries.dmpworks.tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path()


def test_parse_nsf_award_id():
    inputs = []
    expected = []
    data_path = os.path.join(FIXTURES_FOLDER, "nsf_award_ids.csv")

    # Load test data
    convert = lambda s: s.strip() or None  # Convert empty strings to None
    with open(data_path, mode="r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = row["text"]
            inputs.append(text)
            expected.append(
                NSFAwardID(
                    text=text,
                    org_id=convert(row["org_id"]),
                    award_id=convert(
                        row["award_id"],
                    ),
                )
            )

    # Check that award IDs parse
    print(f"test_parse_nsf_award_id:")
    for inp, exp in zip(inputs, expected):
        print(f"input: {inp}")
        print(f"\texpected: {exp}")
        parsed = NSFAwardID.parse(inp)
        print(f"\tparsed: {parsed}")
        assert exp.award_id == parsed.award_id
