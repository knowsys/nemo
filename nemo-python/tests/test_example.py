import unittest
import tempfile
import os
import csv
from decimal import Decimal

from nmo_python import (
    load_string,
    NemoEngine,
    NemoOutputManager,
    NemoLiteral,
)


class TestExample(unittest.TestCase):
    def setUp(self):
        self.rules = """
        data(1, 2) .
        data(hi, 42) .
        data(hello, world) .
        data(py, 3.14) .
        data(msg, "hello world"@en) .
        data(3.14, circle).

        calculated(?x, !v) :- data(?y, ?x) .

        interesting(py).
        interesting(msg).

        interesting(?x) :- data(?x, ?y), interesting(?y).
        interesting(?y) :- data(?x, ?y), interesting(?x).
        """

        self.engine = NemoEngine(load_string(self.rules))
        self.engine.reason()

        self.expected_api_result = [
            ["<world>", "_:0"],
            ["<circle>", "_:1"],
             [
                NemoLiteral("hello world", lang="en"),
                "_:2",
            ],
            [2, "_:3"],
            [42, "_:4"],
            [3.14, "_:5"],
        ]

        self.expected_serialized_result = [
            ["world", "_:0"],
            ["circle", "_:1"],
            [
                '"hello world"@en',
                "_:2",
            ],
            [
                "2",
                "_:3",
            ],
            [
                "42",
                "_:4",
            ],
            [
                '"3.14"^^<http://www.w3.org/2001/XMLSchema#double>',
                "_:5",
            ],
        ]

    def test_result(self):
        result = list(self.engine.result("calculated"))
        self.assertEqual(result, self.expected_api_result)

    def test_output(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_manager = NemoOutputManager(tmp_dir)
            self.engine.write_result("calculated", output_manager)

            results_file_name = os.path.join(tmp_dir, "calculated.csv")
            self.assertTrue(os.path.exists(results_file_name))

            with open(results_file_name) as results_file:
                results = list(csv.reader(results_file))
                self.assertEqual(results, self.expected_serialized_result)

    def test_trace(self):
        trace = self.engine.trace("interesting(circle)")
        expected_trace = {
            "rule": "interesting(?y) :- data(?x, ?y), interesting(?x) .",
            "assignment": {"?x": 3.14, "?y": "<circle>"},
            "subtraces": [
                {"fact": "data(\"3.14\"^^<http://www.w3.org/2001/XMLSchema#double>, circle)"},
                {
                    "rule": "interesting(?y) :- data(?x, ?y), interesting(?x) .",
                    "assignment": {"?x": "<py>", "?y": 3.14},
                    "subtraces": [
                        {"fact": "data(py, \"3.14\"^^<http://www.w3.org/2001/XMLSchema#double>)"},
                        {"fact": "interesting(py)"},
                    ],
                },
            ],
        }

        self.assertEqual(trace.dict(), expected_trace)


if __name__ == "__main__":
    unittest.main(verbosity=2)
