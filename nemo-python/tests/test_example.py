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
            [2, "_:0"],
            [42, "_:1"],
            ["world", "_:2"],
            [Decimal("3.14"), "_:3"],
            [
                NemoLiteral("hello world", lang="en"),
                "_:4",
            ],
            ["circle", "_:5"],
        ]

        self.expected_serialized_result = [
            [
                '"2"^^<http://www.w3.org/2001/XMLSchema#integer>',
                "_:0",
            ],
            [
                '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
                "_:1",
            ],
            ["world", "_:2"],
            [
                '"3.14"^^<http://www.w3.org/2001/XMLSchema#decimal>',
                "_:3",
            ],
            [
                '"hello world"@en',
                "_:4",
            ],
            ["circle", "_:5"],
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
            "assignment": {"?x": Decimal('3.14'), "?y": "circle"},
            "subtraces": [
                {"fact": "data(3.14, circle)"},
                {
                    "rule": "interesting(?y) :- data(?x, ?y), interesting(?x) .",
                    "assignment": {"?x": "py", "?y": Decimal('3.14')},
                    "subtraces": [
                        {"fact": "data(py, 3.14)"},
                        {"fact": "interesting(py)"},
                    ],
                },
            ],
        }

        self.assertEqual(trace.dict(), expected_trace)


if __name__ == "__main__":
    unittest.main(verbosity=2)
