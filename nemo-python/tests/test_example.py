import unittest
import tempfile
import os
import csv
from decimal import Decimal

from nmo_python import load_string, NemoEngine, NemoOutputManager, NemoLiteral


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
            [2, "__Null#9223372036854775809"],
            [42, "__Null#9223372036854775810"],
            ["world", "__Null#9223372036854775811"],
            [Decimal("3.14"), "__Null#9223372036854775812"],
            [
                NemoLiteral("hello world", lang="en"),
                "__Null#9223372036854775813",
            ],
            ["circle", "__Null#9223372036854775814"],
        ]

        self.expected_serialized_result = [
            [
                '"2"^^<http://www.w3.org/2001/XMLSchema#integer>',
                "<__Null#9223372036854775809>",
            ],
            [
                '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
                "<__Null#9223372036854775810>",
            ],
            ["world", "<__Null#9223372036854775811>"],
            [
                '"3.14"^^<http://www.w3.org/2001/XMLSchema#decimal>',
                "<__Null#9223372036854775812>",
            ],
            [
                '"hello world"@en',
                "<__Null#9223372036854775813>",
            ],
            ["circle", "<__Null#9223372036854775814>"],
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
            "rule": "interesting(circle) :- data(3.14, circle), interesting(3.14) .",
            "subtraces": [
                {"fact": "data(3.14, circle)"},
                {
                    "rule": "interesting(3.14) :- data(py, 3.14), interesting(py) .",
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
