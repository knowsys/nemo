import unittest
import tempfile
import os
import csv

from nmo_python import load_string, NemoEngine, NemoOutputManager


class TestOutputManager(unittest.TestCase):
    def test_example_output(self):
        rules = """
        data(1,2) .
        data(hi,42) .
        data(hello,world) .

        calculated(?x, !v) :- data(?y, ?x) .
        """

        engine = NemoEngine(load_string(rules))
        engine.reason()

        expected_result = [
            ["2", "<__Null#9223372036854775809>"],
            ["42", "<__Null#9223372036854775810>"],
            ["world", "<__Null#9223372036854775811>"],
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_manager = NemoOutputManager(tmp_dir)
            engine.write_result("calculated", output_manager)

            results_file_name = os.path.join(tmp_dir, "calculated.csv")
            self.assertTrue(os.path.exists(results_file_name))

            with open(results_file_name) as results_file:
                results = list(csv.reader(results_file))
                self.assertEqual(results, expected_result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
