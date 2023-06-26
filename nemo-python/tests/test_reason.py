import unittest
import csv
import os
import math
import sys
from os.path import dirname, exists, abspath, isfile

from nmo_python import load_string, load_file, NemoEngine


class TestStringMethods(unittest.TestCase):
    def test_example(self):
        rules = """
        data(1,2) .
        data(hi,42) .
        data(hello,world) .

        calculated(?x, !v) :- data(?y, ?x) .
        """

        engine = NemoEngine(load_string(rules))
        engine.reason()

        result = list(engine.result("calculated"))

        expected_result = [
            ["2", "<__Null#9223372036854775809>"],
            ["42", "<__Null#9223372036854775810>"],
            ["world", "<__Null#9223372036854775811>"],
        ]

        self.assertEqual(result, expected_result)


def stringify(value):
    if not type(value) is float:
        return str(value)

    if math.floor(value) == value:
        return str(math.floor(value))

    return str(value)


def end_to_end_test(path):
    def run_test(self):
        os.chdir(path)
        for file in os.listdir("."):
            if not isfile(file) or not file.endswith(".rls"):
                continue

            program_name = file.removesuffix(".rls")
            program = load_file(file)
            engine = NemoEngine(program)
            engine.reason()

            for relation in program.output_predicates():
                results_file_name = os.path.join(
                    program_name, f"{relation}.csv"
                )
                if not exists(results_file_name):
                    continue

                with open(results_file_name) as results_file:
                    expected = list(csv.reader(results_file))

                    for result in engine.result(relation):
                        result = [stringify(v) for v in result]

                        self.assertTrue(
                            result in expected,
                            f"error at {relation}",
                        )

    return run_test


class TestEndToEnd(unittest.TestCase):
    pass


test_cases_dir = os.path.join(
    dirname(__file__), "..", "..", "resources", "testcases"
)

for directory in os.listdir(test_cases_dir):
    path = abspath(os.path.join(test_cases_dir, directory))
    test_fun = classmethod(end_to_end_test(path))
    test_fun.__name__ = "test_" + directory
    setattr(TestEndToEnd, test_fun.__name__, test_fun)

if __name__ == "__main__":
    unittest.main(verbosity=2)
