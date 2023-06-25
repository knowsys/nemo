import unittest
import csv
import os
import math
import sys

from nmo_python import load_string, load_file, NemoEngine, NemoOutputManager


class TestStringMethods(unittest.TestCase):
    def test_reason(self):
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


class TestEndToEnd(unittest.TestCase):
    def __init__(self, path, *args, **kwargs):
        super(TestEndToEnd, self).__init__(*args, **kwargs)
        self.path = path

    def test_end_to_end(self):
        os.chdir(self.path)
        for file in os.listdir("."):
            if not os.path.isfile(file) or not file.endswith(".rls"):
                continue

            program_name = file.removesuffix(".rls")
            program = load_file(file)
            engine = NemoEngine(program)
            engine.reason()

            for relation in program.output_predicates():
                results_file_name = f"{program_name}/{relation}.csv"
                if not os.path.exists(results_file_name):
                    continue

                with open(results_file_name) as results_file:
                    expected = list(csv.reader(results_file))

                    for result in engine.result(relation):
                        result = [stringify(v) for v in result]

                        self.assertIn(
                            result,
                            expected,
                            f"error at {self.path}/{relation}",
                        )


suite = unittest.TestSuite()
suite.addTest(TestStringMethods("test_reason"))

test_cases = sys.argv[-1]

for directory in os.listdir(test_cases):
    path = os.path.abspath(os.path.join(test_cases, directory))
    suite.addTest(TestEndToEnd(path, "test_end_to_end"))

runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
