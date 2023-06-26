import unittest
import csv
import os
import math
from os.path import dirname, exists, abspath, isfile

from nmo_python import load_file, NemoEngine


def stringify(value):
    if not isinstance(value, float):
        return str(value)

    if math.floor(value) == value:
        return str(math.floor(value))

    return str(value)


def end_to_end_test(test_path):
    def run_test(self):
        os.chdir(test_path)
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
                            f"unexpected {result} in {relation}",
                        )

                        expected.remove(result)

                    self.assertTrue(
                        len(expected) == 0,
                        f"missing results in {relation}: {expected}",
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
