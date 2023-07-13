import unittest
import tempfile
import csv
import os
import math
from os.path import dirname, exists, abspath, isfile

from nmo_python import load_file, NemoEngine, NemoOutputManager


def stringify(value):
    if not isinstance(value, float):
        return str(value)

    if math.floor(value) == value:
        return str(math.floor(value))

    return str(value)


def generate_test(file):
    program_name = file.removesuffix(".rls")
    test_name = "test_" + program_name

    def run_test(self):
        os.chdir(self.path)

        program = load_file(file)
        engine = NemoEngine(program)
        engine.reason()

        for relation in program.output_predicates():
            expected_results_file_name = \
                    os.path.join(program_name, f"{relation}.csv")

            if not exists(expected_results_file_name):
                continue

            with tempfile.TemporaryDirectory() as tmp_dir:
                output_manager = NemoOutputManager(tmp_dir)
                engine.write_result(f"{relation}", output_manager)

                with open(os.path.join(tmp_dir, f"{relation}.csv")) \
                        as results_file:
                    results = list(csv.reader(results_file))

                    with open(expected_results_file_name) \
                            as expected_results_file:
                        expected = list(csv.reader(expected_results_file))

                        for result in results:
                            self.assertTrue(
                                result in expected,
                                f"unexpected {result} in {relation}",
                            )

                            expected.remove(result)

                        self.assertTrue(
                            len(expected) == 0,
                            f"missing results in {relation}: {expected}",
                        )

    return test_name, classmethod(run_test)


def end_to_end_test_case(test_path):
    class_name = os.path.basename(test_path)
    test_case = type(class_name, (unittest.TestCase,), {"path": test_path})

    for file in os.listdir(test_path):
        if not isfile(os.path.join(test_path, file)):
            continue

        if not file.endswith(".rls"):
            continue

        name, method = generate_test(file)
        setattr(test_case, name, method)

    return test_case


def load_tests(loader, tests, pattern):
    test_cases_dir = os.path.join(
        dirname(__file__), "..", "..", "resources", "testcases"
    )

    for directory in os.listdir(test_cases_dir):
        path = abspath(os.path.join(test_cases_dir, directory))
        test_case = end_to_end_test_case(path)
        tests.addTests(loader.loadTestsFromTestCase(test_case))

    return tests


if __name__ == "__main__":
    unittest.main(verbosity=2)
