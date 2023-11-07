import unittest
import tempfile
import csv
import os
import math
from os.path import dirname, exists, abspath, isfile, isdir

from nmo_python import load_file, NemoEngine, NemoOutputManager


def stringify(value):
    if not isinstance(value, float):
        return str(value)

    if math.floor(value) == value:
        return str(math.floor(value))

    return str(value)


def run_blackbox_test(self, file):
    os.chdir(self.path)

    program = load_file(file)
    program_name = file.removesuffix(".rls")
    engine = NemoEngine(program)
    engine.reason()

    for relation in program.output_predicates():
        expected_results_file_name = os.path.join(program_name, f"{relation}.csv")

        if not exists(expected_results_file_name):
            continue

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_manager = NemoOutputManager(tmp_dir)
            engine.write_result(f"{relation}", output_manager)

            with open(os.path.join(tmp_dir, f"{relation}.csv")) as results_file:
                results = list(csv.reader(results_file))

            with open(expected_results_file_name) as expected_results_file:
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


def generate_test(file):
    program_name = file.removesuffix(".rls")
    test_name = "test_" + program_name

    return test_name, classmethod(lambda self: run_blackbox_test(self, file))


def visit_path(test_path, loader, tests, subgroup_path=[]):
    os.chdir(test_path)
    dir_entries = os.listdir(test_path)
    subgroup_path.append(os.path.basename(test_path))

    for subdir in filter(isdir, dir_entries):
        visit_path(os.path.join(test_path, subdir), loader, tests, subgroup_path)
        os.chdir(test_path)

    rules_files = list(filter(lambda f: isfile(f) and f.endswith(".rls"), dir_entries))

    if rules_files.count == 0:
        return

    class_name = "_".join(subgroup_path)
    test_case = type(class_name, (unittest.TestCase,), {"path": test_path})

    for file in rules_files:
        name, method = generate_test(file)
        setattr(test_case, name, method)

    tests.addTests(loader.loadTestsFromTestCase(test_case))
    subgroup_path.pop()


def load_tests(loader, tests, pattern):
    test_cases_dir = os.path.join(dirname(__file__), "..", "..", "resources", "testcases")

    visit_path(abspath(test_cases_dir), loader, tests)
    return tests


if __name__ == "__main__":
    unittest.main(verbosity=2)
