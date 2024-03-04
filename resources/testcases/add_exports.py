from os import path, listdir

testcases_dir = path.abspath(path.join(__file__, ".."))

def visit_test_case_dir(test_case_dir):
    for node in listdir(test_case_dir):
        if path.isdir(path.join(test_case_dir, node)):
            visit_test_case_dir(path.join(test_case_dir, node))
            
        elif node.endswith(".rls"):
            visit_test_case(test_case_dir, node)

def visit_test_case(case_dir, case_file):
    print(f"rules file: {path.join(case_dir, case_file)}")

    export_dir = path.join(case_dir, case_file.removesuffix(".rls"))

    for export_file in listdir(export_dir):
        if export_file.endswith(".csv"):
            export_stmt = f'\t@export {export_file.removesuffix(".csv")} :- csv {{}}.'

        elif export_file.endswith(".tsv"):
            export_stmt = f'\t@export {export_file.removesuffix(".tsv")} :- tsv {{}}.'

        else:
            continue

        print(export_stmt)



visit_test_case_dir(testcases_dir)