from os import path, listdir

testcases_dir = path.abspath(path.join(__file__, ".."))
added = []

def visit_test_case_dir(test_case_dir):
    for node in listdir(test_case_dir):
        if path.isdir(path.join(test_case_dir, node)):
            visit_test_case_dir(path.join(test_case_dir, node))
            
        elif node.endswith(".rls"):
            visit_test_case(test_case_dir, node)

def visit_test_case(case_dir, case_file):
    print(f"rules file: {path.join(case_dir, case_file)}")

    export_dir = path.join(case_dir, case_file.removesuffix(".rls"))
    existing_exports = []

    with open(path.join(case_dir, case_file)) as rules_file:
        for line in rules_file.readlines():
            if line.startswith("@export"):
                predicate = line.removeprefix("@export").split(sep=":-")[0].strip()
                existing_exports.append(predicate)

            if line.startswith("@declare"):
                print("\tfound @declare")

    for export_file in listdir(export_dir):
        export_type = None
        export_predicate = None

        if export_file.endswith(".csv"):
            export_type = "csv"
            export_predicate = export_file.removesuffix(".csv")

        elif export_file.endswith(".tsv"):
            export_type = "tsv"
            export_predicate = export_file.removesuffix(".tsv")

        else:
            continue

        if export_predicate in existing_exports:
            continue

        export_stmt = f'@export {export_predicate} :- {export_type} {{}}.\n'

        print(f'\tadding: {export_predicate} ({export_type})')
        added.append(export_predicate)

        with open(path.join(case_dir, case_file), mode="a") as rules_file:
            rules_file.write("\n")
            rules_file.write(export_stmt)

visit_test_case_dir(testcases_dir)

print(f"Summary: added {added}")
