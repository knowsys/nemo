import unittest
from nmo_python import load_string, NemoEngine, NemoOutputManager

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
            ['2', '<__Null#9223372036854775809>'],
            ['42', '<__Null#9223372036854775810>'],
            ['world', '<__Null#9223372036854775811>']]

        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()