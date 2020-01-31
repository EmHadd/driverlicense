import pytest


def compute_sum(a,b):
    return a+b

@pytest.mark.case_a
def test_sum():
    assert compute_sum(3, 4) == 5


def test_sum2():
    assert compute_sum(3, 4) == 7

#
# @pytest.mark.parametrize("test_input,expected", [("3+5", 8), ("2+4", 6), ("6*9", 42)])
# def test_eval(test_input, expected):
#     assert eval(test_input) == expected
