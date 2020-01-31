import pytest

@pytest.mark.case_c
@pytest.mark.case_a
def test_ret1():
    pass


@pytest.mark.case_a
def test_ret2():
    pass


@pytest.mark.case_b
def test_ret3():
    pass