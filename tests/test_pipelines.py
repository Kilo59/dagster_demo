import pytest
from dagster import execute_pipeline

from demo.complex_pipeline import complex_pipeline


def test_complex_pipeline():
    res = execute_pipeline(complex_pipeline)
    assert res.success
    assert len(res.solid_result_list) == 4
    for solid_res in res.solid_result_list:
        assert solid_res.success


if __name__ == "__main__":
    pytest.run(args=["-vv"])
