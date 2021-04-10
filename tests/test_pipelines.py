import pytest
from dagster import execute_pipeline, execute_solid

from demo.complex_pipeline import complex_pipeline
from demo.hello_cereal import hello_cereal, hello_cereal_pipeline


def test_complex_pipeline():
    res = execute_pipeline(complex_pipeline)
    assert res.success
    assert len(res.solid_result_list) == 4
    for solid_res in res.solid_result_list:
        assert solid_res.success


def test_hello_cereal_solid():
    res = execute_solid(hello_cereal)
    assert res.success
    assert len(res.output_value()) == 77


def test_hello_cereal_pipeline():
    res = execute_pipeline(hello_cereal_pipeline)
    assert res.success
    assert len(res.result_for_solid("hello_cereal").output_value()) == 77


if __name__ == "__main__":
    pytest.run(args=["-vv"])
