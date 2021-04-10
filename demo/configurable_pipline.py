import csv
import pathlib
from typing import List

from dagster import execute_pipeline, pipeline, solid

Cereals = List[dict]


@solid(config_schema={"csv_name": str})
def read_csv(context) -> List[dict]:
    csv_path = pathlib.Path(__file__).parent / context.solid_config["csv_name"]
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info(f"Read {len(lines)} lines")
    return lines


@solid
def sort_by_calories(context, cereals: Cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: int(cereal["calories"]))

    context.log.info(f'Most caloric cereal: {sorted_cereals[-1]["name"]}')


@pipeline
def configurable_pipeline():
    sort_by_calories(read_csv())


if __name__ == "__main__":
    run_config = {"solids": {"read_csv": {"config": {"csv_name": "cereal.csv"}}}}
    result = execute_pipeline(configurable_pipeline, run_config=run_config)
