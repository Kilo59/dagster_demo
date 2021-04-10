import csv
import pathlib
from typing import List

from dagster import execute_pipeline, pipeline, solid

Cereals = List[dict]


@solid
def load_cereals(context) -> Cereals:
    csv_path = pathlib.Path(__file__).parent / "cereal.csv"
    with open(csv_path, "r") as fd:
        # Read the rows in using the standard csv library
        cereals = [row for row in csv.DictReader(fd)]

    context.log.info(f"Found {len(cereals)} cereals")
    return cereals


@solid
def sort_by_calories(context, cereals: Cereals):
    sorted_cereals = list(sorted(cereals, key=lambda cereal: cereal["calories"]))

    context.log.info(f'Most caloric cereal: {sorted_cereals[-1]["name"]}')


@pipeline
def serial_pipline():
    sort_by_calories(load_cereals())


if __name__ == "__main__":
    result = execute_pipeline(serial_pipline)
