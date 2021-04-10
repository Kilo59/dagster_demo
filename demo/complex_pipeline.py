import csv
import pathlib
from typing import List

from dagster import execute_pipeline, pipeline, solid

Cereals = List[dict]


@solid
def load_cereals(_) -> Cereals:
    csv_path = pathlib.Path(__file__).parent / "cereal.csv"
    with open(csv_path, "r") as fd:
        # Read the rows in using the standard csv library
        cereals = [row for row in csv.DictReader(fd)]
    return cereals


@solid
def sort_by_calories(_, cereals: Cereals) -> str:
    sorted_cereals = list(sorted(cereals, key=lambda cereal: cereal["calories"]))
    most_calories = sorted_cereals[-1]["name"]
    return most_calories


@solid
def sort_by_protein(_, cereals: Cereals) -> str:
    sorted_cereals = list(sorted(cereals, key=lambda cereal: cereal["protein"]))
    most_protein = sorted_cereals[-1]["name"]
    return most_protein


@solid
def display_results(context, most_calories: str, most_protein: str):
    context.log.info(f"Most caloric cereal: {most_calories}")
    context.log.info(f"Most protein-rich cereal: {most_protein}")


@pipeline
def complex_pipeline():
    cereals = load_cereals()
    display_results(
        most_calories=sort_by_calories(cereals),
        most_protein=sort_by_protein(cereals),
    )


if __name__ == "__main__":
    result = execute_pipeline(complex_pipeline)
