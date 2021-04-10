import csv
import pathlib
from typing import List

from dagster import (
    DagsterType,
    InputDefinition,
    OutputDefinition,
    execute_pipeline,
    pipeline,
    solid,
    TypeCheck,
    EventMetadataEntry,
)

Cereals = List[dict]


def less_simple_data_frame_type_check(_, value):
    if not isinstance(value, list):
        return TypeCheck(
            success=False,
            description=(
                f"LessSimpleDataFrame should be a list of dicts, got {type(value)}"
            ),
        )

    fields = [field for field in value[0].keys()]

    for i, row in enumerate(value):
        if not isinstance(row, dict):
            return TypeCheck(
                success=False,
                description=(
                    "LessSimpleDataFrame should be a list of dicts, "
                    f"got {type(row)} for row {i + 1}"
                ),
            )
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return TypeCheck(
                success=False,
                description=(
                    "Rows in LessSimpleDataFrame should have the same fields, "
                    f"got {row_fields} for row {i + 1}, expected {fields}"
                ),
            )

    return TypeCheck(
        success=True,
        description="LessSimpleDataFrame summary statistics",
        metadata_entries=[
            EventMetadataEntry.text(
                str(len(value)),
                "n_rows",
                "Number of rows seen in the data frame",
            ),
            EventMetadataEntry.text(
                str(len(value[0].keys()) if len(value) > 0 else 0),
                "n_cols",
                "Number of columns seen in the data frame",
            ),
            EventMetadataEntry.text(
                str(list(value[0].keys()) if len(value) > 0 else []),
                "column_names",
                "Keys of columns seen in the data frame",
            ),
        ],
    )


SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=less_simple_data_frame_type_check,
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",
)


@solid(output_defs=[OutputDefinition(SimpleDataFrame)])
def read_csv(context):
    csv_path = pathlib.Path(__file__).parent / "cereal.csv"
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info(f"Read {len(lines)} lines")
    return lines


@solid(output_defs=[OutputDefinition(SimpleDataFrame)])
def bad_read_csv(context):
    csv_path = pathlib.Path(__file__).parent / "cereal.csv"
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info(f"Read {len(lines)} lines")
    return ["not_a_dict"]


@solid(input_defs=[InputDefinition("cereals", SimpleDataFrame)])
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
    context.log.info(f'Most caloric cereal: {sorted_cereals[-1]["name"]}')


@pipeline
def typed_pipeline():
    sort_by_calories(read_csv())


if __name__ == "__main__":
    result = execute_pipeline(typed_pipeline)