# Authorized: builtins, standard types, import typing, import abc

import typing as tp
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    name = "DataProcessor"
    ingested = 0
    valid_datatypes: list[object]

    def __init__(self) -> None:
        self.storage: list[tuple[str, int]] = []

    @abstractmethod
    def validate(self, data: tp.Any) -> bool:
        print("please create a validate method")
        return False

    @abstractmethod
    def ingest(self, data: tp.Any) -> None:
        print("please create an ingest method")

    def output(self) -> tuple[str, int] | None:
        if not self.storage:
            print(
                f"Error, {self.name} tried to output from an empty storage."
            )
            return None
        temp = self.storage.pop()
        print(f"Extracted {temp[0]} with rank {temp[1]} from {self.name}.")
        return temp

    def is_valid_datatype(self, datatype: tp.Any) -> bool:
        return datatype in self.valid_datatypes


class NumericProcessor(DataProcessor):
    name = "NumericProcessor"
    valid_datatypes = [int, float]

    def validate(self, data: int | float | list[int | float]) -> bool:
        print(f"{self.name} trying to validate '{data}'...")
        is_a_list = isinstance(data, list)
        items: list[int | float] = (
            data if isinstance(data, list) else [data]
        )

        for element in items:
            print(f"validating {element}...", end="")
            if not self.is_valid_datatype(type(element)):
                print(" error!")
                print(f"Invalid input data: {element}. Returning False")
                return False
            print(" OK.")

        toprint = data if is_a_list else items[0]
        print(f"'{toprint}' is a valid input. Returning True.")
        return True

    def ingest(self, data: int | float | list[int | float]) -> None:
        items: list[int | float] = (
            data if isinstance(data, list) else [data]
        )
        for element in items:
            if self.validate(element):
                rank = len(self.storage)
                temp: tuple[str, int] = (str(element), rank)
                self.storage.append(temp)
                self.ingested += 1
                print(f"ingested {temp[0]} with rank {temp[1]}")
            else:
                print(f"Invalid input data: {element}. Cannot ingest.")


class TextProcessor(DataProcessor):
    name = "TextProcessor"
    valid_datatypes = [str]

    def validate(self, data: str | list[str]) -> bool:
        print(f"{self.name} trying to validate '{data}'...")
        is_a_list = isinstance(data, list)
        items: list[str] = data if isinstance(data, list) else [data]

        for element in items:
            print(f"validating {element}...", end="")
            if not self.is_valid_datatype(type(element)):
                print(" error!")
                print(f"Invalid input data: {element}. Returning False")
                return False
            print(" OK.")

        toprint = data if is_a_list else items[0]
        print(f"'{toprint}' is a valid input. Returning True.")
        return True

    def ingest(self, data: str | list[str]) -> None:
        items: list[str] = data if isinstance(data, list) else [data]
        for element in items:
            if self.validate(element):
                rank = len(self.storage)
                temp: tuple[str, int] = (str(element), rank)
                self.storage.append(temp)
                self.ingested += 1
                print(f"ingested {temp[0]} with rank {temp[1]}")
            else:
                print(f"Invalid input data: {element}. Cannot ingest.")


class LogProcessor(DataProcessor):
    name = "LogProcessor"

    def validate(
        self, data: dict[str, str] | list[dict[str, str]]
    ) -> bool:
        print(f"{self.name} trying to validate '{data}'...")
        is_a_list = isinstance(data, list)
        items: list[dict[str, str]] = (
            data if isinstance(data, list) else [data]
        )

        for element in items:
            print(f"validating {element}...", end="")
            if not self.is_log(element):
                print(" error!")
                print(f"Invalid input data: {element}. Returning False")
                return False
            print(" OK.")

        toprint = data if is_a_list else items[0]
        print(f"'{toprint}' is a valid input. Returning True.")
        return True

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        items: list[dict[str, str]] = (
            data if isinstance(data, list) else [data]
        )
        for element in items:
            if self.validate(element):
                rank = len(self.storage)
                temp: tuple[str, int] = (str(element), rank)
                self.storage.append(temp)
                self.ingested += 1
                print(f"ingested {temp[0]} with rank {temp[1]}")
            else:
                print(f"Invalid input data: {element}. Cannot ingest.")

    def is_log(self, data: tp.Any) -> bool:
        if not isinstance(data, dict):
            return False
        return all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in data.items()
        )


if __name__ == "__main__":
    nproc = NumericProcessor()
    print("Testing Numeric Processor...")
    nproc.ingest([42.3, 32])
    nproc.output()
    nproc.output()
    nproc.output()
    nproc.ingest(10)
    nproc.output()
    nproc.output()

    tproc = TextProcessor()
    print("Testing TextProcessor...")
    tproc.ingest([42.3, 32])  # type: ignore[list-item]
    tproc.ingest("Ciaone")
    tproc.output()
    tproc.output()

    lproc = LogProcessor()
    print("Testing LogProcessor...")
    lproc.ingest(
        [
            {"nome": "Mario"},
            {"nome": "Luca"},
            {"nome": 3},  # type: ignore[dict-item]
        ]
    )
    lproc.ingest("Ciaone")  # type: ignore[arg-type]
    lproc.output()
    lproc.output()
