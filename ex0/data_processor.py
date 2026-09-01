# Authorized: builtins, standard types, import typing, import abc

import typing as tp
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    name = "DataProcessor"
    ingested = 0
    valid_datatypes = []

    def __init__(self):
        self.storage: list[(str, int)] = []

    @abstractmethod
    def validate(self, data: tp.Any) -> bool:
        print("please create a validate method")

    @abstractmethod
    def ingest(self, data: tp.Any) -> None:
        print("please create an ingest method")

    def output(self) -> tuple[int, str]:
        try:
            temp = self.storage.pop()
            print(
                f"Extracted {temp[1]} with rank {temp[0]} from {self.name}."
            )
        except IndexError:
            print(
                f"Error, {self.name} tried to output from an empty storage."
            )

    def is_valid_datatype(self, datatype: tp.Any):
        return datatype in self.valid_datatypes


class NumericProcessor(DataProcessor):
    name = "NumericProcessor"
    valid_datatypes = [int, float]

    def validate(self, data: int | float | list[int | float]) -> bool:
        print(f"{self.name} trying to validate '{data}'...")
        is_a_list = isinstance(data, list)
        if not is_a_list:
            data = [data]
        for element in data:
            print(f"validating {element}...", end="")
            if not self.is_valid_datatype(type(element)):
                print(" error!")
                print(f"Invalid input data: {element}. Returning False")
                return False
            print(" OK.")
        toprint = data if is_a_list else data[0]
        print(f"'{toprint}' is a valid input. Returning True.")
        return True

    def ingest(self, data: int | float | list[int | float]) -> None:
        is_a_list = isinstance(data, list)
        if not is_a_list:
            data = [data]
        for element in data:
            if self.validate(element):
                rank = len(self.storage)
                temp = (rank, str(element))
                self.storage.append(temp)
                print(f"ingested {temp[1]} with rank {temp[0]}")
            else:
                print(f"Invalid input data: {element}. Cannot ingest.")


class TextProcessor(DataProcessor):
    name = "TextProcessor"
    valid_datatypes = [str]

    def validate(self, data: str | list[str]) -> bool:
        print(f"{self.name} trying to validate '{data}'...")
        is_a_list = isinstance(data, list)
        if not is_a_list:
            data = [data]
        for element in data:
            print(f"validating {element}...", end="")
            if not self.is_valid_datatype(type(element)):
                print(" error!")
                print(f"Invalid input data: {element}. Returning False")
                return False
            print(" OK.")
        toprint = data if is_a_list else data[0]
        print(f"'{toprint}' is a valid input. Returning True.")
        return True

    def ingest(self, data: str | list[str]) -> None:
        is_a_list = isinstance(data, list)
        if not is_a_list:
            data = [data]
        for element in data:
            if self.validate(element):
                rank = len(self.storage)
                temp = (rank, str(element))
                self.storage.append(temp)
                print(f"ingested {temp[1]} with rank {temp[0]}")
            else:
                print(f"Invalid input data: {element}. Cannot ingest.")


class LogProcessor(DataProcessor):
    name = "LogProcessor"

    def validate(
        self, data: dict[str, str] | list[dict[str, str]]
    ) -> bool:
        print(f"{self.name} trying to validate '{data}'...")
        is_a_list = isinstance(data, list)
        if not is_a_list:
            data = [data]
        for element in data:
            print(f"validating {element}...", end="")
            if not self.is_log(element):
                print(" error!")
                print(f"Invalid input data: {element}. Returning False")
                return False
            print(" OK.")
        toprint = data if is_a_list else data[0]
        print(f"'{toprint}' is a valid input. Returning True.")
        return True

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        is_a_list = isinstance(data, list)
        if not is_a_list:
            data = [data]
        for element in data:
            if self.validate(element):
                rank = len(self.storage)
                temp = (rank, str(element))
                self.storage.append(temp)
                print(f"ingested {temp[1]} with rank {temp[0]}")
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
    tproc.ingest([42.3, 32])
    tproc.ingest("Ciaone")
    tproc.output()
    tproc.output()

    lproc = LogProcessor()
    print("Testing LogProcessor...")
    lproc.ingest(
        [
            {"nome": "Mario"},
            {"nome": "Luca"},
            {"nome": 3},
        ]
    )
    lproc.ingest("Ciaone")
    lproc.output()
    lproc.output()
