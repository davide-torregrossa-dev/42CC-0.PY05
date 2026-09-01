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

    def show(self) -> bool:
        print(
            f"{self.name}: total of {self.ingested} items processed, remaining {len(self.storage)} on processor"
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
                self.ingested += 1
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
                self.ingested += 1
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
                self.ingested += 1
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


class DataStream:
    processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        if isinstance(proc, DataProcessor):
            self.processors.append(proc)
        else:
            print("Trying to register an unvalid DataProcessor.")

    def process_stream(self, stream: list[tp.Any]) -> None:
        stream = self.stream_unpack(stream)
        i = 0
        while i < len(stream):
            validated = self.value_dispatch(stream[i])
            if validated:
                stream.pop(i)
            i += 1 * (not validated)

        print("===process_stream resume=== ")
        if len(stream) > 0:
            print(
                f"DataStream failed to find an appropriate processor for {stream}"
            )
        else:
            print("Everything went fine!")

    def print_processors_stats(self) -> None:
        for processor in self.processors:
            processor.show()

    def stream_unpack(self, stream: list[tp.Any]) -> list[tp.Any]:
        out: list[tp.Any] = []
        for element in stream:
            if not isinstance(element, list):
                out.append(element)
            else:
                for mini_element in element:
                    out.append(mini_element)
        return out

    def value_dispatch(self, value: tp.Any) -> bool:
        element_validated = False
        for processor in self.processors:
            if processor.validate(value):
                processor.ingest(value)
                element_validated = True
        return element_validated


if __name__ == "__main__":
    datastream = DataStream()
    numproc = NumericProcessor()
    tproc = TextProcessor()
    logproc = LogProcessor()
    datastream.register_processor(numproc)
    datastream.register_processor(tproc)
    datastream.register_processor(logproc)
    datastream.process_stream(
        [1, 3, "b", 5, 2, [44, 55], "a", {"a log": "yeah, a log!"}]
    )
    datastream.print_processors_stats()
    datastream.processors[0].output()
    datastream.processors[0].output()
    datastream.processors[0].output()
    datastream.processors[2].output()
    datastream.processors[2].output()
    datastream.print_processors_stats()
