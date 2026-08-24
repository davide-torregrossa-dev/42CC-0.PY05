# Authorized: builtins, standard types, import typing, import abc

import typing as tp
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: tp.Any) -> bool:
        print("please create a validate method")

    @abstractmethod
    def ingest(self, data: tp.Any) -> None:
        print("please create an ingest method")

    def output(self) -> tuple[int, str]:
        print("please create an output method")


class NumericProcessor(DataProcessor):
    def validate(self, data: tp.Any) -> bool:
        print("please create a validate method")

    def ingest(self, data: int | float): ...


class TextProcessor(DataProcessor): ...


class LogProcessor(DataProcessor): ...
