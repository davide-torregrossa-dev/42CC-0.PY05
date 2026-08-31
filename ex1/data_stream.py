import abc
import typing


class DataProcessor(abc.ABC):
    @abc.abstractmethod
    def can_handle(self, item: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def process(self, item: typing.Any) -> None:
        pass

    @abc.abstractmethod
    def output(self, count: int) -> list[typing.Any]:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def total_processed(self) -> int:
        pass

    @property
    @abc.abstractmethod
    def remaining(self) -> int:
        pass


class NumericProcessor(DataProcessor):
    def __init__(self):
        self._total = 0
        self._storage = []

    @property
    def name(self) -> str:
        return "Numeric Processor"

    @property
    def total_processed(self) -> int:
        return self._total

    @property
    def remaining(self) -> int:
        return len(self._storage)

    def can_handle(self, item: typing.Any) -> bool:
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            return True
        if isinstance(item, list):
            return len(item) > 0 and all(
                isinstance(x, (int, float)) and not isinstance(x, bool)
                for x in item
            )
        return False

    def process(self, item: typing.Any) -> None:
        if isinstance(item, list):
            for sub_item in item:
                self._storage.append(sub_item)
                self._total += 1
        else:
            self._storage.append(item)
            self._total += 1

    def output(self, count: int) -> list[typing.Any]:
        extracted = self._storage[:count]
        self._storage = self._storage[count:]
        return extracted


class TextProcessor(DataProcessor):
    def __init__(self):
        self._total = 0
        self._storage = []

    @property
    def name(self) -> str:
        return "Text Processor"

    @property
    def total_processed(self) -> int:
        return self._total

    @property
    def remaining(self) -> int:
        return len(self._storage)

    def can_handle(self, item: typing.Any) -> bool:
        if isinstance(item, str):
            return True
        if isinstance(item, list):
            return len(item) > 0 and all(isinstance(x, str) for x in item)
        return False

    def process(self, item: typing.Any) -> None:
        if isinstance(item, list):
            for sub_item in item:
                self._storage.append(sub_item)
                self._total += 1
        else:
            self._storage.append(item)
            self._total += 1

    def output(self, count: int) -> list[typing.Any]:
        extracted = self._storage[:count]
        self._storage = self._storage[count:]
        return extracted


class LogProcessor(DataProcessor):
    def __init__(self):
        self._total = 0
        self._storage = []

    @property
    def name(self) -> str:
        return "Log Processor"

    @property
    def total_processed(self) -> int:
        return self._total

    @property
    def remaining(self) -> int:
        return len(self._storage)

    def can_handle(self, item: typing.Any) -> bool:
        if isinstance(item, dict):
            return "log_level" in item and "log_message" in item
        if isinstance(item, list):
            return len(item) > 0 and all(
                isinstance(x, dict)
                and "log_level" in x
                and "log_message" in x
                for x in item
            )
        return False

    def process(self, item: typing.Any) -> None:
        if isinstance(item, list):
            for sub_item in item:
                self._storage.append(sub_item)
                self._total += 1
        else:
            self._storage.append(item)
            self._total += 1

    def output(self, count: int) -> list[typing.Any]:
        extracted = self._storage[:count]
        self._storage = self._storage[count:]
        return extracted


class DataStream:
    def __init__(self):
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            handled = False
            for proc in self.processors:
                if proc.can_handle(element):
                    proc.process(element)
                    handled = True
                    break
            if not handled:
                print(
                    f"DataStream error - Can't process element in stream: {element}"
                )

    def print_processors_stats(self) -> None:
        if not self.processors:
            print("== DataStream statistics ==")
            print("No processor found, no data")
            return

        print("== DataStream statistics ==")
        for proc in self.processors:
            print(
                f"{proc.name}: total {proc.total_processed} items processed, remaining {proc.remaining} on processor"
            )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")
    stream = DataStream()

    stream.print_processors_stats()

    print("Registering Numeric Processor")
    numeric_proc = NumericProcessor()
    stream.register_processor(numeric_proc)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)

    stream.print_processors_stats()

    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    print("Send the same batch again")
    stream.process_stream(batch)

    stream.print_processors_stats()

    print(
        "Consume some elements from the data processors: Numeric 3, Text 2, Log 1"
    )
    numeric_proc.output(3)
    text_proc.output(2)
    log_proc.output(1)

    stream.print_processors_stats()
