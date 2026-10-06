from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._queue: list[str] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate the processed data."""
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Ingest data for processing."""
        pass

    def output(self) -> tuple[int, str]:
        """Output the processed data."""
        if not self._queue:
            raise ValueError("No data available for output.")

        value = self._queue.pop(0)
        rank = self._rank
        self._rank += 1
        return rank, value

    def get_stats(self) -> tuple[int, int]:
        remaining = len(self._queue)
        total = self._rank + remaining
        return total, remaining

class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(type(item) in (int, float) for item in data)

        return type(data) in (int, float)

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid data type. Expected int, float, or list of int/float.")

        if isinstance(data, list):
            for item in data:
                self._queue.append(str(item))
        else:
            self._queue.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return isinstance(data, str)

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid data type. Expected str or list of str.")

        if isinstance(data, list):
            self._queue.extend(data)
        else:
            self._queue.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(isinstance(item, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in item.items()) for item in data)
        if isinstance(data, dict):
            return all(isinstance(k, str) and isinstance(v, str) for k, v in data.items())
        return False
    
    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid data type. Expected dict[str, str] or list of dict[str, str].")

        entries = data if isinstance(data, list) else [data]

        for entry in entries:
            if "log_level" in entry and "log_message" in entry:
                text = (
                    f"{entry['log_level']}: " 
                    f"{entry['log_message']}"
                )
            else:
                text = str(entry)

            self._queue.append(text)


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            for processor in self._processors:
                if processor.validate(element):
                    try:
                        processor.ingest(element)
                    except (ValueError, TypeError) as error:
                        print(f"DataStream error - {error}")
                    break
            else:
                print(
                    "DataStream error - Cannot process "
                    f"element in stream: {element}"
                )

    def print_processors_stats(self) -> None:
        print("\n=== DataStream statistics ===")

        if not self._processors:
            print("No processor found, no data")
            return

        for processor in self._processors:
            total, remaining = processor.get_stats()
            name = processor.__class__.__name__

            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")

def main() -> None:
    print("=== Code Nexus - Data Stream ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    numeric = NumericProcessor()
    text = TextProcessor()
    logs = LogProcessor()

    print("\nRegistering Numeric Processor")
    stream.register_processor(numeric)

    batch: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    stream.register_processor(text)
    stream.register_processor(logs)

    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nConsume some elements from the data processors:")

    for _ in range(3):
        rank, value = numeric.output()
        print(f"Numeric value {rank}: {value}")

    for _ in range(2):
        rank, value = text.output()
        print(f"Text value {rank}: {value}")

    rank, value = logs.output()
    print(f"Log entry {rank}: {value}")

    stream.print_processors_stats()


if __name__ == "__main__":
    main()
