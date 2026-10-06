from abc import ABC, abstractmethod
from typing import Any, Protocol



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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        fields: list[str] = []

        for _, value in data:
            if any(char in value for char in (",", '"', "\n", "\r")):
                value = '"' + value.replace('"', '""') + '"'
            fields.append(value)

        print("CSV Output:")
        print(",".join(fields))


class JSONExportPlugin:
    def _quote(self, value: str) -> str:
        escaped = ""
        for char in value:
            if char == '"':
                escaped += '\\"'
            elif char == "\\":
                escaped += "\\\\"
            elif ord(char) < 32:
                escaped += f"\\u{ord(char):04x}"
            else:
                escaped += char

        return '"' + escaped + '"'

    def process_output(self, data: list[tuple[int, str]]) -> None:
        fields: list[str] = []

        for rank, value in data:
            key = f'"item_{rank}"'
            fields.append(f"{key}: {self._quote(value)}")

        print("JSON Output:")
        print("{" + ", ".join(fields) + "}")


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        if nb < 0:
            raise ValueError("Number of items cannot be negative")

        for processor in self._processors:
            batch: list[tuple[int, str]] = []
            _, remaining = processor.get_stats()

            for _ in range(min(nb, remaining)):
                batch.append(processor.output())

            plugin.process_output(batch)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    first_batch: list[Any] = [
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

    print(f"\nSend first batch: {first_batch}")
    stream.process_stream(first_batch)
    stream.print_processors_stats()

    print("\nSend up to 3 items from each processor to CSV:")
    stream.output_pipeline(3, CSVExportPlugin())
    stream.print_processors_stats()

    second_batch: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash",
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]

    print(f"\nSend another batch: {second_batch}")
    stream.process_stream(second_batch)
    stream.print_processors_stats()

    print("\nSend up to 5 items from each processor to JSON:")
    stream.output_pipeline(5, JSONExportPlugin())
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
