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


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    numeric = NumericProcessor()
    text = TextProcessor()
    logs = LogProcessor()

    print("\nTesting Numeric Processor...")
    print("Trying to validate numeric (valid) input:", numeric.validate([1, 2.5, 3]))
    print("Trying to validate numeric (invalid) input:", numeric.validate([1, "wrong"]))

    try:
        numeric.ingest("foo")
    except ValueError as error:
        print(f"Got exception: {error}")

    numeric.ingest([1, 2.5, 3])
    numeric.ingest(42)

    for _ in range(4):
        rank, value = numeric.output()
        print(f"Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    print("Trying to validate text (valid) input:", text.validate(["Hello", "Nexus"]))
    print("Trying to validate text (invalid) input:", text.validate(42))

    text.ingest(["Hello", "Nexus"])
    text.ingest("World")

    for _ in range(3):
        rank, value = text.output()
        print(f"Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    print("Trying to validate log (valid) input:", logs.validate({"log_level": "NOTICE", "log_message": "Connected"}))
    print("Trying to validate log (invalid) input:", logs.validate({"message": 42}))

    logs.ingest({
        "log_level": "NOTICE",
        "log_message": "Connection to server",
    })
    logs.ingest([{
        "log_level": "ERROR",
        "log_message": "Unauthorized access",
    }])

    for _ in range(2):
        rank, value = logs.output()
        print(f"Log entry {rank}: {value}")

    try:
        logs.output()
    except ValueError as error:
        print(f"Empty queue: {error}")


if __name__ == "__main__":
    main()
