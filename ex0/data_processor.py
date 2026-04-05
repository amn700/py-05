import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._queue: typing.List[typing.Tuple[int, str]] = []
        self._next_rank = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        raise NotImplementedError

    def _store(self, payload: str) -> None:
        self._queue.append((self._next_rank, payload))
        self._next_rank += 1

    def output(self) -> typing.Tuple[int, str]:
        if not self._queue:
            raise IndexError("No data to output")
        return self._queue.pop(0)


NumericScalar = typing.Union[int, float]
NumericInput = typing.Union[NumericScalar, typing.List[NumericScalar]]

TextInput = typing.Union[str, typing.List[str]]

LogEntry = typing.Dict[str, str]
LogInput = typing.Union[LogEntry, typing.List[LogEntry]]


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(
                isinstance(item, (int, float)) and not isinstance(item, bool)
                for item in data
            )
        return False

    def ingest(self, data: NumericInput) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self._store(str(item))
            return

        self._store(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: TextInput) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self._store(item)
            return

        self._store(data)


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in data.items()
            )
        if isinstance(data, list):
            return all(
                isinstance(item, dict)
                and all(
                    isinstance(k, str) and isinstance(v, str)
                    for k, v in item.items()
                )
                for item in data
            )
        return False

    def _format_entry(self, entry: LogEntry) -> str:
        if "log_level" in entry and "log_message" in entry:
            level = entry["log_level"].strip()
            message = entry["log_message"].strip()
            return f"{level}: {message}"
        return ", ".join(
            f"{key}={entry[key]}" for key in sorted(entry.keys())
        )

    def ingest(self, data: LogInput) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, list):
            for entry in data:
                self._store(self._format_entry(entry))
            return

        self._store(self._format_entry(data))


def _extract_n(
    processor: DataProcessor, n: int
) -> typing.List[typing.Tuple[int, str]]:
    extracted: typing.List[typing.Tuple[int, str]] = []
    for _ in range(n):
        extracted.append(processor.output())
    return extracted


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    for numeric_candidate in (42, "Hello"):
        print(
            f"Trying to validate input '{numeric_candidate}': "
            f"{numeric.validate(numeric_candidate)}"
        )

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest("foo")
    except Exception as exc:
        print(f"Got exception: {exc}")

    numeric_data: typing.List[NumericScalar] = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")
    numeric.ingest(numeric_data)
    numeric_extract = 3
    print(f"Extracting {numeric_extract} values...")
    for rank, value in _extract_n(numeric, numeric_extract):
        print(f"Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    text = TextProcessor()
    for text_candidate in (42, "Hello"):
        print(
            f"Trying to validate input '{text_candidate}': "
            f"{text.validate(text_candidate)}"
        )

    text_data = ["Hello", "Nexus", "World"]
    print(f"Processing data: {text_data}")
    text.ingest(text_data)
    text_extract = 1
    print(f"Extracting {text_extract} value...")
    for rank, value in _extract_n(text, text_extract):
        print(f"Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    for log_candidate in (
        "Hello",
        {"log_level": "NOTICE", "log_message": "Connection to server"},
    ):
        print(
            f"Trying to validate input '{log_candidate}': "
            f"{log.validate(log_candidate)}"
        )

    log_data = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f"Processing data: {log_data}")
    log.ingest(log_data)
    log_extract = 2
    print(f"Extracting {log_extract} values...")
    for rank, value in _extract_n(log, log_extract):
        print(f"Log entry {rank}: {value}")
