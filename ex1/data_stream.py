import abc
import typing


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._queue: list[tuple[int, str]] = []
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

    def output(self) -> tuple[int, str]:
        if not self._queue:
            raise IndexError("No data to output")
        return self._queue.pop(0)

    def total_processed(self) -> int:
        return self._next_rank

    def remaining(self) -> int:
        return len(self._queue)


NumericInput = int | float | list[int | float]
TextInput = str | list[str]
LogEntry = dict[str, str]
LogInput = LogEntry | list[LogEntry]


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
        return ", ".join(f"{key}={entry[key]}" for key in sorted(entry.keys()))

    def ingest(self, data: LogInput) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, list):
            for entry in data:
                self._store(self._format_entry(entry))
            return

        self._store(self._format_entry(data))


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            handled = False
            for proc in self._processors:
                try:
                    can_handle = proc.validate(element)
                except Exception:
                    can_handle = False

                if can_handle:
                    proc.ingest(element)
                    handled = True
                    break

            if not handled:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            print(
                f"{name}: total {proc.total_processed()} items processed, "
                f"remaining {proc.remaining()} on processor"
            )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")
    ds = DataStream()
    ds.print_processors_stats()

    print("Registering Numeric Processor")
    numeric = NumericProcessor()
    ds.register_processor(numeric)

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
    ds.process_stream(batch)
    ds.print_processors_stats()

    print("Registering other data processors")
    text = TextProcessor()
    log = LogProcessor()
    ds.register_processor(text)
    ds.register_processor(log)

    print("Send the same batch again")
    ds.process_stream(batch)
    ds.print_processors_stats()

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        numeric.output()
    for _ in range(2):
        text.output()
    for _ in range(1):
        log.output()

    ds.print_processors_stats()
