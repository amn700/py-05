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


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


def _csv_escape(value: str) -> str:
    if any(ch in value for ch in (",", '"', "\n", "\r")):
        return '"' + value.replace('"', '""') + '"'
    return value


def _json_escape(value: str) -> str:
    escaped_parts: list[str] = []
    for ch in value:
        if ch == "\\":
            escaped_parts.append("\\\\")
        elif ch == '"':
            escaped_parts.append('\\"')
        elif ch == "\b":
            escaped_parts.append("\\b")
        elif ch == "\f":
            escaped_parts.append("\\f")
        elif ch == "\n":
            escaped_parts.append("\\n")
        elif ch == "\r":
            escaped_parts.append("\\r")
        elif ch == "\t":
            escaped_parts.append("\\t")
        elif ord(ch) < 0x20:
            escaped_parts.append(f"\\u{ord(ch):04x}")
        else:
            escaped_parts.append(ch)
    return "".join(escaped_parts)


class CsvExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        line = ",".join(_csv_escape(value) for _, value in data)
        print("CSV Output:")
        print(line)


class JsonExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        pairs = [
            f'"item_{rank}": "{_json_escape(value)}"' for rank, value in data
        ]
        payload = "{" + ", ".join(pairs) + "}"
        print("JSON Output:")
        print(payload)


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            extracted: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    extracted.append(proc.output())
                except IndexError:
                    break
            plugin.process_output(extracted)

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
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...")
    ds = DataStream()
    ds.print_processors_stats()

    print("Registering Processors")
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    ds.register_processor(numeric)
    ds.register_processor(text)
    ds.register_processor(log)

    batch1 = [
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

    print(f"Send first batch of data on stream: {batch1}")
    ds.process_stream(batch1)
    ds.print_processors_stats()

    print("Send 3 processed data from each processor to a CSV plugin:")
    ds.output_pipeline(3, CsvExportPlugin())
    ds.print_processors_stats()

    batch2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]

    print(f"Send another batch of data: {batch2}")
    ds.process_stream(batch2)
    ds.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    ds.output_pipeline(5, JsonExportPlugin())
    ds.print_processors_stats()
