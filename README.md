*This project was created as part of the 42 curriculum by dswietoc.*

# Python Module 05 — Code Nexus

Polymorphic data streams and export pipelines in Python.

## Description

This project explores abstract classes, method overriding, subtype polymorphism, and structural typing. Three independent exercises build a processing system that validates numeric, text, and log data, stores individual items in FIFO queues, and exports them through interchangeable plugins.

## Requirements

- Python 3.10 or later.
- No third-party runtime dependencies.
- Only `abc` and `typing` are imported by the exercises.
- Optional development tools: `flake8` and `mypy`.

## Setup

```bash
git clone https://github.com/doniu112/42_Python_Module_05.git
cd 42_Python_Module_05
```

Run commands from the repository root. The examples use `python3`; use `python` instead if that is the name of your Python 3 interpreter.

## Exercises

| Exercise | File | Focus |
| --- | --- | --- |
| ex0 | [data_processor.py](ex0/data_processor.py) | Abstract base class, validation, ingestion, FIFO output |
| ex1 | [data_stream.py](ex1/data_stream.py) | Processor registration, polymorphic routing, statistics |
| ex2 | [data_pipeline.py](ex2/data_pipeline.py) | Protocol-based export plugins, manual CSV and JSON encoding |

Each exercise contains its own class definitions and demonstration and can run independently.

## Usage

```bash
python3 ex0/data_processor.py
python3 ex1/data_stream.py
python3 ex2/data_pipeline.py
```

All scripts use predefined demonstration data. No command-line arguments, interactive input, or input files are required.

### Exercise 0 — Data Processor

`DataProcessor` is an abstract base class with:

- `validate(data: Any) -> bool`: checks whether a processor accepts the input.
- `ingest(data: Any) -> None`: an abstract interface overridden with specific input types.
- `output() -> tuple[int, str]`: removes the oldest queued item and returns its rank and text.

| Processor | Accepted input |
| --- | --- |
| `NumericProcessor` | An `int`, a `float`, or a list containing either type |
| `TextProcessor` | A string or a list of strings |
| `LogProcessor` | A dictionary with string keys and values, or a list of such dictionaries |

All input is validated before items are appended, so an invalid list does not partially change the queue. Booleans are rejected by the numeric processor. Empty lists are accepted and add no items.

Items are stored as strings. Logs containing `log_level` and `log_message` are formatted as `LEVEL: message`; other accepted dictionaries use their string representation.

Output ranks start at zero for each processor and continue across subsequent ingestion batches. Calling `output()` on an empty queue raises `ValueError`.

### Exercise 1 — Data Stream

`DataStream` registers processors and routes each stream element to the first processor whose `validate()` returns `True`. If none accepts the element, it prints an error and continues.

Statistics distinguish between all items ingested and items still waiting in the queue. Lists contribute one item per element, not one item per ingestion call.

The demonstration first registers only the numeric processor, so unsupported text and logs produce expected messages. It then registers the other processors, resends the batch, and consumes some results.

Expected final statistics:

| Processor | Total ingested | Remaining |
| --- | ---: | ---: |
| Numeric | 8 | 5 |
| Text | 3 | 1 |
| Log | 2 | 1 |

### Exercise 2 — Data Pipeline

`ExportPlugin` is a `typing.Protocol` requiring:

```python
def process_output(self, data: list[tuple[int, str]]) -> None:
    ...
```

Compatible plugins do not need to inherit from this protocol. The CSV and JSON plugins demonstrate structural typing through a shared method signature.

`output_pipeline(nb, plugin)` consumes up to `nb` items **from each registered processor**, then invokes the plugin separately for each processor.

- CSV output contains values in a single row; commas, quotes, and line breaks are escaped where necessary.
- JSON output maps keys such as `item_3` to string values and escapes quotes, backslashes, and control characters.
- Both formats are constructed manually, without importing `csv` or `json`.
- Exported text is printed to standard output, not written to a file.
- Negative limits raise `ValueError`. A zero limit consumes nothing.
- Empty batches produce an empty CSV row or an empty JSON object.

The demonstration exports up to three items per processor as CSV, ingests another batch, and exports up to five items per processor as JSON.

Expected final statistics:

| Processor | Total ingested | Remaining |
| --- | ---: | ---: |
| Numeric | 11 | 3 |
| Text | 7 | 0 |
| Log | 4 | 0 |

## Code Quality

Optional setup in a Linux, macOS, or WSL shell:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install flake8 mypy
```

On Windows PowerShell, activate the environment with `.\.venv\Scripts\Activate.ps1`.

Check all exercise files:

```bash
python3 -m flake8 ex0/data_processor.py ex1/data_stream.py ex2/data_pipeline.py
python3 -m mypy --strict --explicit-package-bases ex0/data_processor.py ex1/data_stream.py ex2/data_pipeline.py
```

Exercise 0 intentionally calls `numeric.ingest("foo")` without prior validation. The runtime exception is caught, while mypy reports an expected `[arg-type]` diagnostic. This deliberately invalid call is part of the assignment's test scenario.

To check exercises 1 and 2 separately:

```bash
python3 -m mypy --strict --explicit-package-bases ex1/data_stream.py ex2/data_pipeline.py
```

## Concepts to Explain

- Why an abstract class cannot be instantiated directly.
- The difference between validation and ingestion.
- How overriding enables polymorphic routing.
- Why `break` occurs only after selecting a compatible processor.
- How FIFO output and per-processor ranks work.
- How a protocol enables plugins without explicit inheritance.
- Why escaping is necessary when generating CSV and JSON.

