# 529Estimator

An interactive and command-line planner for 529 college savings projections. The tool can model lump-sum deposits, recurring contributions, and hybrid strategies across multiple return-rate scenarios.

## Features
- Graphical interface with interactive plots, per-rate drill downs, and reality checks.
- Command-line mode for quick projections and table exports.
- Efficient compute path with NumPy vectorization when available.
- Parallel execution options (process or thread pools) for evaluating many rates.
- Background execution to keep the GUI responsive during heavy simulations.

## Launching

- **GUI (default):** `python -m 529Estimator` or the `529-estimator` console script starts the Tkinter interface.
- **CLI:** pass the `--cli` flag or use the dedicated `529-estimator-cli` entry point.

## Command-line usage
Run the planner in CLI mode with custom performance options (remember to include `--cli`):

```bash
python -m 529Estimator --cli \
  --rates 4,6,8 --lumps "0.5:1000;1:5000" --monthly "5-18 125" \
  --engine auto --parallel process --workers 4 --periods 12
```

Key performance flags:
- `--engine {auto,numpy,python}` – choose the simulation backend (auto prefers NumPy when installed).
- `--parallel {auto,process,thread,none}` – control per-rate concurrency.
- `--workers N` – cap worker count (0 lets Python decide).
- `--periods N` – compounding periods per year (e.g., 12 monthly, 4 quarterly).

## GUI performance controls
The GUI **Performance** panel mirrors the CLI options:
- **Background compute** toggles offloading heavy work to a worker thread.
- **Computation engine** selects Auto/Force NumPy/Pure Python backends.
- **Parallel mode** chooses process pools, thread pools, or sequential execution.
- **Workers** limits pool size (0 = automatic).
- **Precision** selects the number of periods per year.

## Building with PyInstaller
The script is PyInstaller-friendly (uses `multiprocessing.freeze_support()`). To produce a standalone executable:

```bash
pyinstaller --noconfirm --onefile --name 529_estimator \
  --hidden-import matplotlib.backends.backend_tkagg -m 529Estimator
```

The `-m 529Estimator` flag builds from the package's module entry point, while the hidden import ensures the TkAgg backend is bundled for the GUI. After building, launch the binary with `--gui` (default) or `--cli` as needed.
