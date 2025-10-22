"""Tkinter GUI application for 529Estimator."""
from __future__ import annotations

import colorsys
import math
import os
import queue
import threading
import tkinter as tk
from tkinter import colorchooser, filedialog, messagebox, scrolledtext, ttk
from typing import Any, Dict, List, Optional, Sequence, Tuple

from matplotlib import colors as mcolors
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
from matplotlib.lines import Line2D

from ..computation import (
    compute_one,
    execute_parallel,
    normalize_workers,
    resolve_parallel_mode,
    resolve_strategy_monthlies,
    resolve_use_numpy,
    table_compute_one,
)
from ..finance import HAS_NUMPY, reality_diff, timeline_hybrid, value_at_age_hybrid_exact
from ..parsing import parse_lumps, parse_monthly, parse_rates
from ..reporting import export_csv


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("529 Planner")
        self.geometry("1250x920")
        self._color_vars: Dict[str, tk.StringVar] = {}
        self._plot_windows: List[Tuple[tk.Toplevel, Figure, FigureCanvasTkAgg, NavigationToolbar2Tk]] = []
        self._last_table = None  # (header, rows)
        self._point_ages = [25, 40, 60]
        self._engine_var = tk.StringVar(value="auto")
        self._parallel_var = tk.StringVar(value="auto")
        self._workers_var = tk.StringVar(value="0")
        self._plot_max_var = tk.StringVar(value="90")
        self._precision_var = tk.StringVar(value="Monthly (12)")
        self.var_fast = tk.BooleanVar(value=True)
        self._compute_win: Optional[tk.Toplevel] = None

        self._work_q: "queue.Queue[Tuple[str, object]]" = queue.Queue()

        self._build_menu()
        self._build_inputs()
        self._build_reality_check()
        self._build_buttons()
        self._build_text()
        self._color_win: Optional[tk.Toplevel] = None

    # ---------- Menu / Help ----------
    def _build_menu(self) -> None:
        menubar = tk.Menu(self)
        settings = tk.Menu(menubar, tearoff=0)
        settings.add_command(label="Computation Options…", command=self._open_compute_options)
        menubar.add_cascade(label="Settings", menu=settings)
        helpmenu = tk.Menu(menubar, tearoff=0)
        helpmenu.add_command(label="How it works", command=self._open_help)
        helpmenu.add_command(label="529 Info & Resources", command=self._open_529_info)
        menubar.add_cascade(label="Help", menu=helpmenu)
        self.config(menu=menubar)

    def _open_help(self) -> None:
        win = tk.Toplevel(self)
        win.title("529 Planner — How it works")
        win.geometry("820x680")
        txt = scrolledtext.ScrolledText(win, wrap="word")
        txt.pack(fill="both", expand=True)
        txt.insert(
            "end",
            """\
STRATEGY DIFFERENCES (read this first)
--------------------------------------
• Lump Sum:
  You add one or more one-time deposits ("lumps"). Money compounds from those dates forward.
  Pre-start lumps (e.g., at 0.5 years) are rolled forward to the start age and become part of the starting balance.

• Continual Contributions (Monthly):
  You add a fixed amount every period (monthly), between a start and end age.
  Contributions before the start age are also rolled forward to the start as the initial seed.

• Hybrid:
  Combines Lump Sum and Monthly. This often outperforms either alone because deposits happen both early (lumps) and consistently (monthly).

General Notes:
  - Warm, verdant, and cool color families call out Lump, Monthly, and Hybrid strategies respectively. Line dashes stay Lump '-' | Monthly '--' | Hybrid '-.'
  - These are estimates. Real returns vary and fees/taxes are ignored for simplicity.

HOW TO USE
----------
Inputs
• Return rates (%): Comma-separated annual returns, e.g. 4,6,8.
• Start/End age (years): Simulation range.
• Lump events: "age:amount; ..." e.g. "0.5:1000; 1:5000".
• Monthly: "start-end amount" e.g. "0-18 125".

Plot
• "Plot 0-25", "Plot 25-Max", and "Plot All" open interactive charts for the selected age ranges (Max defaults to 90 and can be adjusted in Standard Settings).
• Use "Build Table" for the detailed comparison grid (and CSV export).
  - Scroll to zoom around cursor; left-drag to pan; toolbar for box-zoom/pan/save.
  - Click legend labels to hide/show series.
  - Use the AGE SLIDER at the bottom (moves in 1/12-year steps) to move markers and read current values.

Reality Check
• Enter current age (year + month) and observed value to compare to each strategy/rate.
• Leave blank to skip.

Build Table
• Produces a year-by-year grid showing Lump, Monthly, and Hybrid side-by-side for all rates.
• "Export Table CSV" writes exactly what's shown.

Computation Options
• Precision: choose Monthly (12), Quarterly (4), or Yearly (1) compounding for speed/roughness tradeoffs.
• Background compute keeps the UI responsive by running heavy work in a worker thread.
• Engine/parallel/worker controls live under Settings → Computation Options (default Auto across the board).
• NumPy vector math is used whenever available for accelerated simulations.
""",
        )
        txt.config(state="disabled")

    def _open_529_info(self) -> None:
        win = tk.Toplevel(self)
        win.title("529 Plan Essentials")
        win.geometry("780x640")
        info = scrolledtext.ScrolledText(win, wrap="word")
        info.pack(fill="both", expand=True)
        info.insert(
            "end",
            """WHAT IS A 529 PLAN?
--------------------
• A 529 plan is a tax-advantaged savings plan designed to encourage saving for future education costs.
• Earnings grow tax-deferred and qualified withdrawals for education expenses are federal tax-free. Many states also offer tax benefits.

QUALIFIED EDUCATION EXPENSES
-----------------------------
• Tuition and fees for college, university, trade, and vocational schools.
• Room and board when the beneficiary is at least a half-time student (subject to school-published cost-of-attendance limits).
• Required books, supplies, equipment, and computers with internet access.
• Up to $10,000 per year for K–12 tuition (check state rules).
• Up to $10,000 lifetime (per beneficiary) can be used to repay qualified student loans.

CONTRIBUTIONS & BENEFICIARIES
------------------------------
• Contributions are considered gifts; annual exclusion limits apply ($17,000 per donor in 2023/2024, or $85,000 via 5-year election).
• You retain control of the account and can change the beneficiary to another qualified family member without tax consequences.
• Excess or non-qualified withdrawals may incur income tax on earnings plus a 10% federal penalty.

KEY OFFICIAL RESOURCES
----------------------
• IRS Publication 970 (Tax Benefits for Education): https://www.irs.gov/publications/p970
• SEC 529 Plan Overview: https://www.investor.gov/introduction-investing/investing-basics/educational-resources/529-plans
• College Savings Plans Network (state plan links): https://www.collegesavings.org

Always confirm rules with your plan administrator and consult a financial or tax professional for personalized guidance.
""",
        )
        info.config(state="disabled")

    def _open_compute_options(self) -> None:
        if self._compute_win and tk.Toplevel.winfo_exists(self._compute_win):
            self._compute_win.lift()
            return

        win = tk.Toplevel(self)
        win.title("Computation Options")
        win.geometry("420x260")
        self._compute_win = win

        cpu_count = os.cpu_count() or 1
        numpy_status = "Yes" if HAS_NUMPY else "No"
        recommended_parallel = "Process Pool" if cpu_count > 1 else "Off"
        info_lines = [
            f"NumPy available: {numpy_status}",
            f"Recommended parallel mode: {recommended_parallel}",
            f"Detected worker capacity: {cpu_count}",
        ]
        ttk.Label(win, text="\n".join(info_lines), justify="left").grid(
            row=0, column=0, columnspan=2, padx=10, pady=(10, 8), sticky="w"
        )

        precision_values = ["Monthly (12)", "Quarterly (4)", "Yearly (1)"]

        ttk.Label(win, text="Precision:").grid(row=1, column=0, padx=10, pady=4, sticky="e")
        precision_combo = ttk.Combobox(
            win,
            values=precision_values,
            state="readonly",
            width=30,
            textvariable=self._precision_var,
        )
        precision_combo.set(self._precision_var.get())
        precision_combo.grid(row=1, column=1, padx=10, pady=4, sticky="w")

        ttk.Checkbutton(
            win,
            text="Background compute (run heavy work in a worker thread)",
            variable=self.var_fast,
        ).grid(row=2, column=0, columnspan=2, padx=10, pady=4, sticky="w")

        engine_options = [
            ("auto", "Auto (prefer NumPy when available)"),
            ("numpy", "Force NumPy"),
            ("python", "Pure Python"),
        ]
        parallel_options = [
            ("auto", "Auto"),
            ("process", "Process Pool"),
            ("thread", "Thread Pool"),
            ("none", "Off"),
        ]

        def current_label(options: Sequence[Tuple[str, str]], value: str) -> str:
            for val, label in options:
                if val == value:
                    return label
            return options[0][1]

        ttk.Label(win, text="Engine:").grid(row=3, column=0, padx=10, pady=4, sticky="e")
        engine_combo = ttk.Combobox(
            win,
            values=[label for _, label in engine_options],
            state="readonly",
            width=30,
        )
        engine_combo.set(current_label(engine_options, self._engine_var.get()))

        def on_engine_change(_event=None) -> None:
            label = engine_combo.get()
            for val, text in engine_options:
                if text == label:
                    self._engine_var.set(val)
                    break

        engine_combo.bind("<<ComboboxSelected>>", on_engine_change)
        engine_combo.grid(row=3, column=1, padx=10, pady=4, sticky="w")

        ttk.Label(win, text="Parallel mode:").grid(row=4, column=0, padx=10, pady=4, sticky="e")
        parallel_combo = ttk.Combobox(
            win,
            values=[label for _, label in parallel_options],
            state="readonly",
            width=30,
        )
        parallel_combo.set(current_label(parallel_options, self._parallel_var.get()))

        def on_parallel_change(_event=None) -> None:
            label = parallel_combo.get()
            for val, text in parallel_options:
                if text == label:
                    self._parallel_var.set(val)
                    break

        parallel_combo.bind("<<ComboboxSelected>>", on_parallel_change)
        parallel_combo.grid(row=4, column=1, padx=10, pady=4, sticky="w")

        ttk.Label(win, text="Workers (0 = auto):").grid(row=5, column=0, padx=10, pady=4, sticky="e")
        workers_spin = ttk.Spinbox(
            win,
            from_=0,
            to=max(cpu_count, 64),
            width=6,
            textvariable=self._workers_var,
        )
        workers_spin.grid(row=5, column=1, padx=10, pady=4, sticky="w")

        ttk.Label(
            win,
            text="(Settings default to Auto; adjust only if you need manual control.)",
            justify="left",
        ).grid(row=6, column=0, columnspan=2, padx=10, pady=(8, 10), sticky="w")

        def _on_close() -> None:
            self._compute_win = None
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", _on_close)

    # ---------- UI ----------
    def _build_inputs(self) -> None:
        container = ttk.Frame(self)
        container.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)

        standard = ttk.LabelFrame(container, text="Standard Settings")
        standard.pack(side=tk.TOP, fill=tk.X, pady=(0, 6))
        for col in (1, 3, 5, 7):
            standard.grid_columnconfigure(col, weight=1)

        ttk.Label(standard, text="Return rates (%) comma:").grid(row=0, column=0, sticky="w")
        self.ent_rates = ttk.Entry(standard, width=24)
        self.ent_rates.insert(0, "4,6,8")
        self.ent_rates.grid(row=0, column=1, padx=6, pady=4, sticky="w")

        ttk.Label(standard, text="Start age:").grid(row=0, column=2, sticky="w")
        self.ent_start = ttk.Entry(standard, width=6)
        self.ent_start.insert(0, "0")
        self.ent_start.grid(row=0, column=3, padx=6, pady=4, sticky="w")

        ttk.Label(standard, text="End age:").grid(row=0, column=4, sticky="w")
        self.ent_end = ttk.Entry(standard, width=6)
        self.ent_end.insert(0, "25")
        self.ent_end.grid(row=0, column=5, padx=6, pady=4, sticky="w")
        self.ent_end.bind("<FocusOut>", self._on_end_age_focus_out)
        self.ent_end.bind("<Return>", self._on_end_age_focus_out)

        ttk.Label(standard, text="Plot max age:").grid(row=0, column=6, sticky="w")
        self.ent_plot_max = ttk.Entry(standard, width=6, textvariable=self._plot_max_var)
        self.ent_plot_max.grid(row=0, column=7, padx=6, pady=4, sticky="w")
        self.ent_plot_max.bind("<FocusOut>", self._on_plot_max_focus_out)
        self.ent_plot_max.bind("<Return>", self._on_plot_max_focus_out)

        ttk.Label(standard, text="Plot max age:").grid(row=0, column=6, sticky="w")
        self.spn_plot_max = ttk.Spinbox(
            standard,
            from_=25,
            to=150,
            width=6,
            textvariable=self._plot_max_var,
        )
        self.spn_plot_max.grid(row=0, column=7, padx=6, pady=4, sticky="w")

        ttk.Button(
            standard,
            text="Style Colors…",
            command=self._open_color_popout,
        ).grid(row=0, column=8, padx=8, pady=4)

        lump_frame = ttk.LabelFrame(container, text="Lump Sum Strategy")
        lump_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 6))
        lump_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(lump_frame, text="Lump events (age:amount; …):").grid(
            row=0, column=0, sticky="w"
        )
        self.ent_lumps = ttk.Entry(lump_frame, width=36)
        self.ent_lumps.insert(0, "0.5:5000")
        self.ent_lumps.grid(row=0, column=1, padx=6, pady=(4, 2), sticky="we")
        ttk.Label(
            lump_frame,
            text="One-time deposits scheduled at specific ages.",
        ).grid(row=1, column=0, columnspan=2, sticky="w", padx=0, pady=(0, 4))

        monthly_frame = ttk.LabelFrame(container, text="Monthly Contributions")
        monthly_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 6))
        monthly_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(monthly_frame, text="Monthly (start-end amount):").grid(
            row=0, column=0, sticky="w"
        )
        self.ent_monthly = ttk.Entry(monthly_frame, width=36)
        self.ent_monthly.insert(0, "0-18 125")
        self.ent_monthly.grid(row=0, column=1, padx=6, pady=(4, 2), sticky="we")
        ttk.Label(
            monthly_frame,
            text="Recurring monthly contributions for the base schedule.",
        ).grid(row=1, column=0, columnspan=2, sticky="w", padx=0, pady=(0, 4))

        hybrid_frame = ttk.LabelFrame(container, text="Hybrid Strategy")
        hybrid_frame.pack(side=tk.TOP, fill=tk.X)
        hybrid_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(hybrid_frame, text="Hybrid extra monthly:").grid(row=0, column=0, sticky="w")
        self.ent_monthly_extra = ttk.Entry(hybrid_frame, width=36)
        self.ent_monthly_extra.insert(0, "5-18 125")
        self.ent_monthly_extra.grid(row=0, column=1, padx=6, pady=(4, 2), sticky="we")
        ttk.Label(
            hybrid_frame,
            text="Hybrid-only segments; separate with ';'.",
        ).grid(row=1, column=0, columnspan=2, sticky="w", padx=0)
        ttk.Label(
            hybrid_frame,
            text=(
                "Hybrid totals reflect the lump contributions plus the hybrid-only"
                " monthly schedule."
            ),
            wraplength=520,
        ).grid(row=2, column=0, columnspan=2, sticky="w", padx=0, pady=(2, 4))

    def _open_color_popout(self) -> None:
        if (
            hasattr(self, "_color_win")
            and self._color_win
            and tk.Toplevel.winfo_exists(self._color_win)
        ):
            self._color_win.lift()
            return
        self._color_win = tk.Toplevel(self)
        self._color_win.title("Style Colors")
        self._color_win.geometry("360x220")
        ttk.Label(
            self._color_win,
            text="Set the base color for each investment strategy.",
        ).grid(row=0, column=0, columnspan=3, padx=12, pady=(12, 6), sticky="w")

        style_defaults = {
            "lump": "#E15759",
            "monthly": "#3A9247",
            "hybrid": "#3266C0",
        }
        style_labels = [
            ("lump", "Lump Sum"),
            ("monthly", "Monthly Contributions"),
            ("hybrid", "Hybrid"),
        ]

        for idx, (style_key, label_text) in enumerate(style_labels, start=1):
            ttk.Label(self._color_win, text=label_text).grid(
                row=idx, column=0, padx=12, pady=6, sticky="w"
            )
            var = self._color_vars.get(style_key)
            if not var:
                var = tk.StringVar(value=style_defaults[style_key])
                self._color_vars[style_key] = var
            elif not var.get().strip():
                var.set(style_defaults[style_key])

            entry = ttk.Entry(self._color_win, width=14, textvariable=var)
            entry.grid(row=idx, column=1, padx=6, pady=6, sticky="w")

            def pick(style=style_key, v=var, label=label_text) -> None:
                color = colorchooser.askcolor(title=f"Pick color for {label}")[1]
                if color:
                    v.set(color)

            button = tk.Button(self._color_win, text="Pick", command=pick, width=8)
            button.grid(row=idx, column=2, padx=6, pady=6)

            def update_button(*_: object, v=var, btn=button) -> None:
                self._configure_color_button(btn, v.get())

            update_button()
            trace_id = var.trace_add("write", update_button)

            def cleanup(event: tk.Event, v=var, trace=trace_id) -> None:
                if v is not None:
                    try:
                        v.trace_remove("write", trace)
                    except tk.TclError:
                        pass

            button.bind("<Destroy>", cleanup, add="+")

        ttk.Label(
            self._color_win,
            text="Adjusting a color updates all charts after the next build.",
            wraplength=320,
        ).grid(row=len(style_labels) + 1, column=0, columnspan=3, padx=12, pady=(4, 12), sticky="w")

    @staticmethod
    def _normalize_hex_color(color_value: str) -> Optional[str]:
        if not color_value:
            return None
        try:
            rgba = mcolors.to_rgba(color_value)
        except (TypeError, ValueError):
            return None
        return mcolors.to_hex(rgba)

    @staticmethod
    def _button_text_color(bg_hex: str) -> str:
        r, g, b = mcolors.to_rgb(bg_hex)
        luminance = 0.299 * r + 0.587 * g + 0.114 * b
        return "#000000" if luminance > 0.6 else "#FFFFFF"

    def _configure_color_button(self, button: tk.Button, color_value: str) -> None:
        normalized = self._normalize_hex_color(color_value.strip()) if color_value else None
        if not normalized:
            default_bg = self._color_win.cget("bg") if self._color_win else self.cget("bg")
            button.configure(
                bg=default_bg,
                activebackground=default_bg,
                fg="#000000",
                activeforeground="#000000",
            )
            return

        text_color = self._button_text_color(normalized)
        button.configure(
            bg=normalized,
            activebackground=normalized,
            fg=text_color,
            activeforeground=text_color,
            highlightbackground=normalized,
            highlightcolor=normalized,
        )

    def _build_reality_check(self) -> None:
        frm = ttk.LabelFrame(self, text="Reality Check")
        frm.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)

        ttk.Label(frm, text="Current age (year):").grid(row=0, column=0, sticky="w")
        self.ent_rc_year = ttk.Entry(frm, width=6)
        self.ent_rc_year.grid(row=0, column=1, padx=6)

        ttk.Label(frm, text="Current age (month):").grid(row=0, column=2, sticky="w")
        self.ent_rc_month = ttk.Entry(frm, width=6)
        self.ent_rc_month.grid(row=0, column=3, padx=6)

        ttk.Label(frm, text="Observed value ($):").grid(row=0, column=4, sticky="w")
        self.ent_rc_value = ttk.Entry(frm, width=12)
        self.ent_rc_value.grid(row=0, column=5, padx=6)

        ttk.Button(frm, text="Run Reality Check", command=self._run_reality_check).grid(
            row=0, column=6, padx=8
        )

    def _build_buttons(self) -> None:
        frm = ttk.Frame(self)
        frm.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)

        ttk.Button(frm, text="Plot 0-25", command=lambda: self._plot_clicked_range(0, 25)).pack(
            side=tk.LEFT, padx=4
        )
        self.btn_plot_mid = ttk.Button(
            frm,
            text=self._format_mid_plot_label(),
            command=lambda: self._plot_clicked_range(25, self._get_plot_max_age()),
        )
        self.btn_plot_mid.pack(side=tk.LEFT, padx=4)
        self.btn_plot_all = ttk.Button(
            frm,
            text="Plot All",
            command=lambda: self._plot_clicked_range(0, self._get_plot_max_age()),
        )
        self.btn_plot_all.pack(side=tk.LEFT, padx=4)
        self._plot_max_var.trace_add("write", self._on_plot_max_var_changed)
        ttk.Button(frm, text="Per-rate charts", command=self._per_rate_clicked).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(frm, text="Build Table", command=self._table_clicked).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(frm, text="Export Table CSV", command=self._export_table_csv).pack(
            side=tk.LEFT, padx=4
        )

    def _build_text(self) -> None:
        frm = ttk.LabelFrame(self, text="Summary")
        frm.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=6)
        self.txt = scrolledtext.ScrolledText(frm, wrap="word", height=16)
        self.txt.pack(fill=tk.BOTH, expand=True)

    # ---------- parsing helpers ----------
    def _parse_rates(self) -> List[float]:
        try:
            return parse_rates(self.ent_rates.get())
        except Exception as exc:  # pragma: no cover - user input path
            messagebox.showerror("Error", str(exc))
            return []

    def _get_precision(self) -> int:
        label = self._precision_var.get()
        if "Quarterly" in label:
            return 4
        if "Yearly" in label:
            return 1
        return 12

    def _get_engine_mode(self) -> str:
        value = (self._engine_var.get() or "auto").lower()
        if value not in {"auto", "numpy", "python"}:
            return "auto"
        return value

    def _get_parallel_mode(self) -> str:
        value = (self._parallel_var.get() or "auto").lower()
        if value not in {"auto", "process", "thread", "none"}:
            return "auto"
        return value

    def _get_worker_count(self) -> Optional[int]:
        text = (self._workers_var.get() or "").strip()
        if not text:
            return None
        try:
            value = int(text)
        except Exception:
            return None
        return value if value > 0 else None

    def _get_inputs(self):
        rates = self._parse_rates()
        lumps = parse_lumps(self.ent_lumps.get())
        monthly_base = parse_monthly(self.ent_monthly.get()) if self.ent_monthly.get().strip() else []
        monthly_extra = (
            parse_monthly(self.ent_monthly_extra.get())
            if getattr(self, "ent_monthly_extra", None) and self.ent_monthly_extra.get().strip()
            else []
        )
        start_age = int(self.ent_start.get())
        end_age = int(self.ent_end.get())
        m = self._get_precision()
        return rates, lumps, monthly_base, monthly_extra, start_age, end_age, m

    def _get_plot_max_age(self) -> float:
        text = (self._plot_max_var.get() or "90").strip()
        try:
            value = float(text)
        except ValueError:
            value = 90.0
        if value < 25:
            value = 25.0
        if value > 150:
            value = 150.0
        return value

    def _coerce_entry_int(
        self,
        entry: tk.Entry,
        default: int,
        *,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
    ) -> int:
        text = (entry.get() or "").strip()
        try:
            value = int(text)
        except ValueError:
            value = default
        if minimum is not None and value < minimum:
            value = minimum
        if maximum is not None and value > maximum:
            value = maximum
        entry.delete(0, tk.END)
        entry.insert(0, str(value))
        return value

    def _ensure_plot_max_at_least(self, minimum: int) -> None:
        minimum = max(25, min(150, minimum))
        current = int(round(self._get_plot_max_age()))
        desired = max(current, minimum)
        if desired != current or (self._plot_max_var.get() or "").strip() != str(desired):
            self._plot_max_var.set(str(desired))
        else:
            # Ensure the text is normalized even when already within range.
            normalized = str(current)
            if (self._plot_max_var.get() or "").strip() != normalized:
                self._plot_max_var.set(normalized)

    def _on_end_age_focus_out(self, event: Optional[object] = None) -> None:
        start_age = self._coerce_entry_int(
            self.ent_start,
            default=0,
            minimum=0,
        )
        default_end = max(start_age, 25)
        end_age = self._coerce_entry_int(
            self.ent_end,
            default=default_end,
            minimum=start_age,
        )
        self._ensure_plot_max_at_least(end_age)

    def _on_plot_max_focus_out(self, event: Optional[object] = None) -> None:
        current = int(round(self._get_plot_max_age()))
        if (self._plot_max_var.get() or "").strip() != str(current):
            self._plot_max_var.set(str(current))
        try:
            end_age = int(self.ent_end.get())
        except ValueError:
            return
        self._ensure_plot_max_at_least(end_age)

    def _format_mid_plot_label(self) -> str:
        max_age = int(round(self._get_plot_max_age()))
        return f"Plot 25-{max_age}"

    def _on_plot_max_var_changed(self, *_: object) -> None:
        focus_widget = self.focus_get()
        current_text = (self._plot_max_var.get() or "").strip()
        editing_widgets = {self.ent_plot_max}
        spin_widget = getattr(self, "spn_plot_max", None)
        if spin_widget is not None:
            editing_widgets.add(spin_widget)

        if focus_widget in editing_widgets:
            try:
                candidate = int(current_text)
            except ValueError:
                return
            if 25 <= candidate <= 150 and hasattr(self, "btn_plot_mid"):
                self.btn_plot_mid.configure(text=f"Plot 25-{candidate}")
            return

        clean_value = int(round(self._get_plot_max_age()))
        normalized = str(clean_value)
        if current_text != normalized:
            self._plot_max_var.set(normalized)
            return
        if hasattr(self, "btn_plot_mid"):
            self.btn_plot_mid.configure(text=self._format_mid_plot_label())

    # ---------- concurrency helpers ----------
    def _run_in_thread(self, fn, *args, **kwargs) -> None:
        def target() -> None:
            try:
                result = fn(*args, **kwargs)
                self._work_q.put(("ok", result))
            except Exception as exc:  # pragma: no cover - background thread path
                self._work_q.put(("err", exc))

        threading.Thread(target=target, daemon=True).start()
        self.after(50, self._poll_work_queue)

    def _poll_work_queue(self) -> None:
        try:
            status, payload = self._work_q.get_nowait()
        except queue.Empty:  # pragma: no cover - background thread path
            self.after(50, self._poll_work_queue)
            return
        if status == "ok":
            if callable(payload):
                try:
                    payload()
                except Exception as exc:  # pragma: no cover
                    messagebox.showerror("Error", str(exc))
        else:
            messagebox.showerror("Error", str(payload))

    # ---------- summary rendering ----------
    def _compute_point_estimates(
        self, rates, lumps, monthly_base, monthly_extra, start_age, m
    ):
        ages = [age for age in self._point_ages if age >= start_age]
        if not ages:
            ages = [start_age]
        results = {"lump": [], "monthly": [], "hybrid": []}
        monthly_only, hybrid_monthlies = resolve_strategy_monthlies(
            monthly_base, monthly_extra
        )
        has_monthly = bool(monthly_only)
        for r in rates:
            lump_vals = []
            monthly_vals = []
            hybrid_vals = []
            for age in ages:
                lump_vals.append(value_at_age_hybrid_exact(lumps, None, r, age, start_age, m))
                if has_monthly:
                    monthly_vals.append(
                        value_at_age_hybrid_exact([], monthly_only, r, age, start_age, m)
                    )
                hybrid_vals.append(
                    value_at_age_hybrid_exact(lumps, hybrid_monthlies, r, age, start_age, m)
                )
            results["lump"].append((r, lump_vals))
            if has_monthly:
                results["monthly"].append((r, monthly_vals))
            results["hybrid"].append((r, hybrid_vals))
        return ages, results, has_monthly, bool(monthly_extra)

    def _render_point_estimates(self, ages, data, has_monthly, has_extra) -> None:
        def fmt_age(age):
            return f"{int(age)}" if float(age).is_integer() else f"{age:.1f}"

        self.txt.delete("1.0", "end")
        age_list = ", ".join(f"Age {fmt_age(a)}" for a in ages)
        self.txt.insert("end", f"POINT ESTIMATES ({age_list})\n")

        strat_info = [
            ("lump", "Lump Sum"),
            ("monthly", "Monthly Contributions"),
            ("hybrid", "Hybrid"),
        ]
        for key, title in strat_info:
            if key == "monthly" and not has_monthly:
                self.txt.insert(
                    "end",
                    "\n" + title + "\n  (no baseline monthly contribution schedule provided)\n",
                )
                continue
            rows = data.get(key, [])
            if not rows:
                self.txt.insert("end", f"\n{title}\n  (no data)\n")
                continue
            self.txt.insert("end", f"\n{title}\n")
            for rate, values in rows:
                rate_pct = int(round(rate * 100))
                parts = [
                    f"Age {fmt_age(a)}: ${val:,.2f}" for a, val in zip(ages, values)
                ]
                self.txt.insert(
                    "end", f"  Rate {rate_pct}% -> " + " | ".join(parts) + "\n"
                )

        if has_extra:
            self.txt.insert(
                "end",
                "\nHybrid totals reflect the lump contributions plus the hybrid-only monthly schedule.\n",
            )

    def _render_table_to_text(self, widget, header, rows, title):
        """Render a simple text table into the provided Tk text widget."""

        def _format_cell(idx, value):
            if idx == 0:
                return f"{value}"
            if isinstance(value, (int, float)):
                return f"${value:,.2f}"
            return str(value)

        widget.configure(state="normal")
        widget.delete("1.0", "end")

        formatted_rows = []
        widths = [len(str(col)) for col in header]

        for row in rows:
            formatted_row = []
            for idx, value in enumerate(row):
                text = _format_cell(idx, value)
                formatted_row.append(text)
                widths[idx] = max(widths[idx], len(text))
            formatted_rows.append(formatted_row)

        header_line = " | ".join(
            str(col).ljust(widths[idx]) for idx, col in enumerate(header)
        )
        sep_line = "-+-".join("-" * widths[idx] for idx in range(len(header)))

        widget.insert("end", f"{title}\n")
        widget.insert("end", header_line + "\n")
        widget.insert("end", sep_line + "\n")

        for row in formatted_rows:
            line = " | ".join(
                (
                    cell.ljust(widths[idx]) if idx == 0 else cell.rjust(widths[idx])
                )
                for idx, cell in enumerate(row)
            )
            widget.insert("end", line + "\n")

        widget.configure(state="disabled")

    # ---------- plotting helpers ----------
    def _get_color_map(self, rates: Sequence[float]) -> Dict[str, Dict[float, str]]:
        """Return colors grouped by investment strategy and rate."""

        style_defaults = {
            "lump": "#E15759",
            "monthly": "#3A9247",
            "hybrid": "#3266C0",
        }

        cmap: Dict[str, Dict[float, str]] = {
            "lump": {},
            "monthly": {},
            "hybrid": {},
        }

        for strat, default in style_defaults.items():
            var = self._color_vars.get(strat)
            chosen = var.get().strip() if var and var.get().strip() else default
            normalized = self._normalize_hex_color(chosen) or default
            for rate in rates:
                cmap[strat][rate] = normalized
        return cmap

    def _get_rate_styles(self, rates: Sequence[float]) -> Dict[float, str]:
        styles = ["-", "--", ":"]
        unique_rates = sorted(set(rates))
        return {rate: styles[idx % len(styles)] for idx, rate in enumerate(unique_rates)}

    def _shade_variant(self, base_color: str, index: int, total: int) -> str:
        r, g, b = mcolors.to_rgb(base_color)
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        if total <= 1:
            new_l = l
        else:
            span = 0.4
            offset = (index / (total - 1)) - 0.5
            new_l = max(0.1, min(0.9, l + offset * span))
        nr, ng, nb = colorsys.hls_to_rgb(h, new_l, s)
        return mcolors.to_hex((nr, ng, nb))

    def _build_marker_palette(
        self,
        metadata: Dict[str, Tuple[str, Optional[float], Optional[str]]],
        style_colors: Dict[str, Dict[float, str]],
    ) -> Dict[str, Dict[Optional[float], str]]:
        palette: Dict[str, Dict[Optional[float], str]] = {
            "lump": {},
            "monthly": {},
            "hybrid": {},
        }
        rates_per_strategy: Dict[str, List[float]] = {"lump": [], "monthly": [], "hybrid": []}
        for strat, rate_val, _ in metadata.values():
            if rate_val is None:
                continue
            rates_per_strategy.setdefault(strat, []).append(rate_val)
        for strat, rate_list in rates_per_strategy.items():
            if not rate_list:
                continue
            base_map = style_colors.get(strat, {})
            base_color = None
            for color in base_map.values():
                base_color = color
                break
            if not base_color:
                base_color = "#808080"
            unique_rates = sorted(set(rate_list))
            for idx, rate_val in enumerate(unique_rates):
                palette.setdefault(strat, {})[rate_val] = self._shade_variant(
                    base_color, idx, len(unique_rates)
                )
        return palette

    def _plot_series_on_axes(
        self,
        ax,
        rates,
        lumps,
        monthly_base,
        monthly_extra,
        start_age,
        end_age,
        m,
        style_colors,
        combined=True,
        use_numpy=True,
    ) -> Tuple[Dict[str, List[Tuple[float, float]]], Dict[str, Tuple[str, Optional[float], Optional[str]]]]:
        series: Dict[str, List[Tuple[float, float]]] = {}
        metadata: Dict[str, Tuple[str, Optional[float], Optional[str]]] = {}
        rate_styles = self._get_rate_styles(rates)
        monthly_only, hybrid_monthlies = resolve_strategy_monthlies(
            monthly_base, monthly_extra
        )
        for r in rates:
            rate_text = f"{int(r * 100)}%"
            lump_label = f"Lump Sum only @ {rate_text}"
            series[lump_label] = timeline_hybrid(
                lumps, None, r, start_age, end_age, m, use_numpy
            )
            metadata[lump_label] = ("lump", r, rate_text)
            if monthly_only:
                monthly_label = f"Monthly only @ {rate_text}"
                series[monthly_label] = timeline_hybrid(
                    [], monthly_only, r, start_age, end_age, m, use_numpy
                )
                metadata[monthly_label] = ("monthly", r, rate_text)
            hybrid_label = f"Hybrid @ {rate_text}"
            series[hybrid_label] = timeline_hybrid(
                lumps, hybrid_monthlies, r, start_age, end_age, m, use_numpy
            )
            metadata[hybrid_label] = ("hybrid", r, rate_text)
        if combined:
            for label, curve in series.items():
                ages = [a for a, _ in curve]
                vals = [v for _, v in curve]
                strat_key, rate_tag, _ = metadata.get(label, ("lump", None, None))
                linestyle = rate_styles.get(rate_tag, "-")
                ax.plot(
                    ages,
                    vals,
                    label=label,
                    color=style_colors.get(strat_key, {}).get(rate_tag),
                    linestyle=linestyle,
                )
        return series, metadata

    def _attach_mouse_nav(self, fig, ax, canvas):
        state = {"press": None, "last": None}

        def on_scroll(event):
            if event.inaxes != ax:
                return
            base = 1.1
            scale = 1 / base if event.button == "up" else base
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            xdata, ydata = event.xdata, event.ydata
            new_w = (xlim[1] - xlim[0]) * scale
            new_h = (ylim[1] - ylim[0]) * scale
            relx = (xdata - xlim[0]) / (xlim[1] - xlim[0])
            rely = (ydata - ylim[0]) / (ylim[1] - ylim[0])
            ax.set_xlim([xdata - relx * new_w, xdata + (1 - relx) * new_w])
            ax.set_ylim([ydata - rely * new_h, ydata + (1 - rely) * new_h])
            canvas.draw_idle()

        def on_press(event):
            if event.inaxes != ax or event.button != 1:
                return
            state["press"] = (event.xdata, event.ydata)
            state["last"] = (event.xdata, event.ydata)

        def on_release(_event):
            state["press"] = None
            state["last"] = None

        def on_motion(event):
            if state["press"] is None or event.inaxes != ax:
                return
            xprev, yprev = state["last"]
            dx = event.xdata - xprev
            dy = event.ydata - yprev
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            ax.set_xlim(xlim[0] - dx, xlim[1] - dx)
            ax.set_ylim(ylim[0] - dy, ylim[1] - dy)
            state["last"] = (event.xdata, event.ydata)
            canvas.draw_idle()

        fig.canvas.mpl_connect("scroll_event", on_scroll)
        fig.canvas.mpl_connect("button_press_event", on_press)
        fig.canvas.mpl_connect("button_release_event", on_release)
        fig.canvas.mpl_connect("motion_notify_event", on_motion)

    def _attach_legend_toggle(
        self,
        ax,
        canvas,
        metadata: Dict[str, Tuple[str, Optional[float], Optional[str]]],
    ) -> None:
        legend_kwargs = {"loc": "best", "fancybox": True, "framealpha": 0.85}
        group_titles = {"lump": "Lump Sum", "monthly": "Monthly", "hybrid": "Hybrid"}
        group_order = ["lump", "monthly", "hybrid"]
        grouped: Dict[str, List[Tuple[Any, Optional[float], Optional[str]]]] = {
            "lump": [],
            "monthly": [],
            "hybrid": [],
        }

        for line in ax.get_lines():
            label = line.get_label()
            if not label or label == "_nolegend_":
                continue
            meta = metadata.get(label)
            if not meta:
                continue
            strat_key, rate_val, rate_text = meta
            grouped.setdefault(strat_key, []).append((line, rate_val, rate_text))

        legend_handles: List[Any] = []
        legend_labels: List[str] = []
        legend_targets: List[List[Any]] = []
        for strat_key in group_order:
            entries = grouped.get(strat_key)
            if not entries:
                continue
            heading = group_titles.get(strat_key, strat_key.title())
            heading_handle = Line2D([], [], linestyle="", linewidth=0, alpha=0.0)
            legend_handles.append(heading_handle)
            legend_labels.append(heading)
            heading_targets: List[Any] = []
            legend_targets.append(heading_targets)
            entries.sort(
                key=lambda item: (
                    item[1] is None,
                    item[1] if item[1] is not None else 0.0,
                )
            )
            for line, _rate_val, rate_text in entries:
                display_label = f"  {rate_text}" if rate_text else f"  {line.get_label()}"
                legend_handles.append(line)
                legend_labels.append(display_label)
                legend_targets.append([line])
                heading_targets.append(line)

        if not legend_handles:
            return

        legend = ax.legend(legend_handles, legend_labels, **legend_kwargs)
        try:
            legend._legend_box.align = "left"  # type: ignore[attr-defined]
        except AttributeError:
            pass

        handle_source = getattr(legend, "legendHandles", None)
        if handle_source is None:
            handle_source = getattr(legend, "legend_handles", [])

        legend_texts = list(legend.get_texts())
        legend_mapping: Dict[Any, Tuple[List[Any], Optional[Any]]] = {}
        legend_artists = list(handle_source)

        for idx, txt in enumerate(legend_texts):
            targets = legend_targets[idx] if idx < len(legend_targets) else []
            handle = legend_artists[idx] if idx < len(legend_artists) else None
            legend_mapping[txt] = (targets, handle)
            txt.set_picker(bool(targets))

        for idx, handle in enumerate(legend_artists):
            targets = legend_targets[idx] if idx < len(legend_targets) else []
            legend_mapping[handle] = (targets, handle)
            try:
                handle.set_picker(bool(targets))
            except AttributeError:
                pass

        def on_pick(event):
            if not hasattr(event.artist, "get_text"):
                return
            mapping = legend_mapping.get(event.artist)
            if not mapping:
                return
            target_lines, handle = mapping
            if not target_lines:
                return
            currently_visible = any(line.get_visible() for line in target_lines)
            new_state = not currently_visible
            for line in target_lines:
                line.set_visible(new_state)
                marker = getattr(line, "_marker_line", None)
                if marker is not None:
                    xdata = marker.get_xdata()
                    try:
                        has_data = len(xdata) > 0
                    except TypeError:
                        has_data = bool(xdata)
                    marker.set_visible(new_state and has_data)
            event.artist.set_alpha(1.0 if new_state else 0.2)
            if handle is not None:
                handle.set_alpha(1.0 if new_state else 0.2)
            canvas.draw_idle()

        fig = ax.figure
        fig.canvas.mpl_connect("pick_event", on_pick)

    # ---------- slider helpers ----------
    def _interp(self, ages, vals, x):
        if x <= ages[0]:
            return vals[0]
        if x >= ages[-1]:
            return vals[-1]
        lo = int(x)
        hi = lo + 1
        try:
            i_lo = ages.index(lo)
            i_hi = ages.index(hi)
        except ValueError:
            i_lo = i_hi = None
            for idx in range(len(ages) - 1):
                if ages[idx] <= x <= ages[idx + 1]:
                    i_lo, i_hi = idx, idx + 1
                    break
            if i_lo is None:
                return vals[-1]
            lo = ages[i_lo]
            hi = ages[i_hi]
        y0, y1 = vals[i_lo], vals[i_hi]
        t = (x - lo) / (hi - lo)
        return y0 + t * (y1 - y0)

    def _format_age_for_display(self, value: float) -> str:
        years = int(math.floor(value + 1e-9))
        months = int(round((value - years) * 12))
        if months == 12:
            years += 1
            months = 0
        return f"{years}y {months}m ({value:.2f})"

    def _add_slider_and_readout(
        self,
        parent_win,
        fig,
        ax,
        canvas,
        series,
        metadata,
        style_colors,
        start_age,
        end_age,
    ):
        frm = ttk.Frame(parent_win)
        frm.pack(side="bottom", fill="x", padx=6, pady=(4, 6))

        grid = ttk.Frame(frm)
        grid.pack(fill="x")
        for col_idx in range(4):
            grid.columnconfigure(col_idx, weight=2 if col_idx == 0 else 1)

        headings = ["Age", "Lump", "Monthly", "Hybrid"]
        for idx, heading in enumerate(headings):
            ttk.Label(
                grid,
                text=heading,
                font=("TkDefaultFont", 10, "bold"),
            ).grid(row=0, column=idx, padx=6, pady=(2, 4), sticky="w")

        age_value = tk.StringVar(value=self._format_age_for_display(start_age))
        var = tk.DoubleVar(value=float(start_age))
        slider = tk.Scale(
            grid,
            from_=start_age,
            to=end_age,
            orient="horizontal",
            resolution=1 / 12,
            variable=var,
            showvalue=False,
        )
        slider.grid(row=1, column=0, padx=6, sticky="ew")
        ttk.Label(grid, textvariable=age_value).grid(
            row=2, column=0, padx=6, pady=(4, 2), sticky="w"
        )

        strategy_present: Dict[str, bool] = {"lump": False, "monthly": False, "hybrid": False}
        strategy_columns = {"lump": 1, "monthly": 2, "hybrid": 3}
        bg_color = parent_win.cget("bg") or parent_win.cget("background") or self.cget("bg")
        column_frames: Dict[str, tk.Frame] = {}
        column_rows: Dict[str, List[Tuple[tk.Frame, tk.Label, tk.Label]]] = {}
        empty_labels: Dict[str, tk.Label] = {}
        for strat in ["lump", "monthly", "hybrid"]:
            frame = tk.Frame(grid, bg=bg_color)
            frame.grid(
                row=1,
                column=strategy_columns[strat],
                rowspan=2,
                padx=6,
                sticky="nw",
            )
            column_frames[strat] = frame
            column_rows[strat] = []
            placeholder = tk.Label(
                frame,
                text="--",
                font=("TkDefaultFont", 10),
                bg=bg_color,
                anchor="w",
                justify="left",
            )
            placeholder.pack(anchor="w")
            empty_labels[strat] = placeholder

        marker_palette = self._build_marker_palette(metadata, style_colors)
        data_lines = {line.get_label(): line for line in ax.get_lines()}
        line_cache: Dict[
            str,
            Tuple[
                List[float],
                List[float],
                Any,
                str,
                Optional[str],
                Optional[float],
                str,
                Any,
            ],
        ] = {}
        for label, curve in series.items():
            meta = metadata.get(label)
            if not meta:
                continue
            strat_key, rate_val, rate_text = meta
            ages = [float(a) for a, _ in curve]
            vals = [v for _, v in curve]
            line = data_lines.get(label)
            if line is None:
                continue
            strategy_present[strat_key] = True
            marker_color = marker_palette.get(strat_key, {}).get(rate_val)
            if not marker_color:
                marker_color = style_colors.get(strat_key, {}).get(rate_val) or line.get_color()
            marker_line, = ax.plot(
                [], [], marker="o", linestyle="", color=marker_color, markersize=6
            )
            marker_line.set_visible(False)
            marker_line.set_label("_nolegend_")
            marker_line.set_zorder(line.get_zorder() + 1)
            setattr(line, "_marker_line", marker_line)
            marker_hex = mcolors.to_hex(marker_line.get_color())
            line_cache[label] = (
                ages,
                vals,
                marker_line,
                strat_key,
                rate_text,
                rate_val,
                marker_hex,
                line,
            )

        def render_column(
            strat_key: str,
            entries: List[Tuple[Optional[float], Optional[str], float, str]],
        ) -> None:
            rows = column_rows[strat_key]
            placeholder = empty_labels[strat_key]
            if not strategy_present[strat_key]:
                for row_frame, dot_label, text_label in rows:
                    row_frame.destroy()
                column_rows[strat_key] = []
                placeholder.config(text="n/a")
                if not placeholder.winfo_manager():
                    placeholder.pack(anchor="w")
                return
            if not entries:
                for row_frame, dot_label, text_label in rows:
                    row_frame.destroy()
                column_rows[strat_key] = []
                placeholder.config(text="--")
                if not placeholder.winfo_manager():
                    placeholder.pack(anchor="w")
                return
            if placeholder.winfo_manager():
                placeholder.pack_forget()
            entries.sort(
                key=lambda item: (
                    item[0] is None,
                    item[0] if item[0] is not None else 0.0,
                )
            )
            while len(rows) < len(entries):
                row_frame = tk.Frame(column_frames[strat_key], bg=bg_color)
                dot_label = tk.Label(
                    row_frame,
                    text="●",
                    font=("TkDefaultFont", 11),
                    bg=bg_color,
                )
                dot_label.pack(side="left", padx=(0, 4))
                text_label = tk.Label(
                    row_frame,
                    text="",
                    font=("TkDefaultFont", 10),
                    bg=bg_color,
                    anchor="w",
                    justify="left",
                )
                text_label.pack(side="left")
                row_frame.pack(anchor="w", pady=1)
                rows.append((row_frame, dot_label, text_label))
            while len(rows) > len(entries):
                row_frame, dot_label, text_label = rows.pop()
                row_frame.destroy()
            for idx, (rate_val, rate_tag, value, marker_hex) in enumerate(entries):
                row_frame, dot_label, text_label = rows[idx]
                dot_label.configure(fg=marker_hex)
                display_text = (
                    f"${value:,.0f}" if not rate_tag else f"{rate_tag}: ${value:,.0f}"
                )
                text_label.configure(text=display_text)
                if not row_frame.winfo_manager():
                    row_frame.pack(anchor="w", pady=1)
            column_rows[strat_key] = rows

        def update_markers(*_args) -> None:
            x = var.get()
            age_value.set(self._format_age_for_display(x))
            grouped: Dict[
                str,
                List[Tuple[Optional[float], Optional[str], float, str]],
            ] = {key: [] for key in column_frames}
            for (
                ages,
                vals,
                marker,
                strat_key,
                rate_tag,
                rate_val,
                marker_hex,
                line_ref,
            ) in line_cache.values():
                if not line_ref.get_visible():
                    marker.set_visible(False)
                    continue
                y = self._interp(ages, vals, x)
                marker.set_data([x], [y])
                marker.set_visible(True)
                grouped[strat_key].append((rate_val, rate_tag, y, marker_hex))
            for strat_key, entries in grouped.items():
                render_column(strat_key, entries)
            canvas.draw_idle()

        slider.configure(command=lambda _value: update_markers())
        update_markers()

    # ---------- button handlers ----------
    def _plot_clicked_range(self, view_start: float, view_end: float):
        fast = self.var_fast.get()
        if fast:
            self._run_in_thread(self._plot_heavy, view_start, view_end)
        else:
            ui_update = self._plot_heavy(view_start, view_end)
            if callable(ui_update):
                ui_update()

    def _plot_heavy(self, view_start: float, view_end: float):
        rates, lumps, monthly_base, monthly_extra, start_age, end_age, m = self._get_inputs()
        engine_mode = self._get_engine_mode()
        try:
            use_numpy = resolve_use_numpy(engine_mode)
        except RuntimeError as exc:
            raise RuntimeError(
                f"{exc} Please install NumPy or switch to the Pure Python engine."
            )
        parallel_mode = resolve_parallel_mode(self._get_parallel_mode(), len(rates))
        workers = self._get_worker_count()
        style_colors = self._get_color_map(rates)
        rate_styles = self._get_rate_styles(rates)

        sim_end = max(end_age, int(math.ceil(view_end)))
        tasks = [
            (r, lumps, monthly_base, monthly_extra, start_age, sim_end, m, use_numpy)
            for r in rates
        ]
        results = execute_parallel(compute_one, tasks, parallel_mode, workers)

        series: Dict[str, List[Tuple[int, float]]] = {}
        metadata: Dict[str, Tuple[str, Optional[float], Optional[str]]] = {}
        for res in results:
            r_val = res["rate"]
            pct = int(r_val * 100)
            lump_curve = res["lump"] or []
            lump_label = f"Lump Sum only @ {pct}%"
            series[lump_label] = lump_curve
            metadata[lump_label] = ("lump", r_val, f"{pct}%")
            if res["monthly"] is not None:
                monthly_curve = res["monthly"]
                monthly_label = f"Monthly only @ {pct}%"
                series[monthly_label] = monthly_curve
                metadata[monthly_label] = ("monthly", r_val, f"{pct}%")
            hybrid_curve = res["hybrid"]
            hybrid_label = f"Hybrid @ {pct}%"
            series[hybrid_label] = hybrid_curve
            metadata[hybrid_label] = ("hybrid", r_val, f"{pct}%")

        point_summary = self._compute_point_estimates(
            rates, lumps, monthly_base, monthly_extra, start_age, m
        )

        def ui_update():
            self._render_point_estimates(*point_summary)
            win = tk.Toplevel(self)
            win.title(f"Projections {view_start:.0f}-{view_end:.0f}")
            win.geometry("1100x780")

            fig = Figure(figsize=(9.8, 5.6), dpi=100)
            ax = fig.add_subplot(111)
            ax.grid(True)
            ax.set_title("529 Projections")
            ax.set_xlabel("Age")
            ax.set_ylabel("Estimated Value ($)")
            for label, curve in series.items():
                meta = metadata.get(label)
                if not meta:
                    continue
                strat_key, rate_val, _rate_text = meta
                ages = [a for a, _ in curve]
                vals = [v for _, v in curve]
                color_map = style_colors.get(strat_key, {})
                color = color_map.get(rate_val)
                if color is None and color_map:
                    color = next(iter(color_map.values()))
                line, = ax.plot(
                    ages,
                    vals,
                    label=label,
                    color=color,
                    linestyle=rate_styles.get(rate_val, "-"),
                )
            ax.set_xlim(view_start, view_end)

            canvas = FigureCanvasTkAgg(fig, master=win)
            canvas.draw()
            canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
            toolbar = NavigationToolbar2Tk(canvas, win)
            toolbar.update()
            self._attach_mouse_nav(fig, ax, canvas)
            self._attach_legend_toggle(ax, canvas, metadata)

            age_extents = [
                (min(a for a, _ in curve), max(a for a, _ in curve))
                for curve in series.values()
                if curve
            ]
            if age_extents:
                min_available = min(lo for lo, _ in age_extents)
                max_available = max(hi for _, hi in age_extents)
            else:
                min_available = view_start
                max_available = view_end
            slider_start = max(view_start, min_available)
            slider_end = min(view_end, max_available)
            if slider_end < slider_start:
                slider_end = slider_start

            self._add_slider_and_readout(
                win,
                fig,
                ax,
                canvas,
                series,
                metadata,
                style_colors,
                slider_start,
                slider_end,
            )

            self._plot_windows.append((win, fig, canvas, toolbar))

            def _on_close():
                try:
                    self._plot_windows = [
                        entry for entry in self._plot_windows if entry[0] is not win
                    ]
                finally:
                    win.destroy()

            win.protocol("WM_DELETE_WINDOW", _on_close)

        return ui_update

    def _per_rate_clicked(self):
        fast = self.var_fast.get()
        if fast:
            self._run_in_thread(self._per_rate_heavy)
        else:
            ui_update = self._per_rate_heavy()
            if callable(ui_update):
                ui_update()

    def _per_rate_heavy(self):
        rates, lumps, monthly_base, monthly_extra, start_age, end_age, m = self._get_inputs()
        engine_mode = self._get_engine_mode()
        try:
            use_numpy = resolve_use_numpy(engine_mode)
        except RuntimeError as exc:
            raise RuntimeError(
                f"{exc} Please install NumPy or switch to the Pure Python engine."
            )
        parallel_mode = resolve_parallel_mode(self._get_parallel_mode(), len(rates))
        workers = self._get_worker_count()
        style_colors = self._get_color_map(rates)
        rate_styles = self._get_rate_styles(rates)

        tasks = [
            (r, lumps, monthly_base, monthly_extra, start_age, end_age, m, use_numpy)
            for r in rates
        ]
        results = execute_parallel(compute_one, tasks, parallel_mode, workers)

        def ui_update():
            for res in results:
                rate_val = res["rate"]
                rate_pct = int(rate_val * 100)
                rate_label = f"{rate_pct}%"
                series = {f"Lump Sum only @ {rate_label}": res["lump"] or []}
                metadata = {
                    f"Lump Sum only @ {rate_label}": ("lump", rate_val, rate_label)
                }
                if res["monthly"] is not None:
                    series[f"Monthly only @ {rate_label}"] = res["monthly"]
                    metadata[f"Monthly only @ {rate_label}"] = (
                        "monthly",
                        rate_val,
                        rate_label,
                    )
                series[f"Hybrid @ {rate_label}"] = res["hybrid"]
                metadata[f"Hybrid @ {rate_label}"] = ("hybrid", rate_val, rate_label)
                win = tk.Toplevel(self)
                win.title(f"Rate {rate_label}")
                fig = Figure(figsize=(7.8, 4.8), dpi=100)
                ax = fig.add_subplot(111)
                ax.grid(True)
                ax.set_title(f"Rate {rate_label}")
                ax.set_xlabel("Age")
                ax.set_ylabel("Estimated Value ($)")
                color_lump = style_colors["lump"].get(res["rate"])
                color_monthly = style_colors["monthly"].get(res["rate"])
                color_hybrid = style_colors["hybrid"].get(res["rate"])
                lc = res["lump"]
                lump_line, = ax.plot(
                    [a for a, _ in lc],
                    [v for _, v in lc],
                    label=f"Lump Sum only @ {rate_label}",
                    color=color_lump,
                    linestyle="-",
                )
                if res["monthly"] is not None:
                    mc = res["monthly"]
                    monthly_line, = ax.plot(
                        [a for a, _ in mc],
                        [v for _, v in mc],
                        label=f"Monthly only @ {rate_label}",
                        color=color_monthly,
                        linestyle="--",
                    )
                hc = res["hybrid"]
                hybrid_line, = ax.plot(
                    [a for a, _ in hc],
                    [v for _, v in hc],
                    label=f"Hybrid @ {rate_label}",
                    color=color_hybrid,
                    linestyle="-.",
                )
                canvas = FigureCanvasTkAgg(fig, master=win)
                canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
                toolbar = NavigationToolbar2Tk(canvas, win)
                toolbar.update()
                self._attach_mouse_nav(fig, ax, canvas)
                self._attach_legend_toggle(ax, canvas, metadata)
                canvas.draw()
                self._add_slider_and_readout(
                    win,
                    fig,
                    ax,
                    canvas,
                    series,
                    metadata,
                    style_colors,
                    start_age,
                    end_age,
                )

                self._plot_windows.append((win, fig, canvas, toolbar))

        return ui_update

    def _table_clicked(self):
        fast = self.var_fast.get()
        if fast:
            self._run_in_thread(self._table_heavy)
        else:
            ui_update = self._table_heavy()
            if callable(ui_update):
                ui_update()

    def _table_heavy(self):
        rates, lumps, monthly_base, monthly_extra, start_age, end_age, m = self._get_inputs()
        engine_mode = self._get_engine_mode()
        try:
            use_numpy = resolve_use_numpy(engine_mode)
        except RuntimeError as exc:
            raise RuntimeError(
                f"{exc} Please install NumPy or switch to the Pure Python engine."
            )
        parallel_mode = resolve_parallel_mode(self._get_parallel_mode(), len(rates))
        workers = self._get_worker_count()

        table_end_age = max(end_age, int(round(self._get_plot_max_age())))
        ages = list(range(start_age, table_end_age + 1))
        tasks = [
            (
                r,
                lumps,
                monthly_base,
                start_age,
                table_end_age,
                m,
                use_numpy,
                ages,
                monthly_extra,
            )
            for r in rates
        ]
        results = execute_parallel(table_compute_one, tasks, parallel_mode, workers)

        header = ["Age"]
        for r in rates:
            pct = int(r * 100)
            header.extend([f"{pct}% Lump", f"{pct}% Monthly", f"{pct}% Hybrid"])
        rows: List[List] = []
        for age in ages:
            row = [age]
            for rate, lump_map, monthly_map, hybrid_map in results:
                row.extend([
                    lump_map.get(age, 0.0),
                    monthly_map.get(age, 0.0),
                    hybrid_map.get(age, 0.0),
                ])
            rows.append(row)

        self._last_table = (header, rows)

        self.txt.configure(state="normal")
        self.txt.delete("1.0", "end")
        self._render_table_to_text(self.txt, header, rows, "YEARLY COMPARISON TABLE")

        def ui_update():
            self._render_table_to_text(
                self.txt, header, rows, "YEARLY COMPARISON TABLE"
            )

        return ui_update

    def _export_table_csv(self):
        if not self._last_table:
            messagebox.showinfo("Export", "Build the table before exporting.")
            return
        header, rows = self._last_table
        path = filedialog.asksaveasfilename(
            title="Export CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not path:
            return
        export_csv(path, header, rows)
        messagebox.showinfo("Export", f"CSV exported to {path}")

    def _run_reality_check(self):
        try:
            rates, lumps, monthly_base, monthly_extra, start_age, end_age, m = self._get_inputs()
        except Exception as exc:
            messagebox.showerror("Error", str(exc))
            return
        monthly_only, hybrid_monthlies = resolve_strategy_monthlies(
            monthly_base, monthly_extra
        )
        state_reset = False
        try:
            year_s = self.ent_rc_year.get().strip()
            month_s = self.ent_rc_month.get().strip()
            obs_s = self.ent_rc_value.get().strip()
            if not year_s or not month_s or not obs_s:
                messagebox.showinfo(
                    "Reality Check", "Enter year, month, and observed value first."
                )
                return
            year = int(year_s)
            month = int(month_s)
            observed = float(obs_s)
            age_float = year + month / 12.0
            self.txt.configure(state="normal")
            state_reset = True
            self.txt.delete("1.0", "end")
            self.txt.insert(
                "end",
                f"REALITY CHECK - Age {age_float:.2f}, Observed ${observed:,.2f}\n",
            )
            for r in rates:
                v_lump = value_at_age_hybrid_exact(
                    lumps, None, r, age_float, start_age, m
                )
                v_month = (
                    value_at_age_hybrid_exact(
                        [], monthly_only, r, age_float, start_age, m
                    )
                    if monthly_only
                    else 0.0
                )
                v_hyb = value_at_age_hybrid_exact(
                    lumps, hybrid_monthlies, r, age_float, start_age, m
                )
                dl = reality_diff(v_lump, observed)
                dm = reality_diff(v_month, observed)
                dh = reality_diff(v_hyb, observed)
                message = (
                    f"\nRate {int(r * 100)}%\n"
                    f"  Lump Sum only: expected ${dl['expected']:,.2f} | delta ${dl['difference']:,.2f} ({dl['difference_pct']:.2f}%)\n"
                    f"  Monthly only:  expected ${dm['expected']:,.2f} | delta ${dm['difference']:,.2f} ({dm['difference_pct']:.2f}%)\n"
                    f"  Hybrid:        expected ${dh['expected']:,.2f} | delta ${dh['difference']:,.2f} ({dh['difference_pct']:.2f}%)\n"
                )
                self.txt.insert("end", message)
        except Exception as exc:
            if state_reset:
                self.txt.configure(state="disabled")
            messagebox.showerror("Error", str(exc))
        else:
            if state_reset:
                self.txt.configure(state="disabled")


def run() -> None:
    App().mainloop()


__all__ = ["run", "App"]
