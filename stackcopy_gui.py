#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""A photographer-friendly customtkinter front-end for ``--lightroomimport``.

All scanning, stack detection, planning, and file operations remain in
``stackcopy.py``. This module launches the CLI, renders its machine-readable
plan/progress events, and keeps the raw output available on demand.
"""

from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import unquote

if os.environ.get("STACKCOPY_RUN_CLI") == "1":
    import stackcopy

    stackcopy.main()
    sys.exit(0)

import customtkinter as ctk  # noqa: E402
from tkinter import filedialog, messagebox  # noqa: E402

try:
    from stackcopy import path_is_within  # noqa: E402
except Exception:  # pragma: no cover - fallback for a broken old bundle

    def path_is_within(path: str, root: str) -> bool:
        import platform

        keys = [
            os.path.normcase(os.path.abspath(os.path.normpath(item)))
            for item in (path, root)
        ]
        if platform.system() in ("Windows", "Darwin") or any(
            re.match(r"^/mnt/[A-Za-z](?:/|$)", item.replace(os.sep, "/"))
            for item in keys
        ):
            keys = [key.casefold() for key in keys]
        try:
            return os.path.commonpath(keys) == keys[1]
        except ValueError:
            return False


PROGRESS_SENTINEL = "@@SCPROGRESS"
LOW_SPACE_SENTINEL = "@@SCLOWSPACE"
TERMINATE_TIMEOUT_SECONDS = 3.0
APP_NAME = "Stackcopy"
SETTINGS_FILENAME = "gui-state.json"
MOVE_MODE = "Move off the card"
COPY_MODE = "Copy, leave card untouched"


# ---------------------------------------------------------------------------
# Display-free helpers
# ---------------------------------------------------------------------------


def default_dirs() -> tuple[str, str]:
    try:
        import stackcopy

        pictures = stackcopy._default_pictures_dir()
    except Exception:
        pictures = os.path.join(os.path.expanduser("~"), "Pictures")
    return (
        os.path.join(pictures, "Lightroom"),
        os.path.join(pictures, "olympus.stack.input.photos"),
    )


def cli_command(cli_args: list[str]) -> tuple[list[str], dict[str, str]]:
    env = os.environ.copy()
    if getattr(sys, "frozen", False):
        helper_name = "StackcopyCLI.exe" if os.name == "nt" else "StackcopyCLI"
        helper = os.path.join(os.path.dirname(sys.executable), helper_name)
        if os.path.exists(helper):
            env.pop("STACKCOPY_RUN_CLI", None)
            return [helper, *cli_args], env
        env["STACKCOPY_RUN_CLI"] = "1"
        return [sys.executable, *cli_args], env
    here = os.path.dirname(os.path.abspath(__file__))
    return [sys.executable, os.path.join(here, "stackcopy.py"), *cli_args], env


def parse_progress(line: str) -> tuple[dict[str, str], str | None]:
    """Parse a progress sentinel, including percent-escaped display fields."""
    body = line[len(PROGRESS_SENTINEL) :].strip()
    filename: str | None = None
    marker = " file="
    if marker in body:
        body, filename = body.split(marker, 1)
        filename = filename.strip()
    elif body.startswith("file="):
        filename = body[len("file=") :].strip()
        body = ""
    fields: dict[str, str] = {}
    for token in body.split():
        if "=" in token:
            key, value = token.split("=", 1)
            fields[key] = unquote(value) if key == "stack_output_name" else value
    return fields, filename


def parse_plan_json(text: str) -> dict[str, object] | None:
    """Validate the CLI plan payload without depending on Tk."""
    try:
        payload = json.loads(text.strip())
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    integer_fields = ("total", "bytes", "stacks", "stacked_outputs", "stack_inputs")
    for field in integer_fields:
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
    others = payload.get("others")
    if isinstance(others, dict):
        others = others.get("total")
    if isinstance(others, bool) or not isinstance(others, int) or others < 0:
        return None
    normalized = dict(payload)
    normalized["others"] = others
    if normalized["total"] != (
        normalized["stacked_outputs"] + normalized["stack_inputs"] + others
    ):
        return None
    subdirs = normalized.get("source_subdirs_scanned", [])
    if not isinstance(subdirs, list) or not all(
        isinstance(item, str) for item in subdirs
    ):
        return None
    normalized["source_subdirs_scanned"] = subdirs
    return normalized


def import_button_label(
    plan: dict[str, object] | None,
    *,
    leave_on_card: bool,
    preview: bool = False,
) -> str:
    if preview:
        return "Preview without moving anything"
    if plan is None:
        return "Start import"
    action = "Copy" if leave_on_card else "Move"
    return f"{action} {int(plan['total'])} files"


def source_inside_destination_error(
    source: str, lightroom_dir: str, stack_input_dir: str
) -> str | None:
    try:
        real_source = os.path.realpath(source)
    except OSError:
        return None
    for label, destination in (
        ("Lightroom destination", lightroom_dir),
        ("stack-input folder", stack_input_dir),
    ):
        try:
            real_destination = os.path.realpath(destination)
        except OSError:
            continue
        if path_is_within(real_source, real_destination):
            relation = (
                "is" if path_is_within(real_destination, real_source) else "is inside"
            )
            return (
                f"The source folder {relation} the {label}.\n\n"
                f"Source:\n{real_source}\n\nDestination:\n{real_destination}\n\n"
                "Importing a destination back into itself would re-sort and "
                "rename files Stackcopy has already filed. Choose the camera "
                "card or another folder outside the destinations."
            )
    return None


def destinations_are_same(first: str, second: str) -> bool:
    return path_is_within(first, second) and path_is_within(second, first)


def parse_low_space_report(line: str) -> dict[str, object] | None:
    try:
        payload = json.loads(line[len(LOW_SPACE_SENTINEL) :].strip())
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def low_space_dialog_message(report: dict[str, object] | None) -> str:
    if not report:
        return (
            "Stackcopy reports the destination is low on free space.\n\nProceed anyway?"
        )
    count = report.get("count")
    required_label = f"Required ({count} files)" if count is not None else "Required"
    estimated = str(report.get("estimated_free", "unknown"))
    if report.get("shortfall"):
        estimated += f" (short by {report['shortfall']})"
    return (
        "The destination may not have enough free space for this import.\n\n"
        f"Destination:\n{report.get('destination', 'unknown')}\n\n"
        f"Current free space: {report.get('free', 'unknown')}\n"
        f"{required_label}: {report.get('required', 'unknown')}\n"
        f"Estimated free after import: {estimated}\n"
        f"Reserve threshold: {report.get('reserve', 'unknown')}\n\nProceed anyway?"
    )


def format_bytes(value: int | float) -> str:
    amount = float(max(0, value))
    units = ("bytes", "KB", "MB", "GB", "TB")
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            if unit == "bytes":
                return f"{int(amount)} bytes"
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TB"


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    minutes, remainder = divmod(int(seconds), 60)
    return f"{minutes} min {remainder} sec"


def format_eta(seconds: float) -> str:
    rounded = max(1, int(round(seconds / 5) * 5))
    if rounded < 60:
        return f"about {rounded} seconds left"
    return f"about {max(1, round(rounded / 60))} minutes left"


def parse_cli_summary(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    patterns = {
        "problems": r"^\s*Failures:\s*(\d+)\s*$",
        "imported": r"Done\. Imported\s+(\d+)\s+files",
    }
    for key, pattern in patterns.items():
        matches = re.findall(pattern, text, re.MULTILINE)
        if matches:
            result[key] = int(matches[-1])
    return result


def success_metrics(elapsed: float, byte_count: int) -> str:
    """Build the compact success meta line used by the terminal result."""
    safe_elapsed = max(0.001, elapsed)
    details = [format_duration(safe_elapsed)]
    if byte_count:
        details.extend(
            (format_bytes(byte_count), f"{format_bytes(byte_count / safe_elapsed)}/s")
        )
    details.append("nothing failed")
    return " · ".join(details)


def _mono_family() -> str:
    if os.name == "nt":
        return "Consolas"
    if sys.platform == "darwin":
        return "Menlo"
    return "monospace"


def _settings_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return base / APP_NAME / SETTINGS_FILENAME


def load_gui_state() -> dict[str, str]:
    try:
        with _settings_path().open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        key: value
        for key, value in data.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def save_gui_state(state: dict[str, str]) -> None:
    path = _settings_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    except Exception:
        return


def volume_label(path: str) -> str | None:
    if os.name != "nt" or not path:
        return None
    try:
        import ctypes

        drive, _ = os.path.splitdrive(os.path.abspath(path))
        if not drive:
            return None
        buffer = ctypes.create_unicode_buffer(261)
        ok = ctypes.windll.kernel32.GetVolumeInformationW(
            drive + "\\", buffer, len(buffer), None, None, None, None, 0
        )
        return buffer.value if ok and buffer.value else None
    except (AttributeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Window
# ---------------------------------------------------------------------------


class StackcopyGUI(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Stackcopy — Import from card")
        self.geometry("920x860")
        self.minsize(820, 760)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._proc: subprocess.Popen | None = None
        self._queue: queue.Queue = queue.Queue()
        self._running = False
        self._plan: dict[str, object] | None = None
        self._plan_generation = 0
        self._plan_after: str | None = None
        self._save_state_scheduled = False
        self._pending: tuple[list[str], str, str, bool] | None = None
        self._assume_yes = False
        self._terminated_by_user = False
        self._degraded = False
        self._low_space_report: dict[str, object] | None = None
        self._last_dest: str | None = None
        self._log_lines: list[str] = []
        self._started_at = 0.0
        self._total = 0
        self._done = 0
        self._current_role: str | None = None
        self._bucket_done = {"stack_output": 0, "stack_input": 0, "other": 0}
        self._stack_indexes: dict[str, int] = {}
        self._active_stack_name: str | None = None

        lightroom_default, stack_default = default_dirs()
        saved = load_gui_state()
        self.src_var = ctk.StringVar(value=saved.get("source_dir", ""))
        self.dst_var = ctk.StringVar(
            value=saved.get("lightroom_dir", lightroom_default)
        )
        self.stk_var = ctk.StringVar(value=saved.get("stack_input_dir", stack_default))
        self.mode_var = ctk.StringVar(
            value=COPY_MODE if saved.get("file_mode") == "copy" else MOVE_MODE
        )
        self.verbose_var = ctk.BooleanVar(value=saved.get("verbose") == "true")
        self.detect_stacks_var = ctk.BooleanVar(
            value=saved.get("detect_stacks", "true") == "true"
        )
        self.debug_stacks_var = ctk.BooleanVar(
            value=saved.get("debug_stacks") == "true"
        )
        self._advanced_open = saved.get("advanced_open") == "true"
        self._log_open = False

        self.body = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.body.grid(row=0, column=0, sticky="nsew")
        self.body.grid_columnconfigure(0, weight=1)
        self._build_header()
        self._build_source_strip()
        self._build_mode_section()
        self._build_plan_section()
        self._build_actions()
        self._build_activity()

        self.src_var.trace_add("write", lambda *_: self._on_path_changed())
        self.dst_var.trace_add("write", lambda *_: self._on_path_changed())
        self.stk_var.trace_add("write", lambda *_: self._on_path_changed())
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(100, self._drain_queue)
        self.after(150, self._schedule_plan_scan)
        self._refresh_idle_plan()

    # -- construction ----------------------------------------------------

    def _build_header(self) -> None:
        header = ctk.CTkFrame(self.body, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew", padx=22, pady=(16, 8))
        header.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            header,
            text="Import from card",
            anchor="w",
            font=ctk.CTkFont(size=27, weight="bold"),
        ).grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(
            header,
            text=(
                "Your camera writes every frame of an in-camera stack to the card "
                "alongside the one finished JPG it made from them. Stackcopy files "
                "the finished photo where Lightroom expects it and parks the frames "
                "that fed it somewhere separate, so your library only shows pictures "
                "you might actually edit."
            ),
            anchor="w",
            justify="left",
            wraplength=850,
            text_color=("gray32", "gray74"),
        ).grid(row=1, column=0, sticky="ew", pady=(5, 0))

    def _build_source_strip(self) -> None:
        self.source_frame = ctk.CTkFrame(self.body, corner_radius=10)
        self.source_frame.grid(row=1, column=0, sticky="ew", padx=22, pady=(5, 13))
        self.source_frame.grid_columnconfigure(1, weight=1)
        self._draw_icon(self.source_frame, 0, "card")
        self.source_title_var = ctk.StringVar(value="Choose your camera card")
        self.source_scan_var = ctk.StringVar(value="No source folder selected.")
        ctk.CTkLabel(
            self.source_frame,
            textvariable=self.source_title_var,
            anchor="w",
            font=ctk.CTkFont(size=15, weight="bold"),
        ).grid(row=0, column=1, sticky="ew", pady=(12, 0))
        ctk.CTkLabel(
            self.source_frame,
            textvariable=self.source_scan_var,
            anchor="w",
            justify="left",
            wraplength=590,
            text_color=("gray38", "gray66"),
        ).grid(row=1, column=1, sticky="ew", pady=(1, 12))
        self.choose_source_btn = self._text_button(
            self.source_frame,
            "Choose a different folder…",
            lambda: self._browse(self.src_var, "Choose a camera card or folder"),
        )
        self.choose_source_btn.grid(row=0, column=2, rowspan=2, padx=14)

    def _build_mode_section(self) -> None:
        frame = ctk.CTkFrame(self.body, fg_color="transparent")
        frame.grid(row=2, column=0, sticky="ew", padx=22, pady=(0, 13))
        frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            frame,
            text="What happens to the files on the card",
            anchor="w",
            font=ctk.CTkFont(size=16, weight="bold"),
        ).grid(row=0, column=0, sticky="ew")
        self.mode_control = ctk.CTkSegmentedButton(
            frame,
            values=[MOVE_MODE, COPY_MODE],
            variable=self.mode_var,
            command=self._on_mode_changed,
            height=36,
        )
        self.mode_control.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        self.mode_help_var = ctk.StringVar()
        ctk.CTkLabel(
            frame,
            textvariable=self.mode_help_var,
            anchor="w",
            justify="left",
            text_color=("gray40", "gray66"),
        ).grid(row=2, column=0, sticky="ew", pady=(4, 0))
        self._sync_mode_help()

    def _build_plan_section(self) -> None:
        self.plan_heading_var = ctk.StringVar(value="Where these files will land")
        ctk.CTkLabel(
            self.body,
            textvariable=self.plan_heading_var,
            anchor="w",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).grid(row=3, column=0, sticky="ew", padx=22, pady=(0, 5))
        self.plan_rows = ctk.CTkFrame(self.body, corner_radius=10)
        self.plan_rows.grid(row=4, column=0, sticky="ew", padx=22)
        self.plan_rows.grid_columnconfigure(1, weight=1)
        self.plan_headline_vars: dict[str, ctk.StringVar] = {}
        self.plan_path_vars: dict[str, ctk.StringVar] = {}
        self.destination_buttons: list[ctk.CTkButton] = []
        definitions = (
            ("stack_output", "photo", self.dst_var),
            ("stack_input", "frames", self.stk_var),
            ("other", "folder", self.dst_var),
        )
        for row, (key, icon, path_var) in enumerate(definitions):
            self._draw_icon(self.plan_rows, row * 2, icon)
            headline = ctk.StringVar()
            path_text = ctk.StringVar()
            self.plan_headline_vars[key] = headline
            self.plan_path_vars[key] = path_text
            ctk.CTkLabel(
                self.plan_rows,
                textvariable=headline,
                anchor="w",
                justify="left",
                wraplength=650,
                font=ctk.CTkFont(weight="bold"),
            ).grid(row=row * 2, column=1, sticky="ew", pady=(10, 0))
            ctk.CTkLabel(
                self.plan_rows,
                textvariable=path_text,
                anchor="w",
                font=ctk.CTkFont(family=_mono_family(), size=12),
                text_color=("gray35", "gray68"),
            ).grid(row=row * 2 + 1, column=1, sticky="ew", pady=(1, 10))
            button = self._text_button(
                self.plan_rows,
                "Change",
                lambda variable=path_var: self._browse(
                    variable, "Choose a destination"
                ),
            )
            button.grid(row=row * 2, column=2, rowspan=2, padx=(10, 14))
            self.destination_buttons.append(button)

    def _build_actions(self) -> None:
        self.actions = ctk.CTkFrame(self.body, fg_color="transparent")
        self.actions.grid(row=5, column=0, sticky="ew", padx=22, pady=(15, 18))
        self.actions.grid_columnconfigure(2, weight=1)
        self.start_btn = ctk.CTkButton(
            self.actions, height=40, width=175, command=lambda: self._start(False)
        )
        self.start_btn.grid(row=0, column=0)
        self.preview_btn = ctk.CTkButton(
            self.actions,
            text="Preview without moving anything",
            height=40,
            width=230,
            fg_color="transparent",
            border_width=1,
            command=lambda: self._start(True),
        )
        self.preview_btn.grid(row=0, column=1, padx=(10, 0))
        self.advanced_btn = self._text_button(
            self.actions, "Advanced ▾", self._toggle_advanced
        )
        self.advanced_btn.grid(row=0, column=3, sticky="e")
        self.advanced_frame = ctk.CTkFrame(self.actions, fg_color="transparent")
        self.advanced_frame.grid(
            row=1, column=0, columnspan=4, sticky="ew", pady=(10, 0)
        )
        self.verbose_check = ctk.CTkCheckBox(
            self.advanced_frame,
            text="Verbose log",
            variable=self.verbose_var,
            command=self._save_current_defaults,
        )
        self.verbose_check.grid(row=0, column=0, sticky="w")
        self.detect_check = ctk.CTkCheckBox(
            self.advanced_frame,
            text="Detect stacks",
            variable=self.detect_stacks_var,
            command=self._on_detection_changed,
        )
        self.detect_check.grid(row=0, column=1, sticky="w", padx=(22, 0))
        self.debug_check = ctk.CTkCheckBox(
            self.advanced_frame,
            text="Show stack debug output",
            variable=self.debug_stacks_var,
            command=self._save_current_defaults,
        )
        self.debug_check.grid(row=0, column=2, sticky="w", padx=(22, 0))
        if not self._advanced_open:
            self.advanced_frame.grid_remove()

    def _build_activity(self) -> None:
        self.activity = ctk.CTkFrame(self.body, corner_radius=10)
        self.activity.grid(row=6, column=0, sticky="ew", padx=22, pady=(0, 20))
        self.activity.grid_columnconfigure(0, weight=1)
        self.activity.grid_remove()
        top = ctk.CTkFrame(self.activity, fg_color="transparent")
        top.grid(row=0, column=0, sticky="ew", padx=16, pady=(14, 0))
        top.grid_columnconfigure(0, weight=1)
        self.phase_var = ctk.StringVar(value="Preparing…")
        self.meta_var = ctk.StringVar(value="")
        ctk.CTkLabel(
            top,
            textvariable=self.phase_var,
            anchor="w",
            font=ctk.CTkFont(size=20, weight="bold"),
        ).grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(
            top,
            textvariable=self.meta_var,
            anchor="e",
            text_color=("gray40", "gray66"),
        ).grid(row=0, column=1, sticky="e")
        self.progress = ctk.CTkProgressBar(self.activity)
        self.progress.grid(row=1, column=0, sticky="ew", padx=16, pady=(12, 0))
        self.progress.set(0)
        self.current_file_var = ctk.StringVar(value="Waiting for the file plan…")
        self.current_file_label = ctk.CTkLabel(
            self.activity,
            textvariable=self.current_file_var,
            anchor="w",
            justify="left",
            wraplength=820,
            font=ctk.CTkFont(family=_mono_family(), size=12),
        )
        self.current_file_label.grid(row=2, column=0, sticky="ew", padx=16, pady=(8, 0))
        cards = ctk.CTkFrame(self.activity, fg_color="transparent")
        cards.grid(row=3, column=0, sticky="ew", padx=12, pady=(13, 0))
        for column in range(4):
            cards.grid_columnconfigure(column, weight=1)
        self.counter_vars: dict[str, ctk.StringVar] = {}
        for column, (key, title) in enumerate(
            (
                ("stack_output", "Stacked photos"),
                ("stack_input", "Stack frames"),
                ("other", "Singles & video"),
                ("problems", "Problems"),
            )
        ):
            card = ctk.CTkFrame(cards)
            card.grid(row=0, column=column, sticky="ew", padx=4)
            ctk.CTkLabel(
                card,
                text=title,
                font=ctk.CTkFont(size=11),
                text_color=("gray38", "gray68"),
            ).grid(row=0, column=0, padx=10, pady=(7, 0))
            value = ctk.StringVar(value="0")
            self.counter_vars[key] = value
            ctk.CTkLabel(
                card, textvariable=value, font=ctk.CTkFont(size=17, weight="bold")
            ).grid(row=1, column=0, padx=10, pady=(0, 7))
        self.running_controls = ctk.CTkFrame(self.activity, fg_color="transparent")
        self.running_controls.grid(row=4, column=0, sticky="ew", padx=16, pady=(13, 0))
        self.cancel_btn = ctk.CTkButton(
            self.running_controls,
            text="Stop after this file",
            width=155,
            fg_color="gray38",
            hover_color="gray30",
            command=self._on_cancel,
        )
        self.cancel_btn.grid(row=0, column=0)
        ctk.CTkLabel(
            self.running_controls,
            text=(
                "Stopping is safe — files move one at a time and re-running "
                "picks up the rest."
            ),
            anchor="w",
            text_color=("gray40", "gray66"),
        ).grid(row=0, column=1, padx=(12, 0), sticky="w")
        self.result_controls = ctk.CTkFrame(self.activity, fg_color="transparent")
        self.result_controls.grid(row=5, column=0, sticky="w", padx=16, pady=(13, 0))
        self.open_btn = ctk.CTkButton(
            self.result_controls, text="Open Lightroom folder", command=self._open_dest
        )
        self.open_btn.grid(row=0, column=0)
        ctk.CTkButton(
            self.result_controls,
            text="Import another card",
            fg_color="transparent",
            border_width=1,
            command=self._import_another,
        ).grid(row=0, column=1, padx=(10, 0))
        self.result_controls.grid_remove()
        self.card_empty_note = ctk.CTkFrame(self.activity, border_width=1)
        self.card_empty_note.grid(row=6, column=0, sticky="ew", padx=16, pady=(13, 0))
        ctk.CTkLabel(
            self.card_empty_note,
            text="Your card is now empty",
            anchor="w",
            font=ctk.CTkFont(weight="bold"),
        ).grid(row=0, column=0, sticky="w", padx=12, pady=(9, 0))
        ctk.CTkLabel(
            self.card_empty_note,
            text=(
                "Format the card in the camera before your next shoot rather than "
                "deleting on the computer — it keeps the folder numbering clean."
            ),
            anchor="w",
            justify="left",
            wraplength=790,
        ).grid(row=1, column=0, sticky="ew", padx=12, pady=(2, 9))
        self.card_empty_note.grid_remove()
        self.log_toggle = self._text_button(
            self.activity, "Show detailed log ▾", self._toggle_log
        )
        self.log_toggle.grid(row=7, column=0, sticky="w", padx=12, pady=(10, 7))
        self.log_frame = ctk.CTkFrame(self.activity, fg_color="transparent")
        self.log_frame.grid(row=8, column=0, sticky="ew", padx=16, pady=(0, 14))
        self.log_frame.grid_columnconfigure(0, weight=1)
        self.log = ctk.CTkTextbox(
            self.log_frame,
            height=180,
            wrap="none",
            font=ctk.CTkFont(family=_mono_family(), size=12),
        )
        self.log.grid(row=0, column=0, sticky="ew")
        self.log.configure(state="disabled")
        self.copy_log_btn = ctk.CTkButton(
            self.log_frame,
            text="Copy",
            width=66,
            fg_color="transparent",
            border_width=1,
            command=self._copy_log,
        )
        self.copy_log_btn.grid(row=1, column=0, sticky="e", pady=(6, 0))
        self.log_frame.grid_remove()

    def _draw_icon(self, parent, row: int, kind: str) -> None:
        try:
            color = self._apply_appearance_mode(parent.cget("fg_color"))
            if color == "transparent":
                color = self._apply_appearance_mode(self.cget("fg_color"))
        except Exception:
            color = "#242424"
        canvas = ctk.CTkCanvas(
            parent, width=38, height=38, highlightthickness=0, bg=color
        )
        canvas.grid(row=row, column=0, rowspan=2, padx=(13, 9), pady=7)
        ink = "#3b8ed0"
        if kind == "card":
            canvas.create_polygon(9, 5, 28, 5, 33, 10, 33, 33, 9, 33, fill=ink)
            canvas.create_rectangle(14, 9, 27, 17, fill=color, outline=color)
        elif kind == "photo":
            canvas.create_rectangle(5, 7, 33, 31, outline=ink, width=3)
            canvas.create_oval(22, 11, 28, 17, fill=ink, outline=ink)
            canvas.create_polygon(8, 28, 17, 17, 23, 24, 27, 20, 31, 28, fill=ink)
        elif kind == "frames":
            for offset in (0, 4, 8):
                canvas.create_rectangle(
                    5 + offset,
                    7 + offset,
                    25 + offset,
                    27 + offset,
                    outline=ink,
                    width=2,
                )
        else:
            canvas.create_rectangle(5, 12, 33, 31, outline=ink, width=3)
            canvas.create_polygon(5, 12, 15, 12, 18, 8, 28, 8, 31, 12, fill=ink)

    def _text_button(self, parent, text: str, command) -> ctk.CTkButton:
        return ctk.CTkButton(
            parent,
            text=text,
            command=command,
            width=1,
            fg_color="transparent",
            hover_color=("gray82", "gray28"),
            text_color=("#1f6aa5", "#5aa7df"),
        )

    # -- plan and settings -----------------------------------------------

    def _browse(self, variable: ctk.StringVar, title: str) -> None:
        start = variable.get() or os.path.expanduser("~")
        chosen = filedialog.askdirectory(initialdir=start, title=title)
        if chosen:
            variable.set(chosen)

    def _on_path_changed(self) -> None:
        self._schedule_save()
        self._plan_generation += 1
        self._plan = None
        self._refresh_idle_plan()
        self._schedule_plan_scan()

    def _on_mode_changed(self, _value: str | None = None) -> None:
        self._sync_mode_help()
        self._schedule_save()
        self._plan_generation += 1
        self._refresh_idle_plan()
        self._schedule_plan_scan()

    def _on_detection_changed(self) -> None:
        self._schedule_save()
        self._plan_generation += 1
        self._plan = None
        self._refresh_idle_plan()
        self._schedule_plan_scan()

    def _sync_mode_help(self) -> None:
        if self.mode_var.get() == COPY_MODE:
            text = "Every file stays on the card after a safe copy is written."
        else:
            text = (
                "Moving deletes each file from the card once it is safely written. "
                "The card ends up empty."
            )
        self.mode_help_var.set(text)

    def _schedule_save(self) -> None:
        if self._save_state_scheduled:
            return
        self._save_state_scheduled = True
        self.after_idle(self._save_current_defaults)

    def _save_current_defaults(self) -> None:
        self._save_state_scheduled = False
        save_gui_state(
            {
                "source_dir": self.src_var.get(),
                "lightroom_dir": self.dst_var.get(),
                "stack_input_dir": self.stk_var.get(),
                "file_mode": "copy" if self.mode_var.get() == COPY_MODE else "move",
                "verbose": "true" if self.verbose_var.get() else "false",
                "detect_stacks": "true" if self.detect_stacks_var.get() else "false",
                "debug_stacks": "true" if self.debug_stacks_var.get() else "false",
                "advanced_open": "true" if self._advanced_open else "false",
            }
        )

    def _schedule_plan_scan(self) -> None:
        if self._running:
            return
        if self._plan_after is not None:
            self.after_cancel(self._plan_after)
        self._plan_after = self.after(350, self._begin_plan_scan)

    def _begin_plan_scan(self) -> None:
        self._plan_after = None
        source = self.src_var.get().strip()
        if not source or not os.path.isdir(source):
            self._plan = None
            self._refresh_idle_plan()
            return
        generation = self._plan_generation
        self.source_scan_var.set("Scanning card…")
        args = ["--lightroomimport", source, "--plan-json"]
        if not self.detect_stacks_var.get():
            args.append("--no-stack-detection")
        if self.mode_var.get() == COPY_MODE:
            args.append("--leave-on-card")
        command, env = cli_command(args)
        env["STACKCOPY_LIGHTROOM_IMPORT_DIR"] = self.dst_var.get().strip()
        env["STACKCOPY_STACK_INPUT_DIR"] = self.stk_var.get().strip()
        threading.Thread(
            target=self._plan_worker,
            args=(generation, command, env),
            daemon=True,
        ).start()

    def _plan_worker(
        self, generation: int, command: list[str], env: dict[str, str]
    ) -> None:
        creationflags = (
            getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        )
        try:
            completed = subprocess.run(
                command,
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                creationflags=creationflags,
            )
            payload = (
                parse_plan_json(completed.stdout) if completed.returncode == 0 else None
            )
            self._queue.put(("plan", (generation, payload)))
        except Exception:
            self._queue.put(("plan", (generation, None)))

    def _refresh_idle_plan(self) -> None:
        plan = self._plan
        source = self.src_var.get().strip()
        label = volume_label(source)
        self.source_title_var.set(
            f"{source} — {label}"
            if source and label
            else (source or "Choose your camera card")
        )
        if not source:
            self.source_scan_var.set("No source folder selected.")
        elif plan is None and self.source_scan_var.get() != "Scanning card…":
            self.source_scan_var.set("Plan not available yet.")

        total = int(plan["total"]) if plan else None
        self.plan_heading_var.set(
            f"Where these {total} files will land"
            if total is not None
            else "Where these files will land"
        )
        output_count = int(plan["stacked_outputs"]) if plan else None
        input_count = int(plan["stack_inputs"]) if plan else None
        other_count = int(plan["others"]) if plan else None
        example = (
            str(plan.get("stacked_output_example", "a name like P8081885 stacked.jpg"))
            if plan
            else ""
        )
        generic_output = "Finished stacked photos — renamed with ‘stacked’ added"
        generic_output += " to the name"
        self.plan_headline_vars["stack_output"].set(
            (
                f"{output_count} finished stacked photos — renamed {example}"
                if output_count is not None
                else generic_output
            )
        )
        self.plan_headline_vars["stack_input"].set(
            (
                f"{input_count} frames that fed those stacks — kept in case you want "
                "to stack the RAWs yourself"
                if input_count is not None
                else (
                    "Frames that fed those stacks — kept in case you want to "
                    "stack the RAWs yourself"
                )
            )
        )
        self.plan_headline_vars["other"].set(
            (
                f"{other_count} single shots and videos — names untouched, dated "
                "folders "
                "as Lightroom would make them"
                if other_count is not None
                else (
                    "Single shots and videos — names untouched, dated folders as "
                    "Lightroom would make them"
                )
            )
        )
        if plan:
            lightroom_path = str(plan.get("dest_lightroom") or self.dst_var.get())
            stack_path = str(plan.get("dest_stack_input") or self.stk_var.get())
        else:
            lightroom_path = self.dst_var.get()
            stack_path = self.stk_var.get()
        self.plan_path_vars["stack_output"].set(lightroom_path)
        self.plan_path_vars["stack_input"].set(stack_path)
        self.plan_path_vars["other"].set(lightroom_path)
        self.start_btn.configure(
            text=import_button_label(
                plan, leave_on_card=self.mode_var.get() == COPY_MODE
            )
        )

    def _apply_plan(self, payload: dict[str, object] | None) -> None:
        self._plan = payload
        if payload is None:
            self.source_scan_var.set(
                "A pre-run plan is unavailable; Stackcopy will scan when the "
                "import starts."
            )
        else:
            total = int(payload["total"])
            subdirs = [str(item) for item in payload.get("source_subdirs_scanned", [])]
            noun = "photo or video" if total == 1 else "photos and videos"
            text = f"{total} {noun}, {format_bytes(int(payload['bytes']))}"
            if subdirs:
                text += " — scanned including " + ", ".join(subdirs)
            self.source_scan_var.set(text)
        self._refresh_idle_plan()

    # -- disclosures -----------------------------------------------------

    def _toggle_advanced(self) -> None:
        self._advanced_open = not self._advanced_open
        if self._advanced_open:
            self.advanced_frame.grid()
            self.advanced_btn.configure(text="Advanced ▴")
        else:
            self.advanced_frame.grid_remove()
            self.advanced_btn.configure(text="Advanced ▾")
        self._save_current_defaults()

    def _toggle_log(self) -> None:
        self._log_open = not self._log_open
        if self._log_open:
            self.log_frame.grid()
            self.log_toggle.configure(text="Hide detailed log ▴")
        else:
            self.log_frame.grid_remove()
            self.log_toggle.configure(text="Show detailed log ▾")

    # -- run -------------------------------------------------------------

    def _validate_paths(self) -> tuple[str, str, str] | None:
        source = self.src_var.get().strip()
        lightroom = self.dst_var.get().strip()
        stack_input = self.stk_var.get().strip()
        if not source or not os.path.isdir(source):
            messagebox.showerror("Stackcopy", "Please choose a valid source folder.")
            return None
        if not lightroom or not stack_input:
            messagebox.showerror("Stackcopy", "Please choose both destination folders.")
            return None
        if destinations_are_same(lightroom, stack_input):
            messagebox.showerror(
                "Stackcopy",
                "The Lightroom destination and stack-input folder must be different.",
            )
            return None
        nested_error = source_inside_destination_error(source, lightroom, stack_input)
        if nested_error:
            messagebox.showerror("Stackcopy", nested_error)
            return None
        return source, lightroom, stack_input

    def _start(self, preview: bool) -> None:
        if self._running:
            return
        paths = self._validate_paths()
        if paths is None:
            return
        source, lightroom, stack_input = paths
        args = ["--lightroomimport", source]
        if preview:
            args.append("--dry")
        if self.verbose_var.get():
            args.append("--verbose")
        if not self.detect_stacks_var.get():
            args.append("--no-stack-detection")
        if self.debug_stacks_var.get():
            args.append("--debug-stacks")
        if self.mode_var.get() == COPY_MODE:
            args.append("--leave-on-card")
        self._pending = (args, lightroom, stack_input, preview)
        self._assume_yes = False
        self._launch()

    def _launch(self) -> None:
        assert self._pending is not None
        args, lightroom, stack_input, _preview = self._pending
        command, env = cli_command(args)
        env["STACKCOPY_PROGRESS"] = "1"
        env["STACKCOPY_LOW_SPACE_REPORT"] = "1"
        env["PYTHONUNBUFFERED"] = "1"
        env["STACKCOPY_LIGHTROOM_IMPORT_DIR"] = lightroom
        env["STACKCOPY_STACK_INPUT_DIR"] = stack_input
        env.pop("STACKCOPY_ASSUME_YES", None)
        if self._assume_yes:
            env["STACKCOPY_ASSUME_YES"] = "1"

        self._last_dest = lightroom
        self._low_space_report = None
        self._degraded = False
        self._terminated_by_user = False
        self._log_lines = []
        self._started_at = time.perf_counter()
        self._total = int(self._plan["total"]) if self._plan else 0
        self._done = 0
        self._current_role = None
        self._bucket_done = {"stack_output": 0, "stack_input": 0, "other": 0}
        self._stack_indexes = {}
        self._active_stack_name = None
        self._log_open = False
        self.log_frame.grid_remove()
        self.log_toggle.configure(text="Show detailed log ▾")
        self._clear_log()
        self._set_running(True)
        self.actions.grid_remove()
        self.activity.grid()
        self.result_controls.grid_remove()
        self.card_empty_note.grid_remove()
        self.running_controls.grid()
        self.progress.grid()
        self.current_file_label.grid()
        self.progress.configure(mode="indeterminate")
        self.progress.start()
        self.phase_var.set("Preparing…")
        self.meta_var.set("")
        self.current_file_var.set("Waiting for Stackcopy to finish its safety checks…")
        self._update_counter_cards()
        threading.Thread(target=self._worker, args=(command, env), daemon=True).start()

    def _set_running(self, running: bool) -> None:
        self._running = running
        state = "disabled" if running else "normal"
        for widget in (
            self.choose_source_btn,
            self.mode_control,
            self.start_btn,
            self.preview_btn,
            self.advanced_btn,
            self.verbose_check,
            self.detect_check,
            self.debug_check,
            *self.destination_buttons,
        ):
            widget.configure(state=state)

    def _worker(self, command: list[str], env: dict[str, str]) -> None:
        creationflags = (
            getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        )
        try:
            process = subprocess.Popen(
                command,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creationflags,
            )
        except Exception as exc:
            self._queue.put(("fatal", f"Could not start stackcopy: {exc}"))
            return
        self._proc = process

        def pump(stream, kind: str) -> None:
            for line in iter(stream.readline, ""):
                self._queue.put((kind, line))
            stream.close()

        stdout_thread = threading.Thread(
            target=pump, args=(process.stdout, "out"), daemon=True
        )
        stderr_thread = threading.Thread(
            target=pump, args=(process.stderr, "err"), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()
        stdout_thread.join()
        stderr_thread.join()
        self._queue.put(("done", process.wait()))

    def _drain_queue(self) -> None:
        try:
            while True:
                kind, payload = self._queue.get_nowait()
                if kind == "plan":
                    generation, plan = payload
                    if generation == self._plan_generation and not self._running:
                        self._apply_plan(plan)
                elif kind == "out":
                    self._log_write(payload)
                elif kind == "err":
                    if payload.startswith(PROGRESS_SENTINEL):
                        self._handle_progress(payload)
                    elif payload.startswith(LOW_SPACE_SENTINEL):
                        self._low_space_report = parse_low_space_report(payload)
                        self.phase_var.set("Waiting for your decision…")
                    else:
                        self._log_write(payload)
                elif kind == "fatal":
                    self._log_write(str(payload) + "\n")
                    self._proc = None
                    self._set_running(False)
                    self._show_result(
                        "Failed to start",
                        str(payload),
                        problems=1,
                        allow_open=False,
                    )
                elif kind == "done":
                    self._handle_done(int(payload))
        except queue.Empty:
            pass
        self.after(100, self._drain_queue)

    def _handle_progress(self, line: str) -> None:
        fields, filename = parse_progress(line)
        phase = fields.get("phase")
        total = int(fields.get("total", "0") or 0)
        done = int(fields.get("done", "0") or 0)
        if phase == "scan":
            self.phase_var.set("Scanning card…")
            self.current_file_var.set(
                "Reading photo dates and looking for finished camera stacks…"
            )
            return
        if phase == "prepare":
            self._total = total
            self.phase_var.set("Preparing…")
            self.current_file_var.set(
                "Checking destinations and available disk space before the first file."
            )
            return
        if phase == "start":
            self._total = total
            self.progress.stop()
            self.progress.configure(mode="determinate")
            self.progress.set(0)
            self.phase_var.set("Preparing…" if total else "Nothing found")
            return
        if phase in {"move", "copy"}:
            if self._current_role in self._bucket_done:
                self._bucket_done[self._current_role] += 1
            self._current_role = fields.get("role", "other")
            self._done = done
            self._total = total or self._total
            output_name = fields.get("stack_output_name")
            if output_name:
                self._active_stack_name = output_name
                if output_name not in self._stack_indexes:
                    self._stack_indexes[output_name] = len(self._stack_indexes) + 1
            if self._total:
                self.progress.set(done / self._total)
            self._set_phase_heading(self._current_role)
            self.current_file_var.set(
                self._current_file_sentence(
                    filename or "file", self._current_role, output_name
                )
            )
            self._update_meta()
            self._update_counter_cards()
        elif phase in {"done", "interrupted"}:
            if self._current_role in self._bucket_done and done > self._done:
                self._bucket_done[self._current_role] += 1
            self._current_role = None
            self._done = done
            self._total = total or self._total
            self._degraded = fields.get("degraded") == "1"
            self.progress.set(done / self._total if self._total else 0)
            self._update_counter_cards()

    def _set_phase_heading(self, role: str) -> None:
        plan_stacks = int(self._plan.get("stacks", 0)) if self._plan else 0
        if role in {"stack_output", "stack_input"} and self._active_stack_name:
            current = self._stack_indexes.get(self._active_stack_name, 1)
            self.phase_var.set(
                f"Filing stack {current} of {plan_stacks}"
                if plan_stacks
                else "Filing a camera stack"
            )
        elif role == "other":
            self.phase_var.set("Filing single shots and videos")
        else:
            self.phase_var.set("Importing files…")

    def _current_file_sentence(
        self, filename: str, role: str, output_name: str | None
    ) -> str:
        assert self._pending is not None
        preview = self._pending[3]
        if preview:
            action = "Checking"
        elif self.mode_var.get() == COPY_MODE:
            action = "Copying"
        else:
            action = "Moving"
        if role == "stack_input" and output_name:
            detail = f"an input frame of the stack that made {output_name}"
        elif role == "stack_output":
            detail = f"the finished stacked photo, renamed {output_name or filename}"
        elif Path(filename).suffix.lower() in {
            ".mov",
            ".mp4",
            ".m4v",
            ".avi",
            ".mts",
            ".m2ts",
            ".mpg",
            ".mpeg",
            ".wmv",
        }:
            detail = "a video"
        else:
            detail = "a single photo"
        return f"{action} {filename} — {detail}."

    def _update_meta(self) -> None:
        if not self._total:
            self.meta_var.set("")
            return
        total_bytes = int(self._plan.get("bytes", 0)) if self._plan else 0
        processed_bytes = (
            int(total_bytes * self._done / self._total) if total_bytes else 0
        )
        pieces = [f"{self._done} of {self._total} files"]
        if total_bytes:
            pieces.append(format_bytes(processed_bytes))
        elapsed = max(0.001, time.perf_counter() - self._started_at)
        if self._done and self._done < self._total:
            pieces.append(format_eta(elapsed / self._done * (self._total - self._done)))
        self.meta_var.set(" · ".join(pieces))

    def _bucket_total(self, role: str) -> int | None:
        if not self._plan:
            return None
        key = {
            "stack_output": "stacked_outputs",
            "stack_input": "stack_inputs",
            "other": "others",
        }[role]
        return int(self._plan[key])

    def _update_counter_cards(self, problems: int = 0, terminal: bool = False) -> None:
        for role in ("stack_output", "stack_input", "other"):
            total = self._bucket_total(role)
            done = total if terminal and total is not None else self._bucket_done[role]
            self.counter_vars[role].set(
                f"{done} / {total}" if total is not None else str(done)
            )
        self.counter_vars["problems"].set(str(problems))

    def _handle_done(self, returncode: int) -> None:
        terminated = self._terminated_by_user
        self._terminated_by_user = False
        self._proc = None
        self.progress.stop()
        self._set_running(False)
        if terminated:
            self._show_result(
                "Cancelled",
                "Completed files are safe. Re-run the import to pick up "
                "everything left on the card.",
                problems=0,
                allow_open=False,
            )
            return
        if (
            returncode != 0
            and self._low_space_report is not None
            and not self._assume_yes
        ):
            if messagebox.askyesno(
                "Low disk space", low_space_dialog_message(self._low_space_report)
            ):
                self._assume_yes = True
                self._launch()
            else:
                self._show_result(
                    "Import stopped — low disk space",
                    "No new file was started after the space check. Free some "
                    "space and try again.",
                    problems=0,
                    allow_open=False,
                )
            return

        log_text = "".join(self._log_lines)
        summary = parse_cli_summary(log_text)
        problems = summary.get("problems", 0)
        assert self._pending is not None
        preview = self._pending[3]
        if returncode == 0 and self._total == 0:
            self._show_zero_plan_rows()
            self._show_result(
                "Nothing found",
                "No supported photos or videos matched this import. Check that "
                "you chose the card or its DCIM folder.",
                problems=0,
                allow_open=False,
            )
        elif returncode == 0 and preview:
            self._show_result(
                "Preview complete — nothing was moved",
                f"{self._total} files are ready to import when you are.",
                problems=0,
                allow_open=False,
            )
        elif returncode == 0:
            elapsed = max(0.001, time.perf_counter() - self._started_at)
            byte_count = int(self._plan.get("bytes", 0)) if self._plan else 0
            self._show_result(
                f"{summary.get('imported', self._total)} files imported",
                success_metrics(elapsed, byte_count),
                problems=0,
                allow_open=True,
                success=True,
            )
        elif self._degraded:
            self._show_result(
                "Import finished, but not as planned",
                "The files are safe but were not all placed as planned; review "
                "the log before erasing the card.",
                problems=max(1, problems),
                allow_open=True,
            )
        else:
            self._show_result(
                "Import did not finish",
                f"Stackcopy exited with code {returncode}. Review the detailed "
                "log; files already completed are safe.",
                problems=max(1, problems),
                allow_open=False,
            )

    def _show_zero_plan_rows(self) -> None:
        """Keep the plan visible and explicit when the scan finds no media."""
        self.plan_heading_var.set("Where these 0 files will land")
        self.plan_headline_vars["stack_output"].set(
            "0 finished stacked photos — renamed with ‘stacked’ added to the name"
        )
        self.plan_headline_vars["stack_input"].set(
            "0 frames that fed those stacks — kept in case you want to stack "
            "the RAWs yourself"
        )
        self.plan_headline_vars["other"].set(
            "0 single shots and videos — names untouched, dated folders as "
            "Lightroom would make them"
        )

    def _show_result(
        self,
        heading: str,
        details: str,
        *,
        problems: int,
        allow_open: bool,
        success: bool = False,
    ) -> None:
        self.activity.grid()
        self.running_controls.grid_remove()
        self.result_controls.grid()
        self.open_btn.configure(state="normal" if allow_open else "disabled")
        self.phase_var.set(heading)
        self.meta_var.set(details)
        self.progress.grid_remove()
        self.current_file_label.grid_remove()
        self._update_counter_cards(problems=problems, terminal=success)
        show_empty = bool(
            success
            and self._plan
            and self.mode_var.get() == MOVE_MODE
            and self._plan.get("source_is_removable")
            and self._plan.get("source_would_be_empty_after")
        )
        if show_empty:
            self.card_empty_note.grid()
        else:
            self.card_empty_note.grid_remove()

    # -- log, cancel, open -----------------------------------------------

    def _log_write(self, text: str) -> None:
        self._log_lines.append(text)
        self.log.configure(state="normal")
        self.log.insert("end", text)
        self.log.see("end")
        self.log.configure(state="disabled")

    def _clear_log(self) -> None:
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")

    def _copy_log(self) -> None:
        self.clipboard_clear()
        self.clipboard_append("".join(self._log_lines))
        self.copy_log_btn.configure(text="Copied")
        self.after(1200, lambda: self.copy_log_btn.configure(text="Copy"))

    def _on_cancel(self) -> None:
        process = self._proc
        if process and process.poll() is None:
            self.phase_var.set("Stopping after this file…")
            self._terminate_process(process, "stop")

    def _terminate_process(self, process: subprocess.Popen, action: str) -> bool:
        if process.poll() is not None:
            self._proc = None
            return True
        try:
            process.terminate()
            process.wait(timeout=TERMINATE_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            self._log_write("Stackcopy did not exit after terminate; killing it.\n")
            try:
                process.kill()
                process.wait(timeout=TERMINATE_TIMEOUT_SECONDS)
            except Exception as exc:
                self._log_write(f"Could not {action} Stackcopy: {exc}\n")
                return False
        except Exception as exc:
            self._log_write(f"Could not {action} Stackcopy: {exc}\n")
            return False
        if process.poll() is None:
            return False
        self._proc = None
        self._terminated_by_user = True
        return True

    def _open_dest(self) -> None:
        path = self._last_dest
        if not path or not os.path.isdir(path):
            messagebox.showinfo("Stackcopy", "That folder does not exist yet.")
            return
        try:
            if os.name == "nt":
                os.startfile(path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as exc:
            messagebox.showerror("Stackcopy", f"Could not open folder:\n{exc}")

    def _import_another(self) -> None:
        self.activity.grid_remove()
        self.actions.grid()
        self.src_var.set("")
        self._plan = None
        self._refresh_idle_plan()

    def _on_close(self) -> None:
        process = self._proc
        if process and process.poll() is None:
            if not messagebox.askyesno(
                "Quit", "An import is still running. Stop it and quit?"
            ):
                return
            if not self._terminate_process(process, "stop"):
                return
        self._save_current_defaults()
        self.destroy()


def main() -> None:
    ctk.set_appearance_mode("system")
    ctk.set_default_color_theme("blue")
    StackcopyGUI().mainloop()


if __name__ == "__main__":
    main()
