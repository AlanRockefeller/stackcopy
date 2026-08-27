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

try:
    from tkinter import filedialog, messagebox

    import customtkinter as ctk
except ImportError as exc:
    # Keep the dependency reporter independent of Tk/customtkinter so a broken
    # GUI installation can explain itself instead of ending in a traceback.
    if __name__ == "__main__":
        from stackcopy_gui_bootstrap import show_dependency_error

        show_dependency_error(exc)
        raise SystemExit(1) from None
    raise

try:
    from stackcopy import (  # noqa: E402
        EXIFTOOL_DOWNLOAD_URL,
        EXIFTOOL_MINIMUM_OM_SYSTEM_VERSION_TEXT,
        STACKCOPY_VERSION,
        path_is_within,
    )
except Exception:  # pragma: no cover - fallback for a broken old bundle
    STACKCOPY_VERSION = ""
    EXIFTOOL_DOWNLOAD_URL = "https://exiftool.org/"
    EXIFTOOL_MINIMUM_OM_SYSTEM_VERSION_TEXT = "12.41"

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


try:
    import stackcopy_updater  # noqa: E402
except Exception:  # pragma: no cover - a bundle missing the module must still run
    stackcopy_updater = None


PROGRESS_SENTINEL = "@@SCPROGRESS"
LOW_SPACE_SENTINEL = "@@SCLOWSPACE"
TERMINATE_TIMEOUT_SECONDS = 3.0
APP_NAME = "Stackcopy"
# One version string for the CLI, the GUI, the packaged app's metadata and the
# release tag: imported from stackcopy.py rather than repeated here, so they
# cannot drift apart.  The fallback above only ever fires for a bundle whose
# stackcopy module failed to load, in which case there is no version to show.
APP_TITLE = f"{APP_NAME} {STACKCOPY_VERSION}".strip()
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


def exiftool_status_display(
    plan: dict[str, object] | None,
) -> tuple[str, str, bool] | None:
    """Render the CLI's ExifTool capability report for the header strip.

    Returns (message, tone, offer_download) or None when there is nothing to
    say yet.  ``tone`` is "ok" or "warn"; only a warn state offers the
    download link, and the link is only ever a link - the GUI never fetches
    or installs anything on its own.

    Reads the machine-readable fields from --plan-json rather than scraping
    the human sentences the CLI prints, so the two can be worded
    independently.
    """
    if not plan:
        return None
    status = plan.get("exiftool_status")
    if not isinstance(status, str):
        return None
    version = plan.get("exiftool_version_label") or plan.get("exiftool_version")
    version_text = str(version) if isinstance(version, str) and version else "?"
    minimum = str(
        plan.get("exiftool_minimum_version") or EXIFTOOL_MINIMUM_OM_SYSTEM_VERSION_TEXT
    )
    if status == "om_system_supported":
        where = " (bundled)" if plan.get("exiftool_source") == "bundled" else ""
        return (
            f"ExifTool {version_text}{where} — OM-1 stack metadata enabled",
            "ok",
            False,
        )
    if status == "too_old":
        return (
            f"ExifTool {version_text} is too old for OM SYSTEM MakerNotes. "
            "Stackcopy will use conservative stack detection, which can miss "
            "a stacked JPG whose ORF frames are not alongside it. "
            f"Update ExifTool ({minimum} or newer).",
            "warn",
            True,
        )
    if status == "unusable":
        return (
            "ExifTool could not be used — falling back to conservative stack "
            "detection, which can miss a stacked JPG whose ORF frames are not "
            f"alongside it. Install ExifTool {minimum} or newer.",
            "warn",
            True,
        )
    if status == "missing":
        return (
            "ExifTool not found — using conservative stack detection, which "
            "can miss a stacked JPG whose ORF frames are not alongside it. "
            f"Install ExifTool {minimum} or newer for OM-1 camera metadata.",
            "warn",
            True,
        )
    return None


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


def source_will_be_empty(
    plan: dict[str, object] | None, *, leave_on_card: bool
) -> bool:
    """Derive the mode-sensitive card-empty expectation from a cached plan."""
    return bool(
        plan
        and not leave_on_card
        and plan.get("source_is_removable")
        and plan.get("source_would_be_empty_after")
    )


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


def success_metrics(elapsed: float, byte_count: int, problems: int = 0) -> str:
    """Build the compact success meta line used by the terminal result."""
    safe_elapsed = max(0.001, elapsed)
    details = [format_duration(safe_elapsed)]
    if byte_count:
        details.extend(
            (format_bytes(byte_count), f"{format_bytes(byte_count / safe_elapsed)}/s")
        )
    if problems:
        details.append(f"{problems} {'problem' if problems == 1 else 'problems'}")
    else:
        details.append("nothing failed")
    return " · ".join(details)


def bundled_changelog_path() -> Path | None:
    """Locate ChangeLog.md in a source tree or inside a packaged build.

    Mirrors how ``stackcopy._bundled_exiftool_path`` searches: PyInstaller puts
    data files under ``sys._MEIPASS``, beside the executable, or in
    ``_internal/`` depending on the build style.
    """
    roots: list[Path] = [Path(__file__).resolve().parent]
    bundle_dir = getattr(sys, "_MEIPASS", None)
    if bundle_dir:
        roots.append(Path(bundle_dir))
    if getattr(sys, "frozen", False):
        executable_dir = Path(sys.executable).resolve().parent
        roots.append(executable_dir)
        roots.append(executable_dir / "_internal")
    for root in roots:
        candidate = root / "ChangeLog.md"
        try:
            if candidate.is_file():
                return candidate
        except OSError:  # pragma: no cover - unreadable bundle path
            continue
    return None


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


# gui-state.json began as a flat string-to-string map.  Booleans and numbers
# are now stored as themselves - a settings file a human opens should say
# ``true``, not ``"true"`` - but every reader below still accepts the old
# string spelling, so a file written by an earlier Stackcopy loads unchanged.
STATE_VALUE_TYPES = (str, bool, int, float)


def load_gui_state() -> dict[str, object]:
    """Read the saved settings. A missing or damaged file is simply no settings.

    Nothing in here may raise: a corrupt gui-state.json - including corrupt
    update-checker fields - must never be the reason the window fails to open.
    """
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
        if isinstance(key, str) and isinstance(value, STATE_VALUE_TYPES)
    }


def state_text(state: dict[str, object], key: str, default: str = "") -> str:
    """A stored string, tolerating a value some older build wrote as a number."""
    value = state.get(key, None)
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return default


def state_flag(state: dict[str, object], key: str, default: bool = False) -> bool:
    """A stored boolean, whether it was written as JSON true or as "true"."""
    value = state.get(key, None)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "yes", "1", "on"):
            return True
        if text in ("false", "no", "0", "off"):
            return False
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def save_gui_state(state: dict[str, object]) -> None:
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
        self.title(f"{APP_TITLE} — Import from card")
        self.geometry("920x860")
        self.minsize(820, 760)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._proc: subprocess.Popen | None = None
        self._plan_proc: subprocess.Popen | None = None
        self._plan_proc_lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue()
        self._running = False
        self._closing = False
        self._plan_scanning = False
        self._plan: dict[str, object] | None = None
        self._plan_generation = 0
        self._plan_after: str | None = None
        self._plan_slow_after: str | None = None
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
        self._update_info = None
        self._update_generation = 0
        self._update_checking = False
        self._update_manual = False
        self._update_dialog = None
        self._update_after: str | None = None

        lightroom_default, stack_default = default_dirs()
        # The whole settings file is kept in memory and written back whole, so
        # keys this screen does not own - the update checker's, for instance -
        # survive every save instead of being dropped on the next write.
        saved = load_gui_state()
        self._state: dict[str, object] = dict(saved)
        self.src_var = ctk.StringVar(value=state_text(saved, "source_dir"))
        self.dst_var = ctk.StringVar(
            value=state_text(saved, "lightroom_dir", lightroom_default)
        )
        self.stk_var = ctk.StringVar(
            value=state_text(saved, "stack_input_dir", stack_default)
        )
        self.mode_var = ctk.StringVar(
            value=COPY_MODE if state_text(saved, "file_mode") == "copy" else MOVE_MODE
        )
        self.verbose_var = ctk.BooleanVar(value=state_flag(saved, "verbose"))
        self.detect_stacks_var = ctk.BooleanVar(
            value=state_flag(saved, "detect_stacks", True)
        )
        self.debug_stacks_var = ctk.BooleanVar(value=state_flag(saved, "debug_stacks"))
        self._advanced_open = state_flag(saved, "advanced_open")
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
        self._schedule_automatic_update_check()

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
        version_row = ctk.CTkFrame(header, fg_color="transparent")
        version_row.grid(row=0, column=1, sticky="e", padx=(10, 0))
        ctk.CTkLabel(
            version_row,
            text=APP_TITLE,
            anchor="e",
            font=ctk.CTkFont(size=12),
            text_color=("gray45", "gray62"),
        ).grid(row=0, column=0, sticky="e")
        # A permanent, always-available manual check. It sits beside the
        # version it is about, which is where somebody wondering "am I
        # current?" is already looking.
        self.update_check_btn = self._text_button(
            version_row, "Check for Updates", self._check_for_updates_manually
        )
        self.update_check_btn.grid(row=0, column=1, sticky="e", padx=(6, 0))
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
        ).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5, 0))

        # An unobtrusive capability line rather than a dialog: ExifTool is
        # optional, so this reports what Stackcopy can do, and only offers a
        # link when something is actually worse without it.
        self.exiftool_row = ctk.CTkFrame(header, fg_color="transparent")
        self.exiftool_row.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(7, 0))
        self.exiftool_row.grid_columnconfigure(0, weight=1)
        self.exiftool_var = ctk.StringVar(value="")
        self.exiftool_label = ctk.CTkLabel(
            self.exiftool_row,
            textvariable=self.exiftool_var,
            anchor="w",
            justify="left",
            wraplength=700,
            font=ctk.CTkFont(size=12),
            text_color=("gray45", "gray62"),
        )
        self.exiftool_label.grid(row=0, column=0, sticky="ew")
        self.exiftool_link = self._text_button(
            self.exiftool_row, "Get ExifTool", self._open_exiftool_page
        )
        self.exiftool_link.grid(row=0, column=1, padx=(10, 0))
        self.exiftool_row.grid_remove()

        # The automatic check reports itself here rather than in a dialog.
        # Nobody launched Stackcopy to be interrupted by a version number;
        # the details are one click away for anyone who wants them.
        self.update_row = ctk.CTkFrame(header, fg_color="transparent")
        self.update_row.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(7, 0))
        self.update_row.grid_columnconfigure(0, weight=1)
        self.update_var = ctk.StringVar(value="")
        self.update_label = ctk.CTkLabel(
            self.update_row,
            textvariable=self.update_var,
            anchor="w",
            justify="left",
            wraplength=700,
            font=ctk.CTkFont(size=12),
            text_color=("gray45", "gray62"),
        )
        self.update_label.grid(row=0, column=0, sticky="ew")
        self.update_view_btn = self._text_button(
            self.update_row, "View update", self._show_update_dialog
        )
        self.update_view_btn.grid(row=0, column=1, padx=(10, 0))
        self.update_dismiss_btn = self._text_button(
            self.update_row, "Dismiss", self._hide_update_notice
        )
        self.update_dismiss_btn.grid(row=0, column=2, padx=(4, 0))
        self.update_row.grid_remove()

    def _open_exiftool_page(self) -> None:
        """Open the official download page. Never downloads anything itself."""
        self._open_url(EXIFTOOL_DOWNLOAD_URL)

    def _refresh_exiftool_status(self) -> None:
        display = exiftool_status_display(self._plan)
        if display is None:
            # A cleared or failed plan says nothing new about ExifTool, and
            # its state cannot change while the app is running, so the last
            # answer stays put rather than blinking out.
            return
        message, tone, offer_download = display
        self.exiftool_var.set(message)
        self.exiftool_label.configure(
            text_color=(
                ("#8a5a00", "#e0b050") if tone == "warn" else ("gray45", "gray62")
            )
        )
        if offer_download:
            self.exiftool_link.grid()
        else:
            self.exiftool_link.grid_remove()
        self.exiftool_row.grid()

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
        self.result_body_var = ctk.StringVar(value="")
        self.result_body_label = ctk.CTkLabel(
            top,
            textvariable=self.result_body_var,
            anchor="w",
            justify="left",
            wraplength=820,
            text_color=("gray32", "gray74"),
        )
        self.result_body_label.grid(
            row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0)
        )
        self.result_body_label.grid_remove()
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
        def current_colors() -> tuple[str, str]:
            try:
                background = self._apply_appearance_mode(parent.cget("fg_color"))
                if background == "transparent":
                    background = self._apply_appearance_mode(self.cget("fg_color"))
                button_theme = ctk.ThemeManager.theme["CTkButton"]
                ink = self._apply_appearance_mode(button_theme["fg_color"])
                return background, ink
            except Exception:
                return "#242424", "#1f6aa5"

        color, ink = current_colors()
        canvas = ctk.CTkCanvas(
            parent, width=38, height=38, highlightthickness=0, bg=color
        )
        canvas.grid(row=row, column=0, rowspan=2, padx=(13, 9), pady=7)
        if kind == "card":
            canvas.create_polygon(
                9, 5, 28, 5, 33, 10, 33, 33, 9, 33, fill=ink, tags="icon_fill"
            )
            canvas.create_rectangle(
                14, 9, 27, 17, fill=color, outline=color, tags="icon_bg"
            )
        elif kind == "photo":
            canvas.create_rectangle(
                5, 7, 33, 31, outline=ink, width=3, tags="icon_outline"
            )
            canvas.create_oval(22, 11, 28, 17, fill=ink, outline=ink, tags="icon_fill")
            canvas.create_polygon(
                8,
                28,
                17,
                17,
                23,
                24,
                27,
                20,
                31,
                28,
                fill=ink,
                tags="icon_fill",
            )
        elif kind == "frames":
            for offset in (0, 4, 8):
                canvas.create_rectangle(
                    5 + offset,
                    7 + offset,
                    25 + offset,
                    27 + offset,
                    outline=ink,
                    width=2,
                    tags="icon_outline",
                )
        else:
            canvas.create_rectangle(
                5, 12, 33, 31, outline=ink, width=3, tags="icon_outline"
            )
            canvas.create_polygon(
                5,
                12,
                15,
                12,
                18,
                8,
                28,
                8,
                31,
                12,
                fill=ink,
                tags="icon_fill",
            )

        def refresh_icon(_appearance_mode: str) -> None:
            background, foreground = current_colors()
            canvas.configure(bg=background)
            canvas.itemconfigure("icon_fill", fill=foreground, outline=foreground)
            canvas.itemconfigure("icon_outline", outline=foreground)
            canvas.itemconfigure("icon_bg", fill=background, outline=background)

        ctk.AppearanceModeTracker.add(refresh_icon, canvas)
        canvas.bind(
            "<Destroy>",
            lambda _event: ctk.AppearanceModeTracker.remove(refresh_icon),
            add="+",
        )

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
        self._refresh_idle_plan()

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
        self._state.update(
            {
                "source_dir": self.src_var.get(),
                "lightroom_dir": self.dst_var.get(),
                "stack_input_dir": self.stk_var.get(),
                "file_mode": "copy" if self.mode_var.get() == COPY_MODE else "move",
                "verbose": bool(self.verbose_var.get()),
                "detect_stacks": bool(self.detect_stacks_var.get()),
                "debug_stacks": bool(self.debug_stacks_var.get()),
                "advanced_open": bool(self._advanced_open),
            }
        )
        save_gui_state(self._state)

    def _schedule_plan_scan(self) -> None:
        if self._running:
            return
        self._cancel_plan_scan()
        if self._plan_after is not None:
            self.after_cancel(self._plan_after)
        source = self.src_var.get().strip()
        self._set_plan_scanning(bool(source and os.path.isdir(source)))
        self._plan_after = self.after(350, self._begin_plan_scan)

    def _begin_plan_scan(self) -> None:
        self._plan_after = None
        source = self.src_var.get().strip()
        if not source or not os.path.isdir(source):
            self._plan = None
            self._set_plan_scanning(False)
            self._refresh_idle_plan()
            return
        generation = self._plan_generation
        self._set_plan_scanning(True)
        self._plan_slow_after = self.after(
            2000, lambda: self._show_slow_plan_scan(generation)
        )
        args = ["--lightroomimport", source, "--plan-json"]
        if not self.detect_stacks_var.get():
            args.append("--no-stack-detection")
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
        process: subprocess.Popen | None = None
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
                creationflags=creationflags,
            )
            with self._plan_proc_lock:
                superseded = generation != self._plan_generation or self._closing
                if not superseded:
                    self._plan_proc = process
            if superseded:
                self._stop_plan_process(process)
            stdout, _stderr = process.communicate()
            payload = parse_plan_json(stdout) if process.returncode == 0 else None
            self._queue.put(("plan", (generation, payload)))
        except Exception:
            self._queue.put(("plan", (generation, None)))
        finally:
            with self._plan_proc_lock:
                if self._plan_proc is process:
                    self._plan_proc = None

    @staticmethod
    def _stop_plan_process(process: subprocess.Popen) -> None:
        """Terminate a superseded scan without blocking the Tk event loop."""
        if process.poll() is not None:
            return
        try:
            process.terminate()
            process.wait(timeout=TERMINATE_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            try:
                process.kill()
            except Exception:
                pass
        except Exception:
            pass

    def _cancel_plan_scan(self) -> None:
        with self._plan_proc_lock:
            process = self._plan_proc
            self._plan_proc = None
        if process and process.poll() is None:
            threading.Thread(
                target=self._stop_plan_process, args=(process,), daemon=True
            ).start()

    def _set_plan_scanning(self, scanning: bool) -> None:
        self._plan_scanning = scanning
        if self._plan_slow_after is not None:
            self.after_cancel(self._plan_slow_after)
            self._plan_slow_after = None
        if scanning:
            self.source_scan_var.set("Scanning card…")
        self.start_btn.configure(
            state="disabled" if scanning or self._running else "normal"
        )

    def _show_slow_plan_scan(self, generation: int) -> None:
        self._plan_slow_after = None
        if self._plan_scanning and generation == self._plan_generation:
            self.source_scan_var.set(
                "Scanning card… this can take a moment on a large card."
            )

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
        elif plan is None and not self._plan_scanning:
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
        self._refresh_exiftool_status()
        self._set_plan_scanning(False)
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
        self._cancel_plan_scan()
        self._set_plan_scanning(False)
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
        self.result_body_var.set("")
        self.result_body_label.grid_remove()
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
                elif kind == "update":
                    self._handle_update_result(payload)
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
            role = fields.get("role", "other")
            self._current_role = role if role in self._bucket_done else "other"
            self._bucket_done[self._current_role] += 1
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

    def _update_counter_cards(self, problems: int = 0) -> None:
        for role in ("stack_output", "stack_input", "other"):
            total = self._bucket_total(role)
            done = self._bucket_done[role]
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
                success_metrics(elapsed, byte_count, problems),
                problems=problems,
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
        self.meta_var.set(details if success else "")
        self.result_body_var.set("" if success else details)
        if success or not details:
            self.result_body_label.grid_remove()
        else:
            self.result_body_label.grid()
        self.progress.grid_remove()
        self.current_file_label.grid_remove()
        self._update_counter_cards(problems=problems)
        show_empty = (
            success
            and source_will_be_empty(
                self._plan, leave_on_card=self.mode_var.get() == COPY_MODE
            )
            and problems == 0
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
        self._plan = None
        self._plan_generation += 1
        self._refresh_idle_plan()
        self._schedule_plan_scan()

    # -- update notifications --------------------------------------------
    #
    # Stackcopy only ever *tells* you about a new version. It does not
    # download, replace, install, or restart anything, and the one URL it
    # will open is checked against this project's own release pages first.

    def _updates_available(self) -> bool:
        return stackcopy_updater is not None and bool(STACKCOPY_VERSION)

    def _schedule_automatic_update_check(self) -> None:
        """Queue the startup check. Never blocks the window from appearing."""
        if not self._updates_available():
            return
        delay = int(stackcopy_updater.STARTUP_DELAY_SECONDS * 1000)
        self._update_after = self.after(delay, self._run_automatic_update_check)

    def _run_automatic_update_check(self) -> None:
        self._update_after = None
        if self._closing or not self._updates_available():
            return
        try:
            due = stackcopy_updater.should_check_automatically(self._state)
        except Exception:
            # Unreadable updater state is not worth a crash; skip this launch.
            return
        if due:
            self._start_update_check(manual=False)

    def _check_for_updates_manually(self) -> None:
        """The Check for Updates button. Always runs - cooldowns do not apply."""
        if not self._updates_available():
            message = "This build cannot check for updates."
            if stackcopy_updater is not None:
                message += f"\n\nSee {stackcopy_updater.RELEASES_URL}"
            messagebox.showinfo(APP_NAME, message)
            return
        if self._update_checking:
            self._set_update_notice("Already checking for updates…")
            return
        self._start_update_check(manual=True)

    def _start_update_check(self, manual: bool) -> None:
        self._update_checking = True
        self._update_manual = manual
        self._update_generation += 1
        generation = self._update_generation
        current = STACKCOPY_VERSION

        if manual:
            self.update_check_btn.configure(state="disabled")
            self._set_update_notice("Checking GitHub for a newer version…")

        def worker() -> None:
            payload: dict[str, object] = {
                "generation": generation,
                "manual": manual,
                "info": None,
                "error": "",
            }
            try:
                payload["info"] = stackcopy_updater.check_for_update(current)
            except stackcopy_updater.UpdateCheckError as exc:
                payload["error"] = str(exc)
            except Exception as exc:  # pragma: no cover - defensive
                payload["error"] = f"The update check failed: {exc}"
            self._queue.put(("update", payload))

        threading.Thread(
            target=worker, name="stackcopy-update-check", daemon=True
        ).start()

    def _handle_update_result(self, payload: dict) -> None:
        """Runs on the UI thread, out of the existing queue drain."""
        if self._closing:
            return
        if payload.get("generation") != self._update_generation:
            return
        self._update_checking = False
        manual = bool(payload.get("manual"))
        try:
            self.update_check_btn.configure(state="normal")
        except Exception:  # pragma: no cover - teardown races
            pass

        error = str(payload.get("error") or "")
        if error:
            # A background failure is recorded and forgotten. Being offline is
            # not an error the user asked to hear about.
            stackcopy_updater.record_failure(self._state)
            self._save_current_defaults()
            if manual:
                self._set_update_notice(f"Could not check for updates — {error}")
                messagebox.showwarning(
                    APP_NAME,
                    f"Stackcopy could not check for updates.\n\n{error}\n\n"
                    f"You can look at the releases page yourself:\n"
                    f"{stackcopy_updater.RELEASES_URL}",
                )
            else:
                self._hide_update_notice()
            return

        info = payload.get("info")
        stackcopy_updater.record_success(self._state)
        self._save_current_defaults()
        self._update_info = info

        if not stackcopy_updater.should_notify(info, self._state, manual=manual):
            # Nothing newer, or an automatic check finding a skipped version.
            if manual:
                self._set_update_notice(
                    f"Stackcopy {info.current_version} is the latest version."
                )
                messagebox.showinfo(
                    APP_NAME,
                    f"Stackcopy {info.current_version} is up to date.\n\n"
                    f"The newest release on GitHub is {info.latest_version}.",
                )
            else:
                self._hide_update_notice()
            return

        self._notify_update(info)
        if manual:
            self._show_update_dialog()

    def _notify_update(self, info) -> None:
        self._update_info = info
        self._set_update_notice(f"{info.headline} — you have {info.current_version}.")
        self.update_label.configure(text_color=("#1f6aa5", "#5aa7df"))
        self.update_view_btn.grid()
        self.update_dismiss_btn.grid()

    def _set_update_notice(self, message: str) -> None:
        self.update_var.set(message)
        self.update_label.configure(text_color=("gray45", "gray62"))
        self.update_view_btn.grid_remove()
        self.update_dismiss_btn.grid_remove()
        self.update_row.grid()

    def _hide_update_notice(self) -> None:
        self.update_var.set("")
        self.update_row.grid_remove()

    def _show_update_dialog(self) -> None:
        info = self._update_info
        if info is None or stackcopy_updater is None:
            return
        existing = self._update_dialog
        try:
            if existing is not None and existing.winfo_exists():
                existing.focus()
                return
        except Exception:  # pragma: no cover - the dialog went away underneath us
            self._update_dialog = None

        dialog = ctk.CTkToplevel(self)
        self._update_dialog = dialog
        dialog.title("Update available")
        dialog.geometry("620x460")
        dialog.transient(self)
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(2, weight=1)

        ctk.CTkLabel(
            dialog,
            text=info.headline,
            anchor="w",
            font=ctk.CTkFont(size=21, weight="bold"),
        ).grid(row=0, column=0, sticky="ew", padx=20, pady=(20, 2))
        installed = f"You have Stackcopy {info.current_version}."
        if info.published_at[:10]:
            installed += f"  Released {info.published_at[:10]}."
        ctk.CTkLabel(
            dialog,
            text=installed,
            anchor="w",
            text_color=("gray38", "gray66"),
        ).grid(row=1, column=0, sticky="ew", padx=20)

        notes = ctk.CTkTextbox(dialog, wrap="word", font=ctk.CTkFont(size=12))
        notes.grid(row=2, column=0, sticky="nsew", padx=20, pady=(12, 8))
        notes.insert(
            "1.0",
            info.notes
            or "No release notes were published. Open the release page for details.",
        )
        notes.configure(state="disabled")

        ctk.CTkLabel(
            dialog,
            text=(
                "Stackcopy never downloads or installs an update by itself. "
                "Opening the release page lets you decide."
            ),
            anchor="w",
            justify="left",
            wraplength=560,
            font=ctk.CTkFont(size=11),
            text_color=("gray45", "gray62"),
        ).grid(row=3, column=0, sticky="ew", padx=20)

        buttons = ctk.CTkFrame(dialog, fg_color="transparent")
        buttons.grid(row=4, column=0, sticky="ew", padx=20, pady=(12, 18))
        buttons.grid_columnconfigure(1, weight=1)
        self._text_button(buttons, "Skip This Version", self._skip_this_version).grid(
            row=0, column=0, sticky="w"
        )
        ctk.CTkButton(
            buttons,
            text="Remind Me Later",
            width=130,
            fg_color="transparent",
            border_width=1,
            command=self._remind_me_later,
        ).grid(row=0, column=2, padx=(0, 8))
        ctk.CTkButton(
            buttons,
            text="View Changelog",
            width=130,
            fg_color="transparent",
            border_width=1,
            command=self._open_changelog,
        ).grid(row=0, column=3, padx=(0, 8))
        ctk.CTkButton(
            buttons, text="Open Release", width=120, command=self._open_release_page
        ).grid(row=0, column=4)

        dialog.protocol("WM_DELETE_WINDOW", self._close_update_dialog)
        # Grab only after the window exists, or the lift races the map on X11.
        dialog.after(120, lambda: self._focus_dialog(dialog))

    def _focus_dialog(self, dialog) -> None:
        try:
            if dialog.winfo_exists():
                dialog.lift()
                dialog.focus_force()
        except Exception:  # pragma: no cover - platform dependent
            pass

    def _remind_me_later(self) -> None:
        """Dismiss this notification without remembering anything about it.

        Nothing is persisted, so the next ordinary check raises the same
        version again - which is the whole difference from Skip This Version.
        """
        self._close_update_dialog()
        self._hide_update_notice()

    def _close_update_dialog(self) -> None:
        """Close the dialog only; the header notice stays until dismissed."""
        dialog = self._update_dialog
        self._update_dialog = None
        if dialog is not None:
            try:
                dialog.destroy()
            except Exception:  # pragma: no cover - teardown races
                pass

    def _skip_this_version(self) -> None:
        info = self._update_info
        if info is not None and stackcopy_updater is not None:
            # The normalized application version, so every -buildN re-cut of
            # it stays skipped as well.
            stackcopy_updater.record_skip(self._state, info.latest_version)
            self._save_current_defaults()
        self._close_update_dialog()
        self._hide_update_notice()

    def _open_release_page(self) -> None:
        info = self._update_info
        if info is None or stackcopy_updater is None:
            return
        # Already validated when the response was parsed; checked again here so
        # the guarantee lives next to the browser call.
        url = stackcopy_updater.safe_release_url(info.release_url)
        self._close_update_dialog()
        self._open_url(url)

    def _open_changelog(self) -> None:
        """Prefer the copy that shipped with this build; fall back to GitHub."""
        local = bundled_changelog_path()
        if local is not None and self._open_url(local.as_uri(), quiet=True):
            return
        self._open_url(stackcopy_updater.CHANGELOG_URL)

    def _open_url(self, url: str, quiet: bool = False) -> bool:
        import webbrowser

        try:
            if webbrowser.open(url):
                return True
        except Exception:  # pragma: no cover - platform dependent
            pass
        if not quiet:
            messagebox.showerror(APP_NAME, f"Could not open:\n{url}")
        return False

    def _on_close(self) -> None:
        process = self._proc
        if process and process.poll() is None:
            if not messagebox.askyesno(
                "Quit", "An import is still running. Stop it and quit?"
            ):
                return
            if not self._terminate_process(process, "stop"):
                return
        self._closing = True
        if self._update_after is not None:
            try:
                self.after_cancel(self._update_after)
            except Exception:
                pass
            self._update_after = None
        if self._plan_after is not None:
            self.after_cancel(self._plan_after)
            self._plan_after = None
        self._cancel_plan_scan()
        self._save_current_defaults()
        self.destroy()


def main() -> None:
    ctk.set_appearance_mode("system")
    ctk.set_default_color_theme("blue")
    StackcopyGUI().mainloop()


if __name__ == "__main__":
    main()
