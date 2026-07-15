#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Stackcopy GUI — a small cross-platform front-end for ``stackcopy.py --lightroomimport``.

It asks for the source (camera card) and the two destination folders, then runs
the existing, battle-tested stackcopy CLI as a subprocess and streams its output
into a live log with a progress bar. None of the import logic lives here; this
file only drives the CLI and renders feedback, so the part that actually moves
your photos stays exactly as tested.

Run from source:   python stackcopy_gui.py
Bundled app:       double-click the app built by PyInstaller (see packaging/).

Requires: customtkinter  (pip install -r requirements-gui.txt)
"""

from __future__ import annotations

import os
import sys
import json
import queue
import threading
import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Frozen-app CLI dispatch
# ---------------------------------------------------------------------------
# When PyInstaller bundles this GUI, sys.executable is the app itself, not a
# Python interpreter, so we can't shell out to "python stackcopy.py". Current
# Windows bundles ship a sibling console-mode StackcopyCLI.exe for subprocess
# imports; this guard remains as a fallback for older/non-Windows bundles that
# relaunch the GUI executable with STACKCOPY_RUN_CLI=1.
if os.environ.get("STACKCOPY_RUN_CLI") == "1":
    import stackcopy

    # argparse reads sys.argv[1:], which already holds the CLI args we passed.
    stackcopy.main()
    sys.exit(0)


import customtkinter as ctk  # noqa: E402  (must follow the CLI dispatch above)
from tkinter import filedialog, messagebox  # noqa: E402

PROGRESS_SENTINEL = "@@SCPROGRESS"
LOW_SPACE_SENTINEL = "@@SCLOWSPACE"
TERMINATE_TIMEOUT_SECONDS = 3.0
APP_NAME = "Stackcopy"
SETTINGS_FILENAME = "gui-state.json"


# ---------------------------------------------------------------------------
# Helpers (module-level so they can be unit-tested without a display)
# ---------------------------------------------------------------------------


def default_dirs() -> tuple[str, str]:
    """Best-effort default destinations, reusing stackcopy's own path logic so
    the GUI pre-fills exactly where the CLI would put files."""
    try:
        import stackcopy

        pics = stackcopy._default_pictures_dir()
    except Exception:
        pics = os.path.join(os.path.expanduser("~"), "Pictures")
    return (
        os.path.join(pics, "Lightroom"),
        os.path.join(pics, "olympus.stack.input.photos"),
    )


def cli_command(cli_args: list[str]) -> tuple[list[str], dict[str, str]]:
    """Build the argv and environment to run the stackcopy CLI, working both
    from source and inside a PyInstaller bundle."""
    env = os.environ.copy()
    if getattr(sys, "frozen", False):
        # Bundled Windows builds include a console-mode helper next to the GUI
        # executable. Relaunching a windowed/no-console PyInstaller executable
        # leaves sys.stdout/sys.stderr unavailable, so prefer the helper when
        # present and keep self-dispatch only as a compatibility fallback.
        helper_name = "StackcopyCLI.exe" if os.name == "nt" else "StackcopyCLI"
        helper = os.path.join(os.path.dirname(sys.executable), helper_name)
        if os.path.exists(helper):
            env.pop("STACKCOPY_RUN_CLI", None)
            return [helper, *cli_args], env
        env["STACKCOPY_RUN_CLI"] = "1"
        return [sys.executable, *cli_args], env
    # From source: run stackcopy.py next to this file with the same interpreter.
    here = os.path.dirname(os.path.abspath(__file__))
    return [sys.executable, os.path.join(here, "stackcopy.py"), *cli_args], env


def parse_progress(line: str) -> tuple[dict[str, str], str | None]:
    """Parse one ``@@SCPROGRESS ...`` line into (fields, filename).

    ``file=`` is always last and may contain spaces, so it is split off before
    the remaining ``key=value`` tokens are parsed."""
    body = line[len(PROGRESS_SENTINEL) :].strip()
    fname: str | None = None
    marker = " file="
    if marker in body:
        body, fname = body.split(marker, 1)
        fname = fname.strip()
    elif body.startswith("file="):
        fname = body[len("file=") :].strip()
        body = ""
    fields: dict[str, str] = {}
    for tok in body.split():
        if "=" in tok:
            key, value = tok.split("=", 1)
            fields[key] = value
    return fields, fname


def parse_low_space_report(line: str) -> dict[str, object] | None:
    try:
        payload = json.loads(line[len(LOW_SPACE_SENTINEL) :].strip())
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def low_space_dialog_message(report: dict[str, object] | None) -> str:
    if not report:
        return (
            "Stackcopy reports the destination is low on free space.\n\n"
            "Proceed anyway?"
        )

    count = report.get("count")
    required_label = f"Required ({count} files)" if count is not None else "Required"
    estimated = str(report.get("estimated_free", "unknown"))
    shortfall = report.get("shortfall")
    if shortfall:
        estimated += f" (short by {shortfall})"

    return (
        "The destination may not have enough free space for this import.\n\n"
        f"Destination:\n{report.get('destination', 'unknown')}\n\n"
        f"Current free space: {report.get('free', 'unknown')}\n"
        f"{required_label}: {report.get('required', 'unknown')}\n"
        f"Estimated free after import: {estimated}\n"
        f"Reserve threshold: {report.get('reserve', 'unknown')}\n\n"
        "Proceed anyway?"
    )


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
    path = _settings_path()
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}
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
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    except Exception:
        # Persistence is best-effort; the GUI must keep working even if the
        # config directory is unwritable.
        return


# ---------------------------------------------------------------------------
# The window
# ---------------------------------------------------------------------------


class StackcopyGUI(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Stackcopy - Lightroom Import")
        self.geometry("780x640")
        self.minsize(700, 580)

        # runtime state
        self._proc: subprocess.Popen | None = None
        self._queue: queue.Queue = queue.Queue()
        self._total = 0
        self._running = False
        self._assume_yes = False
        self._terminated_by_user = False
        self._last_dest: str | None = None
        self._tail: list[str] = []  # recent stdout lines, for diagnosis
        self._low_space_report: dict[str, object] | None = None
        self._pending: tuple[list[str], str, str] | None = None
        self._save_state_scheduled = False
        self._restoring_state = False

        self._entries: list[ctk.CTkEntry] = []
        self._browse_btns: list[ctk.CTkButton] = []
        self._checks: list[ctk.CTkCheckBox] = []

        lr_default, stack_default = default_dirs()

        self.grid_columnconfigure(0, weight=1)

        # --- header ---
        ctk.CTkLabel(
            self,
            text="Lightroom Import",
            font=ctk.CTkFont(size=22, weight="bold"),
        ).grid(row=0, column=0, sticky="w", padx=18, pady=(16, 0))
        ctk.CTkLabel(
            self,
            text=(
                "Sort an Olympus / OM-System card into your Lightroom library, "
                "separating the in-camera stack frames from everything else."
            ),
            text_color=("gray40", "gray70"),
            wraplength=720,
            justify="left",
        ).grid(row=1, column=0, sticky="w", padx=18, pady=(2, 6))

        # --- folder pickers ---
        self.src_var = ctk.StringVar()
        self.dst_var = ctk.StringVar(value=lr_default)
        self.stk_var = ctk.StringVar(value=stack_default)

        self._restore_saved_defaults()
        self.src_var.trace_add("write", self._on_settings_changed)
        self.dst_var.trace_add("write", self._on_settings_changed)
        self.stk_var.trace_add("write", self._on_settings_changed)

        self._dir_row(
            2,
            "Source (camera card)",
            self.src_var,
            "Folder to import from - your SD card, or its DCIM folder",
        )
        self._dir_row(
            3,
            "Lightroom destination",
            self.dst_var,
            "Stacked outputs, single shots, and videos go here",
        )
        self._dir_row(
            4,
            "Stack input frames",
            self.stk_var,
            "The raw frames that fed each in-camera stack go here",
        )

        # --- options ---
        opts = ctk.CTkFrame(self, fg_color="transparent")
        opts.grid(row=5, column=0, sticky="w", padx=18, pady=(8, 0))
        self.dry_var = ctk.BooleanVar(value=False)
        self.verbose_var = ctk.BooleanVar(value=False)
        self.detect_stacks_var = ctk.BooleanVar(value=True)
        self.debug_stacks_var = ctk.BooleanVar(value=False)
        self.leave_on_card_var = ctk.BooleanVar(value=False)
        dry = ctk.CTkCheckBox(
            opts,
            text="Dry run (preview only - moves nothing)",
            variable=self.dry_var,
            command=self._sync_start_label,
        )
        dry.grid(row=0, column=0, sticky="w")
        verbose = ctk.CTkCheckBox(opts, text="Verbose log", variable=self.verbose_var)
        verbose.grid(row=0, column=1, padx=(24, 0), sticky="w")
        detect_stacks = ctk.CTkCheckBox(
            opts, text="Detect stacks", variable=self.detect_stacks_var
        )
        detect_stacks.grid(row=1, column=0, sticky="w", pady=(8, 0))
        debug_stacks = ctk.CTkCheckBox(
            opts, text="Show stack debug output", variable=self.debug_stacks_var
        )
        debug_stacks.grid(row=1, column=1, padx=(24, 0), sticky="w", pady=(8, 0))
        leave_on_card = ctk.CTkCheckBox(
            opts, text="Leave files on card", variable=self.leave_on_card_var
        )
        leave_on_card.grid(row=2, column=0, sticky="w", pady=(8, 0))
        self._checks += [dry, verbose, detect_stacks, debug_stacks, leave_on_card]

        # --- action buttons ---
        actions = ctk.CTkFrame(self, fg_color="transparent")
        actions.grid(row=6, column=0, sticky="we", padx=18, pady=(14, 0))
        actions.grid_columnconfigure(2, weight=1)
        self.start_btn = ctk.CTkButton(
            actions, text="Start import", width=170, height=38, command=self._on_start
        )
        self.start_btn.grid(row=0, column=0)
        self.cancel_btn = ctk.CTkButton(
            actions,
            text="Cancel",
            width=100,
            height=38,
            fg_color="gray38",
            hover_color="gray30",
            state="disabled",
            command=self._on_cancel,
        )
        self.cancel_btn.grid(row=0, column=1, padx=(10, 0))
        self.open_btn = ctk.CTkButton(
            actions,
            text="Open destination",
            width=160,
            height=38,
            fg_color="transparent",
            border_width=1,
            state="disabled",
            command=self._open_dest,
        )
        self.open_btn.grid(row=0, column=3, sticky="e")

        # --- progress + status ---
        self.progress = ctk.CTkProgressBar(self)
        self.progress.grid(row=7, column=0, sticky="we", padx=18, pady=(16, 0))
        self.progress.set(0)
        self.status_var = ctk.StringVar(value="Ready.")
        ctk.CTkLabel(self, textvariable=self.status_var, anchor="w").grid(
            row=8, column=0, sticky="we", padx=18, pady=(4, 0)
        )

        # --- log ---
        self.grid_rowconfigure(9, weight=1)
        self.log = ctk.CTkTextbox(
            self, wrap="none", font=ctk.CTkFont(family=_mono_family(), size=12)
        )
        self.log.grid(row=9, column=0, sticky="nsew", padx=18, pady=(8, 16))
        self.log.configure(state="disabled")

        self._sync_start_label()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(100, self._drain_queue)

    # -- layout helper -----------------------------------------------------

    def _dir_row(self, row: int, label: str, var: ctk.StringVar, hint: str) -> None:
        frame = ctk.CTkFrame(self, fg_color="transparent")
        frame.grid(row=row, column=0, sticky="we", padx=18, pady=(8, 0))
        frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            frame, text=label, anchor="w", font=ctk.CTkFont(weight="bold")
        ).grid(row=0, column=0, columnspan=2, sticky="w")
        entry = ctk.CTkEntry(frame, textvariable=var)
        entry.grid(row=1, column=0, sticky="we", pady=(2, 0))
        browse = ctk.CTkButton(
            frame, text="Browse...", width=92, command=lambda v=var: self._browse(v)
        )
        browse.grid(row=1, column=1, padx=(8, 0), pady=(2, 0))
        ctk.CTkLabel(
            frame,
            text=hint,
            anchor="w",
            justify="left",
            text_color=("gray45", "gray60"),
            font=ctk.CTkFont(size=11),
        ).grid(row=2, column=0, columnspan=2, sticky="w")
        self._entries.append(entry)
        self._browse_btns.append(browse)

    # -- small UI helpers --------------------------------------------------

    def _browse(self, var: ctk.StringVar) -> None:
        start = var.get() or os.path.expanduser("~")
        chosen = filedialog.askdirectory(initialdir=start, title="Choose a folder")
        if chosen:
            var.set(chosen)

    def _restore_saved_defaults(self) -> None:
        saved = load_gui_state()
        self._restoring_state = True
        try:
            self.src_var.set(saved.get("source_dir", ""))
            self.dst_var.set(saved.get("lightroom_dir", self.dst_var.get()))
            self.stk_var.set(saved.get("stack_input_dir", self.stk_var.get()))
        finally:
            self._restoring_state = False

    def _on_settings_changed(self, *_: object) -> None:
        if self._restoring_state:
            return
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
            }
        )

    def _sync_start_label(self) -> None:
        if not self._running:
            self.start_btn.configure(
                text="Preview (dry run)" if self.dry_var.get() else "Start import"
            )

    def _log_write(self, text: str) -> None:
        self.log.configure(state="normal")
        self.log.insert("end", text)
        self.log.see("end")
        self.log.configure(state="disabled")

    def _clear_log(self) -> None:
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")

    def _set_running(self, running: bool) -> None:
        self._running = running
        state = "disabled" if running else "normal"
        for w in (*self._entries, *self._browse_btns, *self._checks):
            w.configure(state=state)
        self.start_btn.configure(state=state)
        self.cancel_btn.configure(state="normal" if running else "disabled")
        if not running:
            self._sync_start_label()

    # -- start / run -------------------------------------------------------

    def _on_start(self) -> None:
        if self._running:
            return
        src = self.src_var.get().strip()
        dst = self.dst_var.get().strip()
        stk = self.stk_var.get().strip()
        if not src or not os.path.isdir(src):
            messagebox.showerror("Stackcopy", "Please choose a valid source folder.")
            return
        if not dst or not stk:
            messagebox.showerror("Stackcopy", "Please choose both destination folders.")
            return
        if os.path.abspath(dst) == os.path.abspath(stk):
            messagebox.showerror(
                "Stackcopy",
                "The Lightroom destination and stack-input folder must be different.",
            )
            return

        args = ["--lightroomimport", src]
        if self.dry_var.get():
            args.append("--dry")
        if self.verbose_var.get():
            args.append("--verbose")
        if not self.detect_stacks_var.get():
            args.append("--no-stack-detection")
        if self.debug_stacks_var.get():
            args.append("--debug-stacks")
        if self.leave_on_card_var.get():
            args.append("--leave-on-card")
        self._pending = (args, dst, stk)
        self._assume_yes = False
        self._launch()

    def _launch(self) -> None:
        assert self._pending is not None
        args, dst, stk = self._pending
        cmd, env = cli_command(args)
        env["STACKCOPY_PROGRESS"] = "1"
        env["STACKCOPY_LOW_SPACE_REPORT"] = "1"
        env["PYTHONUNBUFFERED"] = "1"  # stream stdout live, not in one block
        env["STACKCOPY_LIGHTROOM_IMPORT_DIR"] = dst
        env["STACKCOPY_STACK_INPUT_DIR"] = stk
        env.pop("STACKCOPY_ASSUME_YES", None)
        if self._assume_yes:
            env["STACKCOPY_ASSUME_YES"] = "1"

        self._last_dest = dst
        self._tail = []
        self._low_space_report = None
        self._total = 0
        self._terminated_by_user = False

        self._set_running(True)
        self.open_btn.configure(state="disabled")
        self._clear_log()
        self.status_var.set("Scanning and planning...")
        self.progress.configure(mode="indeterminate")
        self.progress.start()

        threading.Thread(target=self._worker, args=(cmd, env), daemon=True).start()

    def _worker(self, cmd: list[str], env: dict[str, str]) -> None:
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            proc = subprocess.Popen(
                cmd,
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
        except Exception as exc:  # noqa: BLE001 - surfaced to the user
            self._queue.put(("fatal", f"Could not start stackcopy: {exc}\n"))
            return
        self._proc = proc

        def pump(stream, kind: str) -> None:
            for line in iter(stream.readline, ""):
                self._queue.put((kind, line))
            stream.close()

        t_out = threading.Thread(target=pump, args=(proc.stdout, "out"), daemon=True)
        t_err = threading.Thread(target=pump, args=(proc.stderr, "err"), daemon=True)
        t_out.start()
        t_err.start()
        t_out.join()
        t_err.join()
        self._queue.put(("done", proc.wait()))

    # -- main-thread UI pump ----------------------------------------------

    def _drain_queue(self) -> None:
        try:
            while True:
                kind, payload = self._queue.get_nowait()
                if kind == "out":
                    self._tail.append(payload)
                    del self._tail[:-50]
                    self._log_write(payload)
                elif kind == "err":
                    if payload.startswith(PROGRESS_SENTINEL):
                        self._handle_progress(payload)
                    elif payload.startswith(LOW_SPACE_SENTINEL):
                        self._low_space_report = parse_low_space_report(payload)
                        self.status_var.set(
                            "Low disk space - waiting for confirmation."
                        )
                    else:
                        self._log_write(payload)
                elif kind == "fatal":
                    self._log_write(payload)
                    self.progress.stop()
                    self.progress.configure(mode="determinate")
                    self.progress.set(0)
                    self.status_var.set("Failed to start - see log.")
                    self._set_running(False)
                elif kind == "done":
                    self._handle_done(int(payload))
        except queue.Empty:
            pass
        self.after(100, self._drain_queue)

    def _handle_progress(self, line: str) -> None:
        fields, fname = parse_progress(line)
        phase = fields.get("phase")
        total = int(fields.get("total", "0") or "0")
        done = int(fields.get("done", "0") or "0")
        if phase == "start":
            self._total = total
            self.progress.stop()
            self.progress.configure(mode="determinate")
            self.progress.set(0)
            self.status_var.set(
                "Nothing to import - no matching files found."
                if total == 0
                else f"Importing...  0 / {total}"
            )
        elif phase in {"move", "copy"}:
            self._total = total or self._total
            if self._total:
                self.progress.set(done / self._total)
            if self.dry_var.get():
                action = "Previewing"
            else:
                action = "Copying" if self.leave_on_card_var.get() else "Moving"
            self.status_var.set(f"{action} {fname or ''}   ({done} / {self._total})")
        elif phase == "done":
            self.progress.set(1.0 if total else 0)

    def _handle_done(self, rc: int) -> None:
        terminated_by_user = self._terminated_by_user
        self._terminated_by_user = False
        self._proc = None
        self.progress.stop()
        self._set_running(False)

        if terminated_by_user:
            self.status_var.set("Cancelled.")
            return

        if rc != 0 and self._low_space_report is not None and not self._assume_yes:
            if messagebox.askyesno(
                "Low disk space",
                low_space_dialog_message(self._low_space_report),
            ):
                self._assume_yes = True
                self._launch()
            else:
                self.status_var.set("Aborted: low disk space.")
            return

        if rc == 0:
            self.progress.set(1.0 if self._total else 0)
            if self._total == 0:
                self.status_var.set("Nothing to import - no matching files found.")
            elif self.dry_var.get():
                self.status_var.set("Preview complete - no files were changed.")
            else:
                action = "copied" if self.leave_on_card_var.get() else "moved"
                self.status_var.set(f"Import complete - {self._total} files {action}.")
                self.open_btn.configure(state="normal")
        else:
            self.status_var.set(f"stackcopy exited with code {rc} - see log.")

    # -- cancel / open / close --------------------------------------------

    def _on_cancel(self) -> None:
        proc = self._proc
        if proc and proc.poll() is None:
            # Each file move is atomic and the import is re-runnable, so stopping
            # between files is safe.
            self.status_var.set("Cancelling...")
            if self._terminate_process(proc, "cancel"):
                self.status_var.set("Cancelled.")

    def _terminate_process(self, proc: subprocess.Popen, action: str) -> bool:
        if proc.poll() is not None:
            self._proc = None
            return True
        try:
            proc.terminate()
            proc.wait(timeout=TERMINATE_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            self._log_write("stackcopy did not exit after terminate; killing it.\n")
            try:
                proc.kill()
                proc.wait(timeout=TERMINATE_TIMEOUT_SECONDS)
            except Exception as exc:  # noqa: BLE001 - surfaced to the user
                self._log_write(f"Could not {action} stackcopy: {exc}\n")
                self.status_var.set(f"Could not {action} - see log.")
                return False
        except Exception as exc:  # noqa: BLE001 - surfaced to the user
            self._log_write(f"Could not {action} stackcopy: {exc}\n")
            self.status_var.set(f"Could not {action} - see log.")
            return False

        if proc.poll() is None:
            self.status_var.set(f"Could not {action} - see log.")
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
            if sys.platform == "darwin":
                subprocess.Popen(["open", path])
            elif os.name == "nt":
                os.startfile(path)  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Stackcopy", f"Could not open folder:\n{exc}")

    def _on_close(self) -> None:
        proc = self._proc
        if proc and proc.poll() is None:
            if not messagebox.askyesno(
                "Quit", "An import is still running. Stop it and quit?"
            ):
                return
            self.status_var.set("Stopping import...")
            if not self._terminate_process(proc, "stop"):
                return
        self._save_current_defaults()
        self.destroy()


def main() -> None:
    ctk.set_appearance_mode("system")
    ctk.set_default_color_theme("blue")
    StackcopyGUI().mainloop()


if __name__ == "__main__":
    main()
