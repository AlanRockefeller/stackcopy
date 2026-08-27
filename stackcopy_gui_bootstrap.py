#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Dependency-error reporting that does not depend on Stackcopy's GUI stack."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

APP_NAME = "Stackcopy"


def dependency_error_message(error: ImportError) -> str:
    """Explain a GUI import failure and give the appropriate repair action."""
    missing = getattr(error, "name", None) or "an unknown GUI module"
    detail = f"Missing module: {missing}"

    if getattr(sys, "frozen", False):
        return (
            "Stackcopy cannot start because its packaged GUI libraries are missing "
            "or damaged.\n\n"
            f"{detail}\n\nReinstall Stackcopy from a complete release download."
        )

    if missing == "_tkinter" or missing == "tkinter" or missing.startswith("tkinter."):
        if sys.platform == "darwin":
            fix = (
                "Install a Python build with Tk support. If you use Homebrew, run:\n\n"
                "brew install python-tk"
            )
        elif os.name == "nt":
            fix = (
                "Repair or reinstall Python and include the optional Tcl/Tk and IDLE "
                "feature."
            )
        else:
            fix = (
                "Install Tk for your Python version. For Debian or Ubuntu, run:\n\n"
                "sudo apt install python3-tk\n\n"
                "On Fedora, run:\n\n"
                "sudo dnf install python3-tkinter"
            )
        return (
            "Stackcopy cannot start because Python's Tk GUI support is not installed.\n\n"
            f"{detail}\n\n{fix}\n\nThen start Stackcopy again."
        )

    requirements = Path(__file__).resolve().with_name("requirements-gui.txt")
    command = f'"{sys.executable}" -m pip install -r "{requirements}"'
    return (
        "Stackcopy cannot start because a required GUI library is not installed.\n\n"
        f"{detail}\n\nInstall the GUI requirements by running:\n\n"
        f"{command}\n\nThen start Stackcopy again."
    )


def _tk_dialog(title: str, message: str) -> bool:
    """Try the standard Tk error dialog, without relying on customtkinter."""
    root = None
    try:
        import tkinter as tk
        from tkinter import messagebox

        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(title, message, parent=root)
        return True
    except Exception:  # noqa: BLE001 - an error reporter must never mask startup
        return False
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:  # noqa: BLE001, S110 - best-effort cleanup
                pass


def _applescript_string(text: str) -> str:
    """Escape a Python string for use inside an AppleScript string literal.

    Newlines have to become ``\\n`` escapes: a raw newline inside the quoted
    literal makes ``osascript`` refuse to compile the script, which is exactly
    the shape a multi-line dependency error takes.
    """
    return (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r\n", "\\n")
        .replace("\r", "\\n")
        .replace("\n", "\\n")
    )


def _native_dialog(title: str, message: str) -> bool:
    """Fall back to a small native dialog when Tk itself is unavailable."""
    try:
        if os.name == "nt":
            import ctypes

            ctypes.windll.user32.MessageBoxW(None, message, title, 0x10)
            return True

        if sys.platform == "darwin" and Path("/usr/bin/osascript").is_file():
            escaped_title = _applescript_string(title)
            escaped_message = _applescript_string(message)
            script = (
                f'display alert "{escaped_title}" message "{escaped_message}" '
                "as critical"
            )
            completed = subprocess.run(
                ["/usr/bin/osascript", "-e", script],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return completed.returncode == 0

        for command in ("zenity", "kdialog"):
            executable = shutil.which(command)
            if executable is None:
                continue
            args = (
                [
                    executable,
                    "--error",
                    f"--title={title}",
                    "--width=520",
                    f"--text={message}",
                ]
                if command == "zenity"
                else [executable, "--error", message, "--title", title]
            )
            subprocess.run(
                args,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
    except Exception:  # noqa: BLE001, S110 - stderr remains available below
        pass
    return False


def show_dependency_error(error: ImportError) -> bool:
    """Report a startup failure in a dialog, with stderr as a final fallback."""
    title = f"{APP_NAME} cannot start"
    message = dependency_error_message(error)
    print(f"{title}\n\n{message}", file=sys.stderr)
    return _tk_dialog(title, message) or _native_dialog(title, message)
