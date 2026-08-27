"""Startup behavior when the optional GUI dependencies are unavailable."""

import os
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy_gui_bootstrap as bootstrap


class DependencyMessageTests(unittest.TestCase):
    def test_missing_gui_package_names_requirements_file_and_install_command(self):
        error = ModuleNotFoundError("No module named 'customtkinter'", name="customtkinter")

        message = bootstrap.dependency_error_message(error)

        self.assertIn("Missing module: customtkinter", message)
        self.assertIn("requirements-gui.txt", message)
        self.assertIn(f'"{sys.executable}" -m pip install -r', message)

    def test_missing_tk_gets_system_package_instructions(self):
        error = ModuleNotFoundError("No module named 'tkinter'", name="tkinter")
        with mock.patch.object(bootstrap.sys, "platform", "linux"), mock.patch.object(
            bootstrap.os, "name", "posix"
        ):
            message = bootstrap.dependency_error_message(error)

        self.assertIn("Python's Tk GUI support", message)
        self.assertIn("sudo apt install python3-tk", message)
        self.assertIn("sudo dnf install python3-tkinter", message)

    def test_missing_low_level_tk_binding_gets_tk_instructions(self):
        error = ModuleNotFoundError("No module named '_tkinter'", name="_tkinter")
        with mock.patch.object(bootstrap.sys, "platform", "linux"), mock.patch.object(
            bootstrap.os, "name", "posix"
        ):
            message = bootstrap.dependency_error_message(error)

        self.assertIn("Python's Tk GUI support", message)
        self.assertIn("sudo apt install python3-tk", message)
        self.assertNotIn("requirements-gui.txt", message)

    def test_frozen_tk_failure_gets_packaged_runtime_guidance(self):
        error = ModuleNotFoundError("No module named 'tkinter'", name="tkinter")
        with mock.patch.object(bootstrap.sys, "frozen", True, create=True):
            message = bootstrap.dependency_error_message(error)

        self.assertIn("packaged GUI libraries", message)
        self.assertIn("Reinstall Stackcopy", message)
        self.assertNotIn("python3-tk", message)
        self.assertNotIn("requirements-gui.txt", message)


class DependencyDialogTests(unittest.TestCase):
    def test_standard_tk_dialog_is_used_without_customtkinter(self):
        calls = []

        class Root:
            def withdraw(self):
                calls.append("withdraw")

            def destroy(self):
                calls.append("destroy")

        tkinter = types.ModuleType("tkinter")
        tkinter.Tk = Root
        messagebox = types.ModuleType("tkinter.messagebox")
        messagebox.showerror = lambda title, message, parent: calls.append(
            (title, message, parent)
        )
        tkinter.messagebox = messagebox

        with mock.patch.dict(
            sys.modules,
            {"tkinter": tkinter, "tkinter.messagebox": messagebox},
        ):
            shown = bootstrap.show_dependency_error(
                ModuleNotFoundError(
                    "No module named 'customtkinter'", name="customtkinter"
                )
            )

        self.assertTrue(shown)
        self.assertEqual(calls[0], "withdraw")
        self.assertEqual(calls[-1], "destroy")
        self.assertEqual(calls[1][0], "Stackcopy cannot start")
        self.assertIn("requirements-gui.txt", calls[1][1])

    def test_launcher_exits_cleanly_after_reporting_missing_dependency(self):
        reporter = mock.Mock(return_value=True)
        fake_bootstrap = types.ModuleType("stackcopy_gui_bootstrap")
        fake_bootstrap.show_dependency_error = reporter

        with mock.patch.dict(
            sys.modules,
            {"customtkinter": None, "stackcopy_gui_bootstrap": fake_bootstrap},
        ), mock.patch.dict(
            os.environ, {"STACKCOPY_RUN_CLI": "0"}
        ), self.assertRaises(SystemExit) as stopped:
            runpy.run_path(str(ROOT / "stackcopy_gui.py"), run_name="__main__")

        self.assertEqual(stopped.exception.code, 1)
        reporter.assert_called_once()
        self.assertIsInstance(reporter.call_args.args[0], ImportError)


if __name__ == "__main__":
    unittest.main()
