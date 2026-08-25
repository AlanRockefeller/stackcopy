"""Display-free helper coverage for the explanatory GUI."""

import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load_gui_module():
    try:
        import stackcopy_gui

        return stackcopy_gui
    except ImportError:
        pass

    class AnyModule(types.ModuleType):
        def __getattr__(self, name):
            if name.startswith("__"):
                raise AttributeError(name)
            placeholder = type(name, (object,), {})
            setattr(self, name, placeholder)
            return placeholder

    stubs = {
        name: AnyModule(name)
        for name in (
            "customtkinter",
            "tkinter",
            "tkinter.filedialog",
            "tkinter.messagebox",
        )
        if name not in sys.modules
    }
    with mock.patch.dict(sys.modules, stubs):
        import stackcopy_gui

        return stackcopy_gui


gui = load_gui_module()


def plan_payload(**changes):
    payload = {
        "total": 348,
        "bytes": 8_600_000_000,
        "stacks": 12,
        "stacked_outputs": 12,
        "stack_inputs": 96,
        "others": 240,
        "newest_date": "2026-08-25",
        "dest_lightroom": r"Pictures\Lightroom\2026\2026-08-25",
        "dest_stack_input": r"Pictures\olympus.stack.input.photos\2026\2026-08-25",
        "source_subdirs_scanned": [r"DCIM\100OMSYS", r"DCIM\101OMSYS"],
        "source_is_removable": True,
        "source_would_be_empty_after": True,
    }
    payload.update(changes)
    return payload


class PlanParserTests(unittest.TestCase):
    def test_valid_payload_is_normalized(self):
        parsed = gui.parse_plan_json(json.dumps(plan_payload()))
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["total"], 348)
        self.assertEqual(parsed["others"], 240)

    def test_nested_others_total_is_accepted_for_compatibility(self):
        parsed = gui.parse_plan_json(
            json.dumps(plan_payload(others={"total": 240, "photos": 220, "videos": 20}))
        )
        self.assertEqual(parsed["others"], 240)

    def test_malformed_or_inconsistent_payload_degrades_to_no_plan(self):
        self.assertIsNone(gui.parse_plan_json("not json"))
        self.assertIsNone(gui.parse_plan_json(json.dumps(plan_payload(total=349))))
        self.assertIsNone(gui.parse_plan_json(json.dumps(plan_payload(bytes=-1))))


class ProgressParserTests(unittest.TestCase):
    def test_role_and_stack_output_name_survive_spaces(self):
        fields, filename = gui.parse_progress(
            "@@SCPROGRESS phase=move done=31 total=348 role=stack_input "
            "stack_output_name=P8081918%20stacked.jpg file=P8081912.ORF\n"
        )
        self.assertEqual(fields["role"], "stack_input")
        self.assertEqual(fields["stack_output_name"], "P8081918 stacked.jpg")
        self.assertEqual(filename, "P8081912.ORF")

    def test_recorded_progress_sequence_counts_each_lines_own_role(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window._total = 0
        window._done = 0
        window._current_role = None
        window._bucket_done = {"stack_output": 0, "stack_input": 0, "other": 0}
        window._degraded = False
        window._plan = plan_payload(total=5, stacked_outputs=1, stack_inputs=2, others=2)
        window.progress = mock.Mock()
        window.phase_var = mock.Mock()
        window.current_file_var = mock.Mock()

        lines = [
            "@@SCPROGRESS phase=start done=0 total=5",
            "@@SCPROGRESS phase=move done=0 total=5 role=stack_output file=A.JPG",
            "@@SCPROGRESS phase=move done=1 total=5 role=stack_input file=A.ORF",
            "@@SCPROGRESS phase=move done=2 total=5 role=stack_input file=B.ORF",
            "@@SCPROGRESS phase=move done=3 total=5 role=other file=C.JPG",
            "@@SCPROGRESS phase=move done=4 total=5 role=other file=D.MOV",
            "@@SCPROGRESS phase=done done=5 total=5 degraded=0",
        ]
        for line in lines:
            gui.StackcopyGUI._handle_progress(window, line)

        self.assertEqual(
            window._bucket_done,
            {"stack_output": 1, "stack_input": 2, "other": 2},
        )
        self.assertEqual(window._done, 5)
        window.progress.set.assert_called_with(1.0)


class ButtonLabelTests(unittest.TestCase):
    def test_plan_count_and_mode_drive_primary_label(self):
        plan = plan_payload()
        self.assertEqual(
            gui.import_button_label(plan, leave_on_card=False), "Move 348 files"
        )
        self.assertEqual(
            gui.import_button_label(plan, leave_on_card=True), "Copy 348 files"
        )

    def test_no_plan_uses_generic_label(self):
        self.assertEqual(
            gui.import_button_label(None, leave_on_card=False), "Start import"
        )

    def test_preview_label_is_explicitly_non_destructive(self):
        self.assertEqual(
            gui.import_button_label(plan_payload(), leave_on_card=False, preview=True),
            "Preview without moving anything",
        )


class ModeChangeTests(unittest.TestCase):
    def test_mode_change_relabels_cached_plan_without_scheduling_a_scan(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window._plan_generation = 7
        window._plan = plan_payload()

        gui.StackcopyGUI._on_mode_changed(window, gui.COPY_MODE)

        window._sync_mode_help.assert_called_once_with()
        window._schedule_save.assert_called_once_with()
        window._refresh_idle_plan.assert_called_once_with()
        window._schedule_plan_scan.assert_not_called()
        self.assertEqual(window._plan_generation, 7)
        self.assertEqual(window._plan, plan_payload())

    def test_card_empty_expectation_is_derived_from_current_mode(self):
        plan = plan_payload()
        self.assertTrue(gui.source_will_be_empty(plan, leave_on_card=False))
        self.assertFalse(gui.source_will_be_empty(plan, leave_on_card=True))


class TerminalSummaryTests(unittest.TestCase):
    def test_success_metrics_include_time_bytes_rate_and_failure_state(self):
        self.assertEqual(
            gui.success_metrics(20.0, 10 * 1024 * 1024),
            "20.0 seconds · 10.0 MB · 512.0 KB/s · nothing failed",
        )

    def test_success_metrics_pluralize_reported_problems(self):
        self.assertEqual(
            gui.success_metrics(1.0, 0, 1), "1.0 seconds · 1 problem"
        )
        self.assertEqual(
            gui.success_metrics(1.0, 0, 3), "1.0 seconds · 3 problems"
        )

    def test_cli_summary_uses_final_failure_and_import_counts(self):
        parsed = gui.parse_cli_summary(
            "Files safely placed: 348\n  Failures: 0\n"
            "Done. Imported 348 files in 18.4 seconds\n"
        )
        self.assertEqual(parsed, {"problems": 0, "imported": 348})


if __name__ == "__main__":
    unittest.main()
