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


class OtherCardFilesTests(unittest.TestCase):
    def test_no_note_when_card_holds_only_media(self):
        self.assertEqual(gui.describe_other_card_files(plan_payload()), "")
        self.assertEqual(gui.describe_other_card_files(None), "")

    def test_trivial_only_is_a_gentle_ignore_message(self):
        note = gui.describe_other_card_files(
            plan_payload(other_files=0, other_files_trivial=4)
        )
        self.assertIn("safe to ignore", note)
        self.assertIn("format the card in the camera", note)

    def test_real_data_triggers_a_keep_warning(self):
        note = gui.describe_other_card_files(
            plan_payload(
                other_files=2,
                other_files_bytes=5_000_000,
                other_files_trivial=3,
                other_file_kinds={".TXT": 1, ".PDF": 1},
                other_file_examples=["notes.txt", "invoice.pdf"],
            )
        )
        self.assertIn("not photos or videos", note)
        self.assertIn("notes.txt", note)
        self.assertIn("Copy anything you want to keep", note)
        self.assertIn("format the card in the camera", note)

    def test_post_import_notice_carries_extra_and_tiny_file_warning(self):
        title, body = gui.post_import_card_notice(
            plan_payload(
                other_files=2,
                other_files_bytes=5_000_000,
                other_files_trivial=3,
                other_file_kinds={".TXT": 2},
            ),
            leave_on_card=True,
        )
        self.assertEqual(title, "Before you format the card")
        self.assertIn("not photos or videos", body)
        self.assertIn("3 tiny camera files are ignored", body)
        self.assertIn("format the card in the camera", body)

    def test_post_import_notice_always_includes_formatting_advice(self):
        title, body = gui.post_import_card_notice(
            plan_payload(), leave_on_card=True
        )
        self.assertEqual(title, "Before your next shoot")
        self.assertIn("format the card in the camera", body)


class PlanScanActivityTests(unittest.TestCase):
    def test_scan_activity_bar_starts_and_stops_with_scan(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window._plan_slow_after = None
        window._plan_scanning = False
        window._running = False
        window.source_scan_progress = mock.Mock()
        window.source_scan_var = mock.Mock()
        window.start_btn = mock.Mock()

        gui.StackcopyGUI._set_plan_scanning(window, True)
        window.source_scan_progress.grid.assert_called_once_with()
        window.source_scan_progress.start.assert_called_once_with()

        gui.StackcopyGUI._set_plan_scanning(window, False)
        window.source_scan_progress.stop.assert_called_once_with()
        window.source_scan_progress.grid_remove.assert_called_once_with()

    def test_repeated_scanning_calls_do_not_restart_activity_bar(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window._plan_slow_after = None
        window._plan_scanning = False
        window._running = False
        window.source_scan_progress = mock.Mock()
        window.source_scan_var = mock.Mock()
        window.start_btn = mock.Mock()

        gui.StackcopyGUI._set_plan_scanning(window, True)
        gui.StackcopyGUI._set_plan_scanning(window, True)
        gui.StackcopyGUI._set_plan_scanning(window, True)

        window.source_scan_progress.start.assert_called_once_with()


class ResultCardNoticeTests(unittest.TestCase):
    @staticmethod
    def result_window(plan):
        window = mock.Mock(spec=gui.StackcopyGUI)
        for name in (
            "activity",
            "running_controls",
            "result_controls",
            "open_btn",
            "phase_var",
            "meta_var",
            "result_body_var",
            "result_body_label",
            "progress",
            "current_file_label",
            "card_followup_note",
            "card_followup_title_var",
            "card_followup_body_var",
            "card_followup_body_label",
        ):
            setattr(window, name, mock.Mock())
        window._plan = plan
        window.mode_var = mock.Mock()
        window.mode_var.get.return_value = gui.COPY_MODE
        return window

    def test_card_warning_is_shown_after_success(self):
        window = self.result_window(
            plan_payload(other_files=1, other_files_bytes=100, other_files_trivial=2)
        )

        gui.StackcopyGUI._show_result(
            window,
            "1 file imported",
            "done",
            problems=0,
            allow_open=True,
            success=True,
        )

        window.card_followup_note.grid.assert_called_once_with()
        body = window.card_followup_body_var.set.call_args.args[0]
        self.assertIn("not photos or videos", body)
        self.assertIn("2 tiny camera files are ignored", body)

    def test_card_warning_is_hidden_when_import_did_not_succeed(self):
        window = self.result_window(
            plan_payload(other_files=1, other_files_bytes=100, other_files_trivial=2)
        )

        gui.StackcopyGUI._show_result(
            window,
            "Import did not finish",
            "failed",
            problems=1,
            allow_open=False,
        )

        window.card_followup_note.grid.assert_not_called()
        window.card_followup_note.grid_remove.assert_called_once_with()


class ConditionalScrollbarTests(unittest.TestCase):
    def test_scrollbar_is_hidden_when_content_fits_and_restored_when_needed(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window._body_scrollbar_visible = None
        window.body = mock.Mock()
        window.body._parent_canvas = mock.Mock()
        window.body._scrollbar = mock.Mock()
        window.body._parent_canvas.winfo_height.return_value = 700
        window.body.winfo_reqheight.return_value = 650

        gui.StackcopyGUI._update_body_scrollbar(window)
        window.body._scrollbar.grid_remove.assert_called_once_with()

        window.body.winfo_reqheight.return_value = 750
        gui.StackcopyGUI._update_body_scrollbar(window)
        window.body._scrollbar.grid.assert_called_once_with()


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
        window._plan = plan_payload(
            total=5, stacked_outputs=1, stack_inputs=2, others=2
        )
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


class ImportAnotherTests(unittest.TestCase):
    def test_keeps_remembered_source_and_rescans_it(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window.activity = mock.Mock()
        window.actions = mock.Mock()
        window.src_var = mock.Mock()
        window._plan = plan_payload()
        window._plan_generation = 4

        gui.StackcopyGUI._import_another(window)

        window.src_var.set.assert_not_called()
        self.assertIsNone(window._plan)
        self.assertEqual(window._plan_generation, 5)
        window._refresh_idle_plan.assert_called_once_with()
        window._schedule_plan_scan.assert_called_once_with()


class TerminalSummaryTests(unittest.TestCase):
    def test_success_metrics_include_time_bytes_rate_and_failure_state(self):
        self.assertEqual(
            gui.success_metrics(20.0, 10 * 1024 * 1024),
            "20.0 seconds · 10.0 MB · 512.0 KB/s · nothing failed",
        )

    def test_success_metrics_pluralize_reported_problems(self):
        self.assertEqual(gui.success_metrics(1.0, 0, 1), "1.0 seconds · 1 problem")
        self.assertEqual(gui.success_metrics(1.0, 0, 3), "1.0 seconds · 3 problems")

    def test_cli_summary_uses_final_failure_and_import_counts(self):
        parsed = gui.parse_cli_summary(
            "Files safely placed: 348\n  Failures: 0\n"
            "Done. Imported 348 files in 18.4 seconds\n"
        )
        self.assertEqual(parsed, {"problems": 0, "imported": 348})


if __name__ == "__main__":
    unittest.main()
