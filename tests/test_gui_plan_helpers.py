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
            gui.import_button_label(
                plan_payload(), leave_on_card=False, preview=True
            ),
            "Preview without moving anything",
        )


if __name__ == "__main__":
    unittest.main()
