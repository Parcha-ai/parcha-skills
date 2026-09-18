from __future__ import annotations

import hashlib
import unittest
from collections import defaultdict

from evals.boundary_identity import native_family_id, protected_family_ids


CLAUDE_SESSION = "01234567-89ab-cdef-0123-456789abcdef"
CODEX_PARENT = "codex-session-0123456789abcdef01234567"


class NativeFamilyIdentityTests(unittest.TestCase):
    def test_preserves_existing_namespaced_hashes(self):
        for kind, native_id in (
            ("claude-parent", CLAUDE_SESSION),
            ("codex-native", CODEX_PARENT),
        ):
            with self.subTest(kind=kind):
                expected = hashlib.sha256(f"{kind}|{native_id}".encode()).hexdigest()
                self.assertEqual(native_family_id(kind, native_id), expected)
                self.assertNotEqual(
                    native_family_id(kind, native_id),
                    hashlib.sha256(native_id.encode()).hexdigest(),
                )

    def test_cross_host_copies_use_the_same_native_identity(self):
        observations = [
            {"source_id": "claude:linux:host-a", "sessionId": CLAUDE_SESSION},
            {"source_id": "claude:linux:host-b", "sessionId": CLAUDE_SESSION},
        ]
        self.assertEqual(len({
            native_family_id("claude-parent", row["sessionId"])
            for row in observations
        }), 1)
        with self.assertRaises(TypeError):
            native_family_id("claude-parent", CLAUDE_SESSION, source_id="host-a")

    def test_bare_uuid_is_not_normalized_a_second_time(self):
        # Splitting "claude-parent|UUID" again formerly discarded the namespace.
        family = native_family_id("claude-parent", CLAUDE_SESSION)
        self.assertEqual(
            family, hashlib.sha256(f"claude-parent|{CLAUDE_SESSION}".encode()).hexdigest()
        )
        for ambiguous in (
            f"claude-parent|{CLAUDE_SESSION}",
            f"claude:linux:host|claude-parent|{CLAUDE_SESSION}",
            family,
        ):
            with self.subTest(value=ambiguous), self.assertRaises(ValueError):
                native_family_id("claude-parent", ambiguous)

    def test_invalid_kind_or_native_id_fails_closed(self):
        for kind in ("claude", "codex", "unknown", "", None, [], True):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                native_family_id(kind, CLAUDE_SESSION)
        for kind, valid in (
            ("claude-parent", CLAUDE_SESSION),
            ("codex-native", CODEX_PARENT),
        ):
            for value in (None, True, [], "", " ", "bad", valid + "\n", " " + valid,
                          valid.upper(), f"{kind}|{valid}", "a" * 64):
                with self.subTest(kind=kind, value=value), self.assertRaises(ValueError):
                    native_family_id(kind, value)
        with self.assertRaises(ValueError):
            native_family_id("claude-parent", CODEX_PARENT)
        with self.assertRaises(ValueError):
            native_family_id("codex-native", CLAUDE_SESSION)


class ProtectedFamilyMembershipTests(unittest.TestCase):
    def test_empty_defaultdict_entries_do_not_protect_a_family(self):
        protected = native_family_id("claude-parent", CLAUDE_SESSION)
        looked_up = native_family_id("codex-native", CODEX_PARENT)
        memberships = defaultdict(set, {protected: {"test"}})
        self.assertEqual(memberships[looked_up], set())
        before = dict(memberships)
        self.assertEqual(protected_family_ids(memberships), frozenset({protected}))
        self.assertEqual(dict(memberships), before)

    def test_only_explicit_optimize_or_test_membership_protects(self):
        ids = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(5)]
        memberships = {
            ids[0]: ["optimize"], ids[1]: ("test",),
            ids[2]: {"validation", "test"}, ids[3]: frozenset({"validation"}),
            ids[4]: [],
        }
        self.assertEqual(protected_family_ids(memberships), frozenset(ids[:3]))
        self.assertEqual(protected_family_ids({}), frozenset())

    def test_invalid_membership_fails_instead_of_admitting_unknown_family(self):
        family = native_family_id("claude-parent", CLAUDE_SESSION)
        for memberships in (
            None, [], {family: None}, {family: "test"}, {family: {"unknown"}},
            {family: ["test", "unknown"]}, {family: [None]}, {family: [True]},
            {family: [[]]}, {family: {"test": True}}, {"": ["test"]},
            {CLAUDE_SESSION: ["test"]}, {family.upper(): ["test"]},
        ):
            with self.subTest(value=memberships), self.assertRaises(ValueError):
                protected_family_ids(memberships)


if __name__ == "__main__":
    unittest.main()
