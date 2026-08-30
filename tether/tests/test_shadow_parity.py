"""L2.4 shadow parity: the plugin's claim boundary versus the legacy router.

For one canonical set of ingress cases (derived from the frozen L0 incident
corpus's ingress incidents: authorization, peer bots, multi-workspace,
edits, bound and unbound threads), the same event is decided by BOTH:

- the legacy pure router (`runtime/routing.py::decide_route`), configured
  exactly as the deployed system binds Tether threads (ambient-owned native
  bindings), and
- the shadow plugin's admission (`runtime/plugin_next/admission.py`).

Hard invariants (any violation fails the suite):
- NO OVER-CLAIM: the shadow never admits an event the legacy router does
  not route NATIVE.
- NO UNDER-CLAIM: every event the legacy router routes NATIVE is admitted.

Every non-identical (action, verdict) pair must map through the explicit
equivalence table below or carry an allowlisted explanation; unexplained
differences fail. The comparison record is emitted as normalized JSON so a
reviewer can diff the full matrix.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"
PLUGIN_ROOT = RUNTIME / "plugin_next"

WORKSPACE = "T12345678"
OWNER = "U12345678"
BOT_SELF = "UBOT00001"
PEER_BOT = "UPEER0001"
CHANNEL = "C1"
THREAD = "100.1"


def load_admission():
    spec = importlib.util.spec_from_file_location(
        "tether_shadow_parity_plugin",
        PLUGIN_ROOT / "__init__.py",
        submodule_search_locations=[str(PLUGIN_ROOT)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["tether_shadow_parity_plugin"] = module
    spec.loader.exec_module(module)
    return module.admission


def load_routing():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        sys.modules.pop("routing", None)
        import routing
        return routing
    finally:
        sys.path[:] = previous


# Equivalence table: (legacy action, shadow verdict) pairs that are the same
# behavior expressed at two layers. `not_ours` means the plugin passes the
# event untouched to normal Hermes dispatch — which IS the legacy hermes or
# silent handling — so the composed system behaves identically.
EQUIVALENT = {
    ("native", "admit"),
    ("hermes", "not_ours"),
    ("silent", "not_ours"),
    ("silent", "deny"),
}

# Allowlisted explained differences. Absence of a key here for a differing
# case fails the test.
EXPLANATIONS = {
    "edits_invisible_until_upstream_ask_2": (
        "Slack message_changed/message_deleted never reach "
        "pre_gateway_dispatch on current Hermes (upstream proposal 2 adds "
        "the fire-sites). The legacy router silences edits; the shadow "
        "never sees them. Net admitted-turn behavior is identical: none."
    ),
}


class ShadowParityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admission = load_admission()
        cls.routing = load_routing()

    # -- legacy side -----------------------------------------------------

    def legacy_decide(self, case):
        r = self.routing
        mentioned_bots = frozenset(case.get("mentioned_bots", set()))
        message = r.NormalizedMessage(
            identity=r.MessageIdentity(
                team_id=case["workspace"],
                channel_id=case["channel"],
                message_ts=case["message_ts"],
            ),
            actor=r.ActorIdentity(
                user_id=case["actor"],
                is_bot=case["is_bot"],
                bot_id=case.get("bot_id"),
            ),
            conversation_kind=r.ConversationKind(case.get("conversation", "channel")),
            observed_at=1000.0,
            thread_ts=case.get("thread"),
            event_kind=r.EventKind(case.get("event_kind", "message")),
            mentioned_user_ids=mentioned_bots,
            mentioned_bot_user_ids=mentioned_bots,
        )
        thread = None
        if case.get("bound"):
            thread = r.ThreadState(
                identity=r.ThreadIdentity(
                    team_id=WORKSPACE,
                    channel_id=case["channel"],
                    thread_ts=case["thread"],
                ),
                binding=r.ActiveBinding(
                    kind=r.BindingKind.NATIVE,
                    bridge_id="brg-parity",
                    writer_id="writer-native",
                    owner_user_id=OWNER,
                    ambient_owned=True,
                ),
            )
        policy = r.RoutingPolicy(
            self_bot_user_id=BOT_SELF,
            hermes_writer_id="writer-hermes",
            allowed_human_user_ids=frozenset({OWNER}),
        )
        decision = r.decide_route(message, thread, policy)
        return {"action": decision.action.value, "reason": decision.reason}

    # -- shadow side ------------------------------------------------------

    def shadow_decide(self, case):
        settings = self.admission.AdmissionSettings(
            workspace_id=WORKSPACE,
            allowed_users=frozenset({OWNER}),
            persona_id="primary",
            policy_generation=1,
        )
        if case.get("event_kind", "message") != "message":
            # These subtypes never reach pre_gateway_dispatch today.
            return {"verdict": "unobserved", "reason": "edit_invisible_to_hook"}
        decision = self.admission.evaluate(
            platform="slack",
            workspace=case["workspace"],
            channel=case["channel"],
            thread=case.get("thread"),
            actor=case["actor"],
            actor_is_bot=case["is_bot"],
            message_id=case["message_ts"],
            settings=settings,
            bound_threads={(CHANNEL, THREAD)} if case.get("bound") else set(),
        )
        return {"verdict": decision["verdict"], "reason": decision["reason"]}

    # -- the matrix -------------------------------------------------------

    CASES = [
        # id, case fields, expected mapping ("equivalent" or explanation key)
        ("owner-on-bound-thread", dict(
            workspace=WORKSPACE, channel=CHANNEL, thread=THREAD, bound=True,
            actor=OWNER, is_bot=False, message_ts="170.1",
        ), "equivalent"),
        ("unauthorized-human-on-bound-thread", dict(
            workspace=WORKSPACE, channel=CHANNEL, thread=THREAD, bound=True,
            actor="U_ATTACKER", is_bot=False, message_ts="170.2",
        ), "equivalent"),
        ("untrusted-bot-on-bound-thread", dict(
            workspace=WORKSPACE, channel=CHANNEL, thread=THREAD, bound=True,
            actor=PEER_BOT, is_bot=True, bot_id="B1", message_ts="170.3",
        ), "equivalent"),
        ("owner-on-unbound-thread", dict(
            workspace=WORKSPACE, channel=CHANNEL, thread="999.9", bound=False,
            actor=OWNER, is_bot=False, message_ts="170.4",
        ), "equivalent"),
        ("owner-dm-without-binding", dict(
            workspace=WORKSPACE, channel="D1", thread=None, bound=False,
            actor=OWNER, is_bot=False, message_ts="170.5", conversation="dm",
        ), "equivalent"),
        ("wrong-workspace-on-bound-thread", dict(
            workspace="T9999", channel=CHANNEL, thread=THREAD, bound=True,
            actor=OWNER, is_bot=False, message_ts="170.6",
        ), "equivalent"),
        ("edit-on-bound-thread", dict(
            workspace=WORKSPACE, channel=CHANNEL, thread=THREAD, bound=True,
            actor=OWNER, is_bot=False, message_ts="170.7", event_kind="edit",
        ), "edits_invisible_until_upstream_ask_2"),
    ]

    def test_shadow_claim_boundary_matches_legacy_native_set(self):
        records = []
        over_claims = []
        under_claims = []
        unexplained = []
        for case_id, case, expected in self.CASES:
            legacy = self.legacy_decide(case)
            shadow = self.shadow_decide(case)
            pair = (legacy["action"], shadow["verdict"])
            equivalent = pair in EQUIVALENT
            record = {
                "case": case_id,
                "legacy": legacy,
                "shadow": shadow,
                "mapping": "equivalent" if equivalent else expected,
            }
            records.append(record)
            if shadow["verdict"] == "admit" and legacy["action"] != "native":
                over_claims.append(record)
            if legacy["action"] == "native" and shadow["verdict"] != "admit":
                under_claims.append(record)
            if not equivalent:
                if expected == "equivalent" or expected not in EXPLANATIONS:
                    unexplained.append(record)
                else:
                    self.assertEqual(record["mapping"], expected)
        rendered = json.dumps(records, indent=1, sort_keys=True)
        self.assertEqual(over_claims, [], f"shadow over-claims:\n{rendered}")
        self.assertEqual(under_claims, [], f"shadow under-claims:\n{rendered}")
        self.assertEqual(unexplained, [], f"unexplained differences:\n{rendered}")
        # The matrix must actually exercise both admit and deny paths.
        verdicts = {record["shadow"]["verdict"] for record in records}
        self.assertIn("admit", verdicts)
        self.assertIn("deny", verdicts)
        actions = {record["legacy"]["action"] for record in records}
        self.assertIn("native", actions)
        self.assertIn("silent", actions)

    def test_expected_pairings_per_case(self):
        expectations = {
            "owner-on-bound-thread": ("native", "admit"),
            "unauthorized-human-on-bound-thread": ("silent", "deny"),
            "untrusted-bot-on-bound-thread": ("silent", "deny"),
            "wrong-workspace-on-bound-thread": ("silent", "deny"),
            "owner-on-unbound-thread": ("silent", "not_ours"),
            "owner-dm-without-binding": ("hermes", "not_ours"),
        }
        for case_id, case, _expected in self.CASES:
            if case_id not in expectations:
                continue
            with self.subTest(case_id):
                legacy = self.legacy_decide(case)
                shadow = self.shadow_decide(case)
                want_action, want_verdict = expectations[case_id]
                self.assertEqual(legacy["action"], want_action, legacy)
                self.assertEqual(shadow["verdict"], want_verdict, shadow)


if __name__ == "__main__":
    unittest.main()
