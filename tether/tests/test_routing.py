from __future__ import annotations

import pathlib
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime.routing import (  # noqa: E402
    ActiveBinding,
    ActorIdentity,
    BindingKind,
    ConversationKind,
    EventKind,
    MessageIdentity,
    NormalizedMessage,
    ParticipationLease,
    RouteAction,
    RoutingPolicy,
    ThreadIdentity,
    ThreadState,
    decide_route,
)


TEAM = "T12345678"
CHANNEL = "C12345678"
THREAD = "1785000000.000001"
HUMAN = "UHUMAN001"
OTHER_HUMAN = "UHUMAN002"
BOT_A = "UBOTAAAA1"
BOT_B = "UBOTBBBB2"
BOT_C = "UBOTCCCC3"
BOT_A_APP = "BBOTAAAA1"
PEER_APP = "BPEER001"
NOW = 1_785_000_000.0


class RoutingDecisionTest(unittest.TestCase):
    def policy(
        self,
        bot_user_id: str = BOT_A,
        *,
        bot_id: str = BOT_A_APP,
        trusted_users=frozenset({BOT_B, BOT_C}),
        trusted_bot_ids=frozenset({PEER_APP}),
        ambient_bot_channels=frozenset(),
        allowed_channels=frozenset(),
    ) -> RoutingPolicy:
        return RoutingPolicy(
            self_bot_user_id=bot_user_id,
            self_bot_id=bot_id,
            hermes_writer_id=f"hermes:{bot_user_id}",
            allowed_human_user_ids=frozenset({HUMAN}),
            trusted_peer_user_ids=frozenset(trusted_users),
            trusted_peer_bot_ids=frozenset(trusted_bot_ids),
            ambient_peer_bot_channels=frozenset(ambient_bot_channels),
            allowed_channel_ids=frozenset(allowed_channels),
        )

    def message(
        self,
        *,
        team=TEAM,
        channel=CHANNEL,
        ts="1785000001.000001",
        thread_ts=THREAD,
        actor=None,
        conversation_kind=ConversationKind.CHANNEL,
        bot_mentions=frozenset(),
        human_mentions=frozenset(),
        unresolved_mentions=frozenset(),
        event_kind=EventKind.MESSAGE,
        observed_at=NOW,
    ) -> NormalizedMessage:
        bot_mentions = frozenset(bot_mentions)
        human_mentions = frozenset(human_mentions)
        unresolved_mentions = frozenset(unresolved_mentions)
        return NormalizedMessage(
            identity=MessageIdentity(team, channel, ts),
            actor=actor or ActorIdentity(HUMAN),
            conversation_kind=conversation_kind,
            observed_at=observed_at,
            thread_ts=thread_ts,
            event_kind=event_kind,
            mentioned_user_ids=bot_mentions | human_mentions | unresolved_mentions,
            mentioned_bot_user_ids=bot_mentions,
            mentioned_human_user_ids=human_mentions,
            unresolved_mention_user_ids=unresolved_mentions,
        )

    def thread(
        self,
        *,
        team=TEAM,
        channel=CHANNEL,
        thread_ts=THREAD,
        binding=None,
        participation=None,
    ) -> ThreadState:
        return ThreadState(
            identity=ThreadIdentity(team, channel, thread_ts),
            binding=binding,
            participation=participation,
        )

    def lease(
        self,
        owner=BOT_A,
        *,
        expires_at=NOW + 300,
        competitors=frozenset(),
    ) -> ParticipationLease:
        return ParticipationLease(
            owner_bot_user_id=owner,
            writer_id=f"hermes:{owner}",
            expires_at=expires_at,
            competing_bot_user_ids=frozenset(competitors),
        )

    def test_human_mention_targets_only_named_bot(self):
        message = self.message(bot_mentions={BOT_A})
        a = decide_route(message, self.thread(), self.policy(BOT_A))
        b = decide_route(message, self.thread(), self.policy(BOT_B))
        self.assertEqual(a.action, RouteAction.HERMES)
        self.assertEqual(a.reason, "self_explicitly_targeted")
        self.assertEqual(b.action, RouteAction.SILENT)
        self.assertEqual(
            b.reason,
            "another_participant_explicitly_targeted",
        )

    def test_human_can_target_two_bots_without_waking_a_third(self):
        message = self.message(bot_mentions={BOT_A, BOT_B})
        decisions = {
            bot: decide_route(message, self.thread(), self.policy(bot))
            for bot in (BOT_A, BOT_B, BOT_C)
        }
        self.assertEqual(decisions[BOT_A].action, RouteAction.HERMES)
        self.assertEqual(decisions[BOT_B].action, RouteAction.HERMES)
        self.assertEqual(decisions[BOT_C].action, RouteAction.SILENT)

    def test_explicit_target_overrides_shared_thread_participation(self):
        message = self.message(bot_mentions={BOT_A})
        shared = self.thread(participation=self.lease(competitors={BOT_B}))
        self.assertEqual(
            decide_route(message, shared, self.policy(BOT_A)).action,
            RouteAction.HERMES,
        )
        self.assertEqual(
            decide_route(message, shared, self.policy(BOT_B)).action,
            RouteAction.SILENT,
        )

    def test_unmentioned_human_reply_routes_to_unique_participation_owner(self):
        decision = decide_route(
            self.message(),
            self.thread(participation=self.lease()),
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.HERMES)
        self.assertEqual(decision.reason, "unique_participation_lease")

    def test_unmentioned_human_reply_is_silent_in_ambiguous_multi_bot_thread(self):
        decision = decide_route(
            self.message(),
            self.thread(participation=self.lease(competitors={BOT_B})),
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.SILENT)
        self.assertEqual(decision.reason, "not_confidently_addressed")

    def test_expired_or_foreign_participation_lease_is_silent(self):
        cases = (
            self.lease(expires_at=NOW - 1),
            self.lease(owner=BOT_B),
        )
        for lease in cases:
            with self.subTest(lease=lease):
                decision = decide_route(
                    self.message(),
                    self.thread(participation=lease),
                    self.policy(),
                )
                self.assertEqual(decision.action, RouteAction.SILENT)

    def test_native_binding_is_the_only_writer_for_unmentioned_human_reply(self):
        binding = ActiveBinding(
            BindingKind.NATIVE,
            bridge_id="brg_native",
            writer_id="codex:session-1",
            ambient_owned=True,
        )
        decision = decide_route(
            self.message(),
            self.thread(binding=binding, participation=self.lease()),
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.NATIVE)
        self.assertEqual(decision.writer_id, "codex:session-1")
        self.assertEqual(decision.bridge_id, "brg_native")

    def test_unowned_active_binding_blocks_participation_fallback(self):
        binding = ActiveBinding(
            BindingKind.NATIVE,
            bridge_id="brg_other_bot",
            writer_id="codex:other",
            ambient_owned=False,
        )
        decision = decide_route(
            self.message(),
            self.thread(binding=binding, participation=self.lease()),
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.SILENT)
        self.assertEqual(decision.reason, "active_binding_not_owned")

    def test_headless_and_hermes_bindings_route_to_the_bound_hermes_writer(self):
        for kind in (BindingKind.HEADLESS, BindingKind.HERMES):
            with self.subTest(kind=kind):
                binding = ActiveBinding(
                    kind,
                    bridge_id=f"brg_{kind.value}",
                    writer_id=f"hermes:{kind.value}",
                    ambient_owned=True,
                )
                decision = decide_route(
                    self.message(),
                    self.thread(binding=binding),
                    self.policy(),
                )
                self.assertEqual(decision.action, RouteAction.HERMES)
                self.assertEqual(decision.writer_id, f"hermes:{kind.value}")

    def test_explicit_other_bot_target_silences_even_a_native_binding(self):
        binding = ActiveBinding(
            BindingKind.NATIVE,
            bridge_id="brg_native",
            writer_id="codex:session-1",
        )
        decision = decide_route(
            self.message(bot_mentions={BOT_B}),
            self.thread(binding=binding),
            self.policy(BOT_A),
        )
        self.assertEqual(decision.action, RouteAction.SILENT)
        self.assertEqual(
            decision.reason,
            "another_participant_explicitly_targeted",
        )

    def test_binding_owner_restriction_applies_to_humans_and_peer_bots(self):
        binding = ActiveBinding(
            BindingKind.NATIVE,
            bridge_id="brg_private",
            writer_id="codex:session-1",
            owner_user_id=HUMAN,
        )
        peer = self.message(
            actor=ActorIdentity(BOT_B, is_bot=True, bot_id=PEER_APP),
            bot_mentions={BOT_A},
        )
        unauthorized_human = self.message(actor=ActorIdentity(OTHER_HUMAN))
        peer_decision = decide_route(peer, self.thread(binding=binding), self.policy())
        human_decision = decide_route(
            unauthorized_human,
            self.thread(binding=binding),
            self.policy(),
        )
        self.assertEqual(peer_decision.reason, "binding_owner_mismatch")
        self.assertEqual(human_decision.reason, "human_not_authorized")

    def test_trusted_peer_bot_must_explicitly_target_local_bot(self):
        peer = ActorIdentity(BOT_B, is_bot=True, bot_id=PEER_APP)
        mentioned = decide_route(
            self.message(actor=peer, bot_mentions={BOT_A}),
            self.thread(),
            self.policy(),
        )
        chatter = decide_route(
            self.message(actor=peer),
            self.thread(participation=self.lease()),
            self.policy(),
        )
        self.assertEqual(mentioned.action, RouteAction.HERMES)
        self.assertEqual(chatter.action, RouteAction.SILENT)
        self.assertEqual(chatter.reason, "peer_bot_did_not_target_self")

    def test_trusted_peer_bot_on_ambient_owned_binding_needs_no_mention(self):
        # A thread bound in the schema-18 domain surfaces as an ambient-owned
        # Hermes binding; peers working that thread must reach the bound
        # session without mentioning the local bot.
        peer = ActorIdentity(BOT_B, is_bot=True, bot_id=PEER_APP)
        bound = ActiveBinding(
            kind=BindingKind.HERMES,
            bridge_id="domain:bnd_1",
            writer_id="domain:bnd_1",
            owner_user_id="*",
            active=True,
            binding_generation=1,
            ambient_owned=True,
            peer_addressable=True,
        )
        decision = decide_route(
            self.message(actor=peer),
            self.thread(binding=bound),
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.HERMES)
        self.assertEqual(decision.reason, "active_hermes_binding")
        stranger = ActorIdentity("U0STRANGER", is_bot=True, bot_id="B0STRANGER")
        denied = decide_route(self.message(actor=stranger), self.thread(binding=bound), self.policy())
        self.assertEqual((denied.action, denied.reason), (RouteAction.SILENT, "untrusted_peer_bot"))

    def test_exact_ambient_bot_channel_routes_without_a_mention(self):
        peer = ActorIdentity("", is_bot=True, bot_id=PEER_APP)
        decision = decide_route(
            self.message(actor=peer, thread_ts=None),
            None,
            self.policy(ambient_bot_channels={(PEER_APP, CHANNEL)}),
        )

        self.assertEqual(decision.action, RouteAction.HERMES)
        self.assertEqual(decision.reason, "trusted_ambient_peer_bot")

    def test_ambient_bot_route_is_bound_to_exact_identity_and_channel(self):
        policy = self.policy(ambient_bot_channels={(PEER_APP, CHANNEL)})
        cases = (
            self.message(
                channel="COTHER001",
                actor=ActorIdentity("", is_bot=True, bot_id=PEER_APP),
                thread_ts=None,
            ),
            self.message(
                actor=ActorIdentity("", is_bot=True, bot_id="BOTHER001"),
                thread_ts=None,
            ),
        )

        for message in cases:
            with self.subTest(message=message.identity):
                decision = decide_route(message, None, policy)
                self.assertEqual(decision.action, RouteAction.SILENT)

    def test_ambient_bot_cannot_redirect_to_another_mentioned_participant(self):
        peer = ActorIdentity("", is_bot=True, bot_id=PEER_APP)
        decision = decide_route(
            self.message(
                actor=peer,
                human_mentions={HUMAN},
                thread_ts=None,
            ),
            None,
            self.policy(ambient_bot_channels={(PEER_APP, CHANNEL)}),
        )

        self.assertEqual(decision.action, RouteAction.SILENT)
        self.assertEqual(decision.reason, "another_participant_explicitly_targeted")

    def test_trusted_bot_id_without_user_id_can_route_when_mentioned(self):
        message = self.message(
            actor=ActorIdentity("", is_bot=True, bot_id=PEER_APP),
            bot_mentions={BOT_A},
        )
        decision = decide_route(message, self.thread(), self.policy())
        self.assertEqual(decision.action, RouteAction.HERMES)

    def test_untrusted_or_self_bot_is_silent(self):
        cases = (
            (
                ActorIdentity("UUNKNOWN", is_bot=True, bot_id="BUNKNOWN"),
                "untrusted_peer_bot",
            ),
            (
                ActorIdentity(BOT_A, is_bot=True, bot_id=BOT_A_APP),
                "self_message",
            ),
        )
        for actor, reason in cases:
            with self.subTest(reason=reason):
                decision = decide_route(
                    self.message(actor=actor, bot_mentions={BOT_A}),
                    self.thread(),
                    self.policy(),
                )
                self.assertEqual(decision.action, RouteAction.SILENT)
                self.assertEqual(decision.reason, reason)

    def test_authorized_direct_message_is_mention_exempt(self):
        decision = decide_route(
            self.message(
                channel="D12345678",
                thread_ts=None,
                conversation_kind=ConversationKind.DM,
            ),
            None,
            self.policy(allowed_channels={"COTHER001"}),
        )
        self.assertEqual(decision.action, RouteAction.HERMES)
        self.assertEqual(decision.reason, "authorized_direct_message")

    def test_peer_bot_direct_message_is_not_mention_exempt(self):
        decision = decide_route(
            self.message(
                channel="D12345678",
                thread_ts=None,
                conversation_kind=ConversationKind.DM,
                actor=ActorIdentity(BOT_B, is_bot=True, bot_id=PEER_APP),
            ),
            None,
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.SILENT)
        self.assertEqual(decision.reason, "peer_bot_did_not_target_self")

    def test_mpim_is_a_shared_surface_not_a_direct_message(self):
        unmentioned = decide_route(
            self.message(
                channel="G12345678",
                thread_ts=None,
                conversation_kind=ConversationKind.MPIM,
            ),
            None,
            self.policy(),
        )
        mentioned = decide_route(
            self.message(
                channel="G12345678",
                thread_ts=None,
                conversation_kind=ConversationKind.MPIM,
                bot_mentions={BOT_A},
            ),
            None,
            self.policy(),
        )
        self.assertEqual(unmentioned.action, RouteAction.SILENT)
        self.assertEqual(mentioned.action, RouteAction.HERMES)

    def test_allowed_channel_policy_applies_to_channels_and_mpims_not_dms(self):
        for kind, channel in (
            (ConversationKind.CHANNEL, CHANNEL),
            (ConversationKind.MPIM, "G12345678"),
        ):
            with self.subTest(kind=kind):
                decision = decide_route(
                    self.message(
                        channel=channel,
                        thread_ts=None,
                        conversation_kind=kind,
                        bot_mentions={BOT_A},
                    ),
                    None,
                    self.policy(allowed_channels={"CALLOWED1"}),
                )
                self.assertEqual(decision.reason, "conversation_not_allowed")

    def test_human_mention_silences_ambient_bot_ownership(self):
        decision = decide_route(
            self.message(human_mentions={OTHER_HUMAN}),
            self.thread(participation=self.lease()),
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.SILENT)
        self.assertEqual(
            decision.reason,
            "another_participant_explicitly_targeted",
        )

    def test_self_and_human_mentions_still_target_this_bot(self):
        decision = decide_route(
            self.message(
                bot_mentions={BOT_A},
                human_mentions={OTHER_HUMAN},
            ),
            self.thread(),
            self.policy(),
        )
        self.assertEqual(decision.action, RouteAction.HERMES)
        self.assertEqual(decision.reason, "self_explicitly_targeted")

    def test_unresolved_mention_fails_closed_unless_self_is_also_targeted(self):
        unresolved = decide_route(
            self.message(unresolved_mentions={"UUNKNOWN"}),
            self.thread(participation=self.lease()),
            self.policy(),
        )
        self_targeted = decide_route(
            self.message(
                bot_mentions={BOT_A},
                unresolved_mentions={"UUNKNOWN"},
            ),
            self.thread(),
            self.policy(),
        )
        self.assertEqual(unresolved.action, RouteAction.SILENT)
        self.assertEqual(unresolved.reason, "mention_resolution_incomplete")
        self.assertEqual(self_targeted.action, RouteAction.HERMES)

    def test_thread_state_must_match_workspace_channel_and_thread(self):
        mismatches = (
            self.thread(team="TOTHER001"),
            self.thread(channel="COTHER001"),
            self.thread(thread_ts="1785000000.999999"),
        )
        for thread in mismatches:
            with self.subTest(identity=thread.identity):
                decision = decide_route(
                    self.message(bot_mentions={BOT_A}),
                    thread,
                    self.policy(),
                )
                self.assertEqual(decision.action, RouteAction.SILENT)
                self.assertEqual(decision.reason, "thread_identity_mismatch")

    def test_composite_identity_distinguishes_same_timestamp_across_scopes(self):
        messages = (
            self.message(team=TEAM, channel=CHANNEL, ts="111.222"),
            self.message(team=TEAM, channel="COTHER001", ts="111.222"),
            self.message(team="TOTHER001", channel=CHANNEL, ts="111.222"),
        )
        keys = {
            decide_route(message, None, self.policy()).dedupe_key
            for message in messages
        }
        self.assertEqual(len(keys), 3)

    def test_same_composite_identity_has_same_dedupe_key(self):
        first = self.message(ts="111.222", bot_mentions={BOT_A})
        replay = self.message(ts="111.222", bot_mentions={BOT_A})
        self.assertEqual(
            decide_route(first, self.thread(), self.policy()).dedupe_key,
            decide_route(replay, self.thread(), self.policy()).dedupe_key,
        )

    def test_edit_and_delete_events_are_silent(self):
        for event_kind in (EventKind.EDIT, EventKind.DELETE):
            with self.subTest(event_kind=event_kind):
                decision = decide_route(
                    self.message(
                        event_kind=event_kind,
                        bot_mentions={BOT_A},
                    ),
                    self.thread(),
                    self.policy(),
                )
                self.assertEqual(decision.action, RouteAction.SILENT)
                self.assertEqual(decision.reason, "unsupported_event_kind")

    def test_normalization_rejects_missing_composite_identity(self):
        for values in (
            ("", CHANNEL, "111.222"),
            (TEAM, "", "111.222"),
            (TEAM, CHANNEL, ""),
        ):
            with self.subTest(values=values), self.assertRaises(ValueError):
                MessageIdentity(*values)

    def test_normalization_requires_complete_disjoint_mention_classes(self):
        with self.assertRaises(ValueError):
            NormalizedMessage(
                identity=MessageIdentity(TEAM, CHANNEL, "111.222"),
                actor=ActorIdentity(HUMAN),
                conversation_kind=ConversationKind.CHANNEL,
                observed_at=NOW,
                mentioned_user_ids=frozenset({BOT_A}),
            )
        with self.assertRaises(ValueError):
            NormalizedMessage(
                identity=MessageIdentity(TEAM, CHANNEL, "111.223"),
                actor=ActorIdentity(HUMAN),
                conversation_kind=ConversationKind.CHANNEL,
                observed_at=NOW,
                mentioned_user_ids=frozenset({BOT_A}),
                mentioned_bot_user_ids=frozenset({BOT_A}),
                unresolved_mention_user_ids=frozenset({BOT_A}),
            )


if __name__ == "__main__":
    unittest.main()
