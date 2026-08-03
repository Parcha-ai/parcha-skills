from __future__ import annotations

import asyncio
import threading
import unittest

from runtime.slack_protocol import (
    CursorProtocolError,
    CursorState,
    MutationDisposition,
    MutationKind,
    RetryAfterCoordinator,
    SlackMethodKey,
    canonicalize_message_mutation,
    parse_retry_after,
    validate_cursor_page,
)


class ManualClock:
    def __init__(self, value: float = 0.0) -> None:
        self._value = value
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._value

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._value += seconds


class MutationProtocolTest(unittest.TestCase):
    def test_canonicalizes_official_message_changed_envelope(self):
        result = canonicalize_message_mutation(
            {
                "team_id": "T123",
                "event_id": "Ev123",
                "event": {
                    "type": "message",
                    "subtype": "message_changed",
                    "hidden": True,
                    "channel": "C123",
                    "ts": "1700000001.000002",
                    "event_ts": "1700000001.000002",
                    "message": {
                        "type": "message",
                        "user": "U123",
                        "text": "new instruction",
                        "blocks": [],
                        "ts": "1700000000.000001",
                        "thread_ts": "1699999999.000001",
                        "edited": {
                            "user": "U123",
                            "ts": "1700000001.000001",
                        },
                    },
                    "previous_message": {
                        "type": "message",
                        "user": "U123",
                        "text": "old instruction",
                        "blocks": [],
                        "ts": "1700000000.000001",
                        "thread_ts": "1699999999.000001",
                    },
                },
            }
        )

        self.assertIs(result.disposition, MutationDisposition.CANONICAL)
        mutation = result.mutation
        self.assertIsNotNone(mutation)
        self.assertIs(mutation.kind, MutationKind.EDIT)
        self.assertEqual(mutation.target_ts, "1700000000.000001")
        self.assertEqual(mutation.event_ts, "1700000001.000002")
        self.assertEqual(mutation.team_id, "T123")
        self.assertEqual(mutation.channel_id, "C123")
        self.assertEqual(mutation.thread_ts, "1699999999.000001")
        self.assertEqual(mutation.actor_user_id, "U123")
        self.assertEqual(mutation.replacement_text, "new instruction")
        self.assertEqual(mutation.event_id, "Ev123")

    def test_ignores_metadata_only_message_changed(self):
        previous = {
            "type": "message",
            "user": "U123",
            "text": "same text",
            "blocks": [{"type": "section", "block_id": "one"}],
            "attachments": [],
            "files": [],
            "ts": "1700000000.000001",
        }
        current = dict(previous)
        current["metadata"] = {
            "event_type": "tether_reply",
            "event_payload": {"bridge_id": "brg_123"},
        }
        current["edited"] = {"user": "U123", "ts": "1700000001.000001"}

        result = canonicalize_message_mutation(
            {
                "type": "message",
                "subtype": "message_changed",
                "channel": "C123",
                "message": current,
                "previous_message": previous,
            }
        )

        self.assertIs(result.disposition, MutationDisposition.IGNORE)
        self.assertEqual(result.reason, "metadata_only_edit")
        self.assertIsNone(result.mutation)

    def test_canonicalizes_minimal_message_deleted_without_inventing_context(self):
        result = canonicalize_message_mutation(
            {
                "type": "message",
                "subtype": "message_deleted",
                "deleted_ts": "1700000000.000001",
            }
        )

        self.assertIs(result.disposition, MutationDisposition.CANONICAL)
        mutation = result.mutation
        self.assertIsNotNone(mutation)
        self.assertIs(mutation.kind, MutationKind.DELETE)
        self.assertEqual(mutation.target_ts, "1700000000.000001")
        self.assertIsNone(mutation.event_ts)
        self.assertIsNone(mutation.channel_id)
        self.assertIsNone(mutation.thread_ts)
        self.assertIsNone(mutation.actor_user_id)
        self.assertIsNone(mutation.replacement_text)

    def test_canonicalizes_message_deleted_with_previous_message(self):
        result = canonicalize_message_mutation(
            {
                "team_id": "T123",
                "event_id": "EvDelete",
                "event": {
                    "type": "message",
                    "subtype": "message_deleted",
                    "channel": "C123",
                    "deleted_ts": "1700000000.000001",
                    "event_ts": "1700000002.000001",
                    "previous_message": {
                        "type": "message",
                        "user": "U123",
                        "text": "remove me",
                        "ts": "1700000000.000001",
                        "thread_ts": "1699999999.000001",
                    },
                },
            }
        )

        mutation = result.mutation
        self.assertIsNotNone(mutation)
        self.assertEqual(mutation.actor_user_id, "U123")
        self.assertEqual(mutation.thread_ts, "1699999999.000001")
        self.assertEqual(mutation.event_id, "EvDelete")

    def test_known_mutations_fail_closed_on_conflicts_or_bad_shapes(self):
        fixtures = (
            (
                {
                    "team_id": "T1",
                    "authorizations": [{"team_id": "T2"}],
                    "event": {
                        "subtype": "message_deleted",
                        "deleted_ts": "1.0",
                    },
                },
                "conflicting_team_id",
            ),
            (
                {
                    "subtype": "message_deleted",
                    "deleted_ts": "1.0",
                    "previous_message": {"ts": "2.0"},
                },
                "conflicting_deleted_ts",
            ),
            (
                {
                    "subtype": "message_changed",
                    "message": {"ts": "1.0", "user": "U1", "text": "new"},
                    "previous_message": {
                        "ts": "1.0",
                        "user": "U2",
                        "text": "old",
                    },
                },
                "conflicting_actor_user_id",
            ),
            (
                {
                    "subtype": "message_changed",
                    "message": {"ts": "1.0", "text": {"bad": True}},
                    "previous_message": {"ts": "1.0", "text": "old"},
                },
                "message_content_malformed",
            ),
            (
                {
                    "subtype": "message_changed",
                    "message": {"ts": "1.0", "text": "new"},
                },
                "previous_message_not_mapping",
            ),
        )
        for payload, reason in fixtures:
            with self.subTest(reason=reason):
                result = canonicalize_message_mutation(payload)
                self.assertIs(result.disposition, MutationDisposition.INVALID)
                self.assertEqual(result.reason, reason)

    def test_non_mutation_and_malformed_event_envelope_are_distinct(self):
        unrelated = canonicalize_message_mutation(
            {"type": "message", "subtype": "bot_message", "ts": "1.0"}
        )
        malformed = canonicalize_message_mutation({"event": "not-an-object"})

        self.assertIs(unrelated.disposition, MutationDisposition.NOT_MUTATION)
        self.assertIs(malformed.disposition, MutationDisposition.INVALID)


class RetryAfterProtocolTest(unittest.TestCase):
    def test_retry_after_parser_is_case_insensitive_bounded_and_fail_closed(self):
        self.assertEqual(parse_retry_after({"Retry-After": "12"}), 12.0)
        self.assertEqual(parse_retry_after({"retry-after": b"2.5"}), 2.5)
        self.assertEqual(
            parse_retry_after({"RETRY-AFTER": "9999"}, maximum=30), 30.0
        )
        for headers in (
            {},
            {"Retry-After": ""},
            {"Retry-After": "-1"},
            {"Retry-After": "NaN"},
            {"Retry-After": "Infinity"},
            {"Retry-After": "tomorrow"},
            {"Retry-After": ["1", "2"]},
            {"Retry-After": True},
        ):
            with self.subTest(headers=headers):
                self.assertIsNone(parse_retry_after(headers))

    def test_coordinator_is_scoped_by_workspace_and_method(self):
        clock = ManualClock(100.0)
        coordinator = RetryAfterCoordinator(
            clock=clock,
            sleep=clock.advance,
            fallback_delay=7,
        )
        key = SlackMethodKey("T1", "conversations.replies")
        other_method = SlackMethodKey("T1", "chat.postMessage")
        other_workspace = SlackMethodKey("T2", "conversations.replies")

        window = coordinator.record_429(key, {"Retry-After": "10"})

        self.assertTrue(window.header_valid)
        self.assertEqual(window.deadline, 110.0)
        self.assertEqual(coordinator.remaining(key), 10.0)
        self.assertEqual(coordinator.remaining(other_method), 0.0)
        self.assertEqual(coordinator.remaining(other_workspace), 0.0)
        coordinator.wait(key)
        self.assertEqual(clock(), 110.0)
        self.assertEqual(coordinator.remaining(key), 0.0)

    def test_method_key_rejects_ambiguous_whitespace(self):
        for workspace, method in (
            (" T1", "chat.postMessage"),
            ("T1", "chat.postMessage "),
        ):
            with self.subTest(workspace=workspace, method=method):
                with self.assertRaises(ValueError):
                    SlackMethodKey(workspace, method)

    def test_malformed_header_uses_bounded_fallback(self):
        clock = ManualClock(5.0)
        coordinator = RetryAfterCoordinator(
            clock=clock,
            sleep=clock.advance,
            fallback_delay=8,
            maximum_delay=10,
        )
        window = coordinator.record_429(
            SlackMethodKey("T1", "users.info"),
            {"Retry-After": "invalid"},
        )

        self.assertFalse(window.header_valid)
        self.assertEqual(window.requested_delay, 8.0)
        self.assertEqual(window.deadline, 13.0)

    def test_monotonic_high_water_ignores_backward_clock_movement(self):
        clock = ManualClock(100.0)
        coordinator = RetryAfterCoordinator(clock=clock, sleep=clock.advance)
        key = SlackMethodKey("T1", "conversations.replies")
        coordinator.record_429(key, {"Retry-After": "10"})

        clock.set(90.0)
        self.assertEqual(coordinator.remaining(key), 10.0)
        clock.set(111.0)
        self.assertEqual(coordinator.remaining(key), 0.0)
        clock.set(50.0)
        self.assertEqual(coordinator.remaining(key), 0.0)

    def test_concurrent_sync_waiters_sleep_without_holding_coordinator_lock(self):
        clock = ManualClock(0.0)
        entered = threading.Barrier(3)
        release = threading.Event()
        failures: list[BaseException] = []

        def blocking_sleep(_delay: float) -> None:
            entered.wait(timeout=2)
            release.wait(timeout=2)

        coordinator = RetryAfterCoordinator(clock=clock, sleep=blocking_sleep)
        key = SlackMethodKey("T1", "conversations.replies")
        coordinator.record_429(key, {"Retry-After": "10"})

        def waiter() -> None:
            try:
                coordinator.wait(key)
            except BaseException as exc:  # pragma: no cover - assertion reports it
                failures.append(exc)

        threads = [threading.Thread(target=waiter) for _ in range(2)]
        for thread in threads:
            thread.start()
        entered.wait(timeout=2)

        other = SlackMethodKey("T1", "chat.postMessage")
        window = coordinator.record_429(other, {"Retry-After": "1"})
        self.assertEqual(window.deadline, 1.0)
        clock.set(10.0)
        release.set()
        for thread in threads:
            thread.join(timeout=2)

        self.assertFalse(failures)
        self.assertTrue(all(not thread.is_alive() for thread in threads))

    def test_sync_wait_can_be_cancelled_without_consuming_full_retry_after(self):
        clock = ManualClock(0.0)
        stop = threading.Event()
        calls: list[float] = []

        def cancelling_sleep(delay: float) -> None:
            calls.append(delay)
            clock.advance(delay)
            stop.set()

        coordinator = RetryAfterCoordinator(
            clock=clock,
            sleep=cancelling_sleep,
        )
        key = SlackMethodKey("T1", "conversations.replies")
        coordinator.record_429(key, {"Retry-After": "3600"})

        self.assertFalse(coordinator.wait(key, stop_event=stop))
        self.assertEqual(calls, [0.25])
        self.assertGreater(coordinator.remaining(key), 3599)

    def test_async_waiters_use_injected_async_sleep(self):
        clock = ManualClock(20.0)
        calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            calls.append(delay)
            clock.advance(delay)
            await asyncio.sleep(0)

        coordinator = RetryAfterCoordinator(
            clock=clock,
            sleep=clock.advance,
            async_sleep=fake_sleep,
        )
        key = SlackMethodKey("T1", "conversations.replies")
        coordinator.record_429(key, {"Retry-After": "3"})

        async def exercise() -> None:
            await asyncio.gather(
                coordinator.wait_async(key),
                coordinator.wait_async(key),
            )

        asyncio.run(exercise())

        self.assertGreaterEqual(len(calls), 1)
        self.assertEqual(coordinator.remaining(key), 0.0)


class CursorProtocolTest(unittest.TestCase):
    def test_valid_pages_return_durable_next_cursor_and_terminal_state(self):
        first = validate_cursor_page(
            {
                "ok": True,
                "messages": [{"ts": "1.0"}],
                "response_metadata": {"next_cursor": "cursor-A"},
            },
            max_pages=3,
        )
        self.assertEqual(first.page.request_cursor, None)
        self.assertEqual(first.page.next_cursor, "cursor-A")
        self.assertEqual(first.page.page_number, 1)
        self.assertFalse(first.state.complete)

        second = validate_cursor_page(
            {
                "ok": True,
                "messages": [{"ts": "2.0"}],
                "response_metadata": {"next_cursor": ""},
            },
            first.state,
            max_pages=3,
        )
        self.assertEqual(second.page.request_cursor, "cursor-A")
        self.assertIsNone(second.state.next_cursor)
        self.assertTrue(second.state.complete)
        self.assertEqual(second.state.pages_seen, 2)

    def test_repeated_cursor_fails_closed_and_preserves_cursor(self):
        state = CursorState(
            next_cursor="cursor-A",
            seen_cursors=("cursor-A",),
            pages_seen=1,
        )

        with self.assertRaises(CursorProtocolError) as raised:
            validate_cursor_page(
                {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {"next_cursor": "cursor-A"},
                },
                state,
            )

        self.assertEqual(raised.exception.code, "repeated_cursor")
        self.assertEqual(raised.exception.state.next_cursor, "cursor-A")
        self.assertEqual(raised.exception.state.pages_seen, 2)

    def test_max_page_exhaustion_preserves_unconsumed_next_cursor(self):
        state = CursorState(
            next_cursor="cursor-A",
            seen_cursors=("cursor-A",),
            pages_seen=1,
        )

        with self.assertRaises(CursorProtocolError) as raised:
            validate_cursor_page(
                {
                    "ok": True,
                    "messages": [{"ts": "2.0"}],
                    "response_metadata": {"next_cursor": "cursor-B"},
                },
                state,
                max_pages=2,
            )

        self.assertEqual(raised.exception.code, "max_pages_exhausted")
        self.assertEqual(raised.exception.state.next_cursor, "cursor-B")
        self.assertEqual(
            raised.exception.state.seen_cursors,
            ("cursor-A", "cursor-B"),
        )

    def test_malformed_pages_fail_closed_without_advancing_state(self):
        state = CursorState(
            next_cursor="cursor-A",
            seen_cursors=("cursor-A",),
            pages_seen=1,
        )
        fixtures = (
            (None, "response_not_mapping"),
            ({"ok": False, "messages": []}, "response_not_ok"),
            ({"ok": True}, "items_not_list"),
            ({"ok": True, "messages": {}}, "items_not_list"),
            ({"ok": True, "messages": ["bad"]}, "item_not_mapping"),
            (
                {
                    "ok": True,
                    "messages": [],
                    "response_metadata": [],
                },
                "metadata_not_mapping",
            ),
            (
                {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {"next_cursor": 4},
                },
                "cursor_not_string",
            ),
            (
                {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {"next_cursor": "   "},
                },
                "cursor_whitespace",
            ),
        )
        for response, code in fixtures:
            with self.subTest(code=code):
                with self.assertRaises(CursorProtocolError) as raised:
                    validate_cursor_page(response, state)
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(raised.exception.state, state)

    def test_complete_sequence_rejects_additional_pages(self):
        state = CursorState(pages_seen=1, complete=True)

        with self.assertRaises(CursorProtocolError) as raised:
            validate_cursor_page({"ok": True, "messages": []}, state)

        self.assertEqual(raised.exception.code, "already_complete")

    def test_restored_state_cannot_exceed_page_budget(self):
        state = CursorState(
            next_cursor="cursor-B",
            seen_cursors=("cursor-A", "cursor-B"),
            pages_seen=2,
        )

        with self.assertRaises(CursorProtocolError) as raised:
            validate_cursor_page(
                {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {"next_cursor": ""},
                },
                state,
                max_pages=2,
            )

        self.assertEqual(raised.exception.code, "max_pages_exhausted")
        self.assertEqual(raised.exception.state, state)

    def test_cursor_state_rejects_incoherent_persisted_values(self):
        invalid_states = (
            {"next_cursor": "cursor-A", "seen_cursors": ()},
            {"seen_cursors": ["cursor-A"]},
            {"pages_seen": True},
            {"complete": 1},
            {"seen_cursors": (" cursor-A",)},
        )
        for values in invalid_states:
            with self.subTest(values=values):
                with self.assertRaises((TypeError, ValueError)):
                    CursorState(**values)


if __name__ == "__main__":
    unittest.main()
