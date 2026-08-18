import hashlib
import importlib.util
import pathlib
import sqlite3
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "runtime" / "domain_schema.py"


def load_schema():
    name = "tether_domain_schema_test"
    spec = importlib.util.spec_from_file_location(name, SCHEMA_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class DomainSchemaTest(unittest.TestCase):
    def setUp(self):
        self.schema = load_schema()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.schema.install_schema(self.db)

    def tearDown(self):
        self.db.close()

    def endpoint(
        self,
        *,
        key="physical-session",
        domain=None,
        endpoint_kind="detached_native",
    ):
        descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=1000,
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        if domain is None:
            domain = descriptor.security_domain_id
        endpoint_id = "end_" + hashlib.sha256(key.encode()).hexdigest()[:24]
        self.db.execute(
            """
            INSERT INTO endpoints(
              endpoint_id,endpoint_key,endpoint_kind,source_kind,source_json,
              ref_version,incarnation,security_domain_id,instance_uid,
              workspace_id,persona_id,authorized_owners_json,
              authorized_owners_hash,
              policy_generation,state,next_lease_fence
            ) VALUES(?,?,?,'headless_run','{}',3,1,?,1000,
                     'T12345678','primary','["U12345678"]',?,1,'ready',1)
            """,
            (
                endpoint_id,
                key,
                endpoint_kind,
                domain,
                descriptor.authorized_owners_hash,
            ),
        )
        self.db.execute(
            """
            INSERT INTO endpoint_authorized_owners(
              endpoint_id,security_domain_id,owner_user_id
            ) VALUES(?,?,'U12345678')
            """,
            (endpoint_id, domain),
        )
        return endpoint_id

    def binding(
        self,
        endpoint_id,
        suffix,
        *,
        state="active",
        thread_ts=None,
        channel_id=None,
    ):
        binding_id = f"brg_{suffix:0<24}"[:28]
        thread_ts = thread_ts or f"1786000000.{suffix:0>6}"
        security_domain_id = self.db.execute(
            "SELECT security_domain_id FROM endpoints WHERE endpoint_id=?",
            (endpoint_id,),
        ).fetchone()[0]
        self.db.execute(
            """
            INSERT INTO thread_bindings(
              binding_id,endpoint_id,security_domain_id,team_id,channel_id,
              thread_ts,owner_user_id,idempotency_key,request_hash,
              generation,state
            ) VALUES(?,? ,?,'T12345678',?,?,'U12345678',?,?,1,?)
            """,
            (
                binding_id,
                endpoint_id,
                security_domain_id,
                channel_id or f"C{suffix:0>8}",
                thread_ts,
                f"idem-{suffix}",
                hashlib.sha256(f"request-{suffix}".encode()).hexdigest(),
                state,
            ),
        )
        return binding_id

    def prepared_attempt(
        self,
        endpoint_id,
        binding_id,
        attempt_id,
        generation,
        *,
        driver_kind="detached_native",
    ):
        event_key = "event-" + attempt_id
        payload = f"run {attempt_id}"
        self.db.execute(
            """
            INSERT INTO queued_turns(
              event_key,binding_id,binding_generation,ordered_at,mutation_kind,
              payload_inline,payload_sha256,payload_bytes,state
            ) VALUES(?,?,?,CURRENT_TIMESTAMP,'create',?,?,?,'ready')
            """,
            (
                event_key,
                binding_id,
                generation,
                payload,
                hashlib.sha256(payload.encode()).hexdigest(),
                len(payload.encode()),
            ),
        )
        self.db.execute(
            """
            INSERT INTO native_attempts(
              attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
              driver_request_id,driver_request_hash,reply_token_hash,state
            ) VALUES(?,?,?,?,?,?,?,?,'prepared')
            """,
            (
                attempt_id,
                endpoint_id,
                binding_id,
                generation,
                driver_kind,
                f"submit-{attempt_id}",
                hashlib.sha256(f"submit-{attempt_id}".encode()).hexdigest(),
                hashlib.sha256(attempt_id.encode()).hexdigest(),
            ),
        )
        self.db.execute(
            """
            INSERT INTO native_attempt_turns(
              attempt_id,ordinal,event_key,binding_id,turn_binding_generation
            ) VALUES(?,0,?,?,?)
            """,
            (attempt_id, event_key, binding_id, generation),
        )
        return event_key

    def open_attempt(
        self,
        endpoint_id,
        binding_id,
        attempt_id,
        *,
        fence=1,
        generation=1,
        driver_kind="detached_native",
    ):
        current_fence = self.db.execute(
            "SELECT next_lease_fence FROM endpoints WHERE endpoint_id=?",
            (endpoint_id,),
        ).fetchone()[0]
        if current_fence != fence:
            self.db.execute(
                "UPDATE endpoints SET next_lease_fence=? WHERE endpoint_id=?",
                (fence, endpoint_id),
            )
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,1,?,datetime('now','+30 minutes'))
            """,
            (attempt_id, endpoint_id, fence),
        )
        return self.prepared_attempt(
            endpoint_id,
            binding_id,
            attempt_id,
            generation,
            driver_kind=driver_kind,
        )

    def receipt(
        self,
        attempt_id,
        endpoint_id,
        fence,
        sequence,
        state,
        *,
        operation="submit",
    ):
        receipt_id = f"receipt:{attempt_id}:{sequence}"
        cursor = f"cursor:{attempt_id}:{sequence}"
        attempt = self.db.execute(
            """
            SELECT driver_request_id,driver_request_hash,
                   cancel_request_id,cancel_request_hash
            FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        request_id, request_hash = (
            (attempt[0], attempt[1])
            if operation == "submit"
            else (attempt[2], attempt[3])
        )
        self.db.execute(
            """
            INSERT INTO driver_receipts(
              receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
              driver_kind,driver_incarnation,operation,request_id,request_hash,
              watch_cursor,state,observed_at
            ) VALUES(?,?,?,?,?,'detached_native','process-fixture',?,?,?,?,?,
                     CURRENT_TIMESTAMP)
            """,
            (
                receipt_id,
                attempt_id,
                endpoint_id,
                fence,
                sequence,
                operation,
                request_id,
                request_hash,
                cursor,
                state,
            ),
        )
        return receipt_id, cursor

    def test_schema_installs_with_foreign_keys_and_all_authorities(self):
        self.assertEqual(self.db.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        objects = set(self.schema.object_names(self.db))
        self.assertTrue(
            {
                "endpoints",
                "thread_bindings",
                "queued_turns",
                "endpoint_leases",
                "native_attempts",
                "native_attempt_turns",
                "operator_resolutions",
                "endpoint_one_open_lease",
            }.issubset(objects)
        )
        self.assertEqual(self.schema.invariant_violations(self.db), [])

    def test_schema_rejects_nonprojectable_endpoint_and_driver_kinds(self):
        descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=1000,
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO endpoints(
                  endpoint_id,endpoint_key,endpoint_kind,source_kind,source_json,
                  ref_version,incarnation,security_domain_id,instance_uid,
                  workspace_id,persona_id,authorized_owners_json,
                  authorized_owners_hash,policy_generation,state
                ) VALUES('end-future','future','hermes_continuation','future_kind',
                         '{}',1,1,?,1000,'T12345678','primary','["U12345678"]',
                         ?,1,'ready')
                """,
                (descriptor.security_domain_id, descriptor.authorized_owners_hash),
            )

    def test_attempt_driver_must_match_endpoint_adapter(self):
        positive_pairs = (
            ("zellij_pane", "zellij"),
            ("herdr_agent", "herdr"),
            ("detached_native", "detached_native"),
            ("hermes_continuation", "detached_native"),
        )
        for index, (endpoint_kind, driver_kind) in enumerate(positive_pairs, 1):
            with self.subTest(endpoint_kind=endpoint_kind, driver_kind=driver_kind):
                endpoint_id = self.endpoint(
                    key=f"adapter-{index}", endpoint_kind=endpoint_kind
                )
                binding_id = self.binding(endpoint_id, f"adapter-{index}")
                attempt_id = f"att-adapter-{index:02d}-positive"
                self.open_attempt(
                    endpoint_id,
                    binding_id,
                    attempt_id,
                    driver_kind=driver_kind,
                )
                self.assertNotIn(
                    "attempt_driver_endpoint_mismatch",
                    self.schema.invariant_violations(self.db),
                )
                self.db.rollback()

        endpoint_id = self.endpoint(key="adapter-mismatch")
        binding_id = self.binding(endpoint_id, "adapter-mismatch")
        self.db.commit()
        self.db.execute("BEGIN")
        self.db.execute("PRAGMA defer_foreign_keys=ON")
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES('att-adapter-mismatch',?,1,1,datetime('now','+30 minutes'))
            """,
            (endpoint_id,),
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "not runnable"):
            self.prepared_attempt(
                endpoint_id,
                binding_id,
                "att-adapter-mismatch",
                1,
                driver_kind="herdr",
            )
        self.db.rollback()

        self.db.execute("DROP TRIGGER native_attempt_binding_guard")
        self.open_attempt(
            endpoint_id,
            binding_id,
            "att-adapter-validator",
            driver_kind="herdr",
        )
        self.assertIn(
            "attempt_driver_endpoint_mismatch",
            self.schema.invariant_violations(self.db),
        )

        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "unsupported-driver")
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES('att-unsupported-driver',?,1,1,datetime('now','+30 minutes'))
            """,
            (endpoint_id,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO native_attempts(
                  attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
                  driver_request_id,driver_request_hash,reply_token_hash,state
                ) VALUES('att-unsupported-driver',?,?,1,'future_driver','request',
                         ?,?,'prepared')
                """,
                (
                    endpoint_id,
                    binding_id,
                    hashlib.sha256(b"request").hexdigest(),
                    hashlib.sha256(b"token").hexdigest(),
                ),
            )

    def test_one_endpoint_owns_many_independent_thread_bindings(self):
        endpoint_id = self.endpoint()
        bindings = [self.binding(endpoint_id, str(index)) for index in range(3)]
        rows = self.db.execute(
            "SELECT binding_id FROM thread_bindings WHERE endpoint_id=? ORDER BY binding_id",
            (endpoint_id,),
        ).fetchall()
        self.assertEqual([row[0] for row in rows], sorted(bindings))

    def test_physical_endpoint_cannot_cross_security_domains(self):
        self.endpoint(key="one-pane", domain="sec_primary")
        with self.assertRaises(sqlite3.IntegrityError):
            self.endpoint(key="one-pane", domain="sec_other")

    def test_nonclosed_binding_uniqueness_covers_rebind_required(self):
        endpoint_id = self.endpoint()
        self.binding(
            endpoint_id,
            "1",
            state="rebind_required",
            thread_ts="1786000000.000001",
            channel_id="C12345678",
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.binding(
                endpoint_id,
                "2",
                state="active",
                thread_ts="1786000000.000001",
                channel_id="C12345678",
            )

    def test_binding_workspace_and_owner_are_enforced_by_security_domain(self):
        endpoint_id = self.endpoint()
        domain_id = self.db.execute(
            "SELECT security_domain_id FROM endpoints WHERE endpoint_id=?",
            (endpoint_id,),
        ).fetchone()[0]
        fields = (
            endpoint_id,
            domain_id,
            hashlib.sha256(b"security-bound-request").hexdigest(),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO thread_bindings(
                  binding_id,endpoint_id,security_domain_id,team_id,channel_id,
                  thread_ts,owner_user_id,idempotency_key,request_hash,
                  generation,state
                ) VALUES('brg_wrongworkspace000000000',?,?,'T_OTHER','C00000001',
                         '1786000000.000001','U12345678','wrong-workspace',?,
                         1,'active')
                """,
                fields,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO thread_bindings(
                  binding_id,endpoint_id,security_domain_id,team_id,channel_id,
                  thread_ts,owner_user_id,idempotency_key,request_hash,
                  generation,state
                ) VALUES('brg_wrongowner000000000000',?,?,'T12345678','C00000002',
                         '1786000000.000002','U_ATTACKER','wrong-owner',?,
                         1,'active')
                """,
                fields,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO endpoint_authorized_owners(
                  endpoint_id,security_domain_id,owner_user_id
                ) VALUES(?,?,'U_ATTACKER')
                """,
                (endpoint_id, domain_id),
            )

    def test_binding_identity_close_and_rebind_are_generation_fenced(self):
        first_endpoint = self.endpoint(key="first-endpoint")
        second_endpoint = self.endpoint(key="second-endpoint")
        binding_id = self.binding(first_endpoint, "1")
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE thread_bindings SET channel_id='C99999999' WHERE binding_id=?",
                (binding_id,),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE thread_bindings SET state='closed' WHERE binding_id=?",
                (binding_id,),
            )
        self.db.execute(
            "UPDATE thread_bindings SET state='rebind_required' WHERE binding_id=?",
            (binding_id,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE thread_bindings SET state='active' WHERE binding_id=?",
                (binding_id,),
            )
        self.db.execute(
            """
            UPDATE thread_bindings SET endpoint_id=?,state='active',
              generation=generation+1,error_code=NULL WHERE binding_id=?
            """,
            (second_endpoint, binding_id),
        )
        self.assertEqual(
            tuple(
                self.db.execute(
                    "SELECT endpoint_id,generation,state FROM thread_bindings WHERE binding_id=?",
                    (binding_id,),
                ).fetchone()
            ),
            (second_endpoint, 2, "active"),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                UPDATE thread_bindings SET endpoint_id=?,state='closed',
                  generation=generation+1 WHERE binding_id=?
                """,
                (first_endpoint, binding_id),
            )

    def test_binding_cannot_change_with_open_lease_and_close_requires_empty_queue(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        attempt_id = "att-binding-open-lease-00001"
        event_key = self.open_attempt(endpoint_id, binding_id, attempt_id)
        self.db.execute(
            """
            UPDATE native_attempts SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        for statement in (
            "UPDATE thread_bindings SET state='rebind_required' WHERE binding_id=?",
            "UPDATE thread_bindings SET state='closed',generation=generation+1 WHERE binding_id=?",
        ):
            with self.assertRaises(sqlite3.IntegrityError):
                self.db.execute(statement, (binding_id,))

        cancel_request_id = f"cancel-{attempt_id}"
        cancel_request_hash = hashlib.sha256(cancel_request_id.encode()).hexdigest()
        self.db.execute(
            """
            UPDATE native_attempts SET cancel_request_id=?,cancel_request_hash=?
            WHERE attempt_id=?
            """,
            (cancel_request_id, cancel_request_hash, attempt_id),
        )
        receipt_id, cursor = self.receipt(
            attempt_id, endpoint_id, 1, 1, "cancelled", operation="cancel"
        )
        self.db.execute(
            """
            UPDATE native_attempts SET state='cancelled',terminal_at=CURRENT_TIMESTAMP,
              last_driver_receipt_id=?,last_driver_sequence=1,receipt_cursor=?
            WHERE attempt_id=?
            """,
            (receipt_id, cursor, attempt_id),
        )
        self.db.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='cancelled' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                UPDATE thread_bindings SET state='closed',generation=generation+1
                WHERE binding_id=?
                """,
                (binding_id,),
            )
        self.db.execute(
            """
            UPDATE queued_turns SET state='cancelled',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key=?
            """,
            (event_key,),
        )
        self.db.execute(
            """
            UPDATE thread_bindings SET state='closed',generation=generation+1
            WHERE binding_id=?
            """,
            (binding_id,),
        )

    def test_attempt_creation_cannot_cross_endpoint_or_seed_receipt_cursor(self):
        first_endpoint = self.endpoint(key="attempt-endpoint-a")
        second_endpoint = self.endpoint(key="attempt-endpoint-b")
        binding_id = self.binding(first_endpoint, "1")
        self.db.execute("SAVEPOINT cross_endpoint")
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES('att-cross-endpoint-00000001',?,1,1,datetime('now','+30 minutes'))
            """,
            (second_endpoint,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO native_attempts(
                  attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
                  driver_request_id,driver_request_hash,reply_token_hash,state
                ) VALUES('att-cross-endpoint-00000001',?,?,1,'detached_native',
                         'submit-cross',?,?,'prepared')
                """,
                (
                    second_endpoint,
                    binding_id,
                    hashlib.sha256(b"submit-cross").hexdigest(),
                    hashlib.sha256(b"token-cross").hexdigest(),
                ),
            )
        self.db.execute("ROLLBACK TO cross_endpoint")
        self.db.execute("RELEASE cross_endpoint")
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES('att-seeded-cursor-000000001',?,1,1,datetime('now','+30 minutes'))
            """,
            (first_endpoint,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO native_attempts(
                  attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
                  driver_request_id,driver_request_hash,reply_token_hash,
                  receipt_cursor,state
                ) VALUES('att-seeded-cursor-000000001',?,?,1,'detached_native',
                         'submit-seeded',?,?,'cursor-skips-history','prepared')
                """,
                (
                    first_endpoint,
                    binding_id,
                    hashlib.sha256(b"submit-seeded").hexdigest(),
                    hashlib.sha256(b"token-seeded").hexdigest(),
                ),
            )

    def test_turn_admission_and_retry_require_current_unexecuted_work(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO queued_turns(
                  event_key,binding_id,binding_generation,ordered_at,mutation_kind,
                  payload_inline,payload_sha256,payload_bytes,state
                ) VALUES('event-future',?,999,CURRENT_TIMESTAMP,'create','run',?,3,'ready')
                """,
                (binding_id, hashlib.sha256(b"run").hexdigest()),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO queued_turns(
                  event_key,binding_id,binding_generation,ordered_at,mutation_kind,
                  payload_inline,payload_sha256,payload_bytes,state,terminal_at
                ) VALUES('event-born-done',?,1,CURRENT_TIMESTAMP,'create','run',?,3,
                         'completed',CURRENT_TIMESTAMP)
                """,
                (binding_id, hashlib.sha256(b"run").hexdigest()),
            )

        first_attempt = "att-not-started-00000000001"
        event_key = self.open_attempt(endpoint_id, binding_id, first_attempt)
        receipt_id, cursor = self.receipt(
            first_attempt, endpoint_id, 1, 1, "not_started"
        )
        self.db.execute(
            """
            UPDATE native_attempts SET state='failed_before_start',
              last_driver_receipt_id=?,last_driver_sequence=1,receipt_cursor=?,
              terminal_at=CURRENT_TIMESTAMP WHERE attempt_id=?
            """,
            (receipt_id, cursor, first_attempt),
        )
        self.db.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='not_started' WHERE attempt_id=?
            """,
            (first_attempt,),
        )
        second_attempt = "att-retry-00000000000000001"
        self.db.execute(
            "UPDATE endpoints SET next_lease_fence=2 WHERE endpoint_id=?",
            (endpoint_id,),
        )
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,1,2,datetime('now','+30 minutes'))
            """,
            (second_attempt, endpoint_id),
        )
        self.db.execute(
            """
            INSERT INTO native_attempts(
              attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
              driver_request_id,driver_request_hash,reply_token_hash,state
            ) VALUES(?,?,?,1,'detached_native',?,?,?,'prepared')
            """,
            (
                second_attempt,
                endpoint_id,
                binding_id,
                f"submit-{second_attempt}",
                hashlib.sha256(f"submit-{second_attempt}".encode()).hexdigest(),
                hashlib.sha256(second_attempt.encode()).hexdigest(),
            ),
        )
        self.db.execute(
            """
            INSERT INTO native_attempt_turns(
              attempt_id,ordinal,event_key,binding_id,turn_binding_generation
            ) VALUES(?,0,?,?,1)
            """,
            (second_attempt, event_key, binding_id),
        )
        self.assertEqual(self.schema.invariant_violations(self.db), [])

    def test_premature_release_and_operator_terminal_without_receipt_are_rejected(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        attempt_id = "att-operator-proof-000000001"
        event_key = self.open_attempt(endpoint_id, binding_id, attempt_id)
        self.db.execute(
            """
            UPDATE native_attempts SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.execute(
            "UPDATE native_attempts SET state='uncertain' WHERE attempt_id=?",
            (attempt_id,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
                  release_reason='premature' WHERE attempt_id=?
                """,
                (attempt_id,),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                UPDATE native_attempts SET state='operator_abandoned',
                  terminal_at=CURRENT_TIMESTAMP WHERE attempt_id=?
                """,
                (attempt_id,),
            )
        self.db.execute(
            """
            INSERT INTO operator_resolutions(
              attempt_id,endpoint_id,lease_fence,action,source_kind,
              authority_receipt_id,operator_principal_hash,evidence_ref,
              evidence_sha256,resolved_at
            ) VALUES(?,?,1,'abandon','authority','authority-receipt-1',?,?,?,
                     CURRENT_TIMESTAMP)
            """,
            (
                attempt_id,
                endpoint_id,
                hashlib.sha256(b"operator").hexdigest(),
                "authority://resolution/1",
                hashlib.sha256(b"evidence").hexdigest(),
            ),
        )
        self.db.execute(
            """
            UPDATE native_attempts SET state='operator_abandoned',
              terminal_at=CURRENT_TIMESTAMP WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.execute(
            """
            UPDATE queued_turns SET state='cancelled',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key=?
            """,
            (event_key,),
        )
        self.db.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='operator_abandoned' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.assertEqual(self.schema.invariant_violations(self.db), [])

    def test_retired_endpoint_invalidates_bindings_and_rejects_new_live_binding(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        self.db.execute(
            "UPDATE endpoints SET state='retired' WHERE endpoint_id=?",
            (endpoint_id,),
        )
        self.assertEqual(
            tuple(
                self.db.execute(
                    "SELECT state,error_code FROM thread_bindings WHERE binding_id=?",
                    (binding_id,),
                ).fetchone()
            ),
            ("rebind_required", "endpoint_retired"),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.binding(endpoint_id, "2")

    def test_queued_turn_content_and_terminal_history_are_immutable(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        self.db.execute(
            """
            INSERT INTO queued_turns(
              event_key,binding_id,binding_generation,ordered_at,mutation_kind,
              payload_inline,payload_sha256,payload_bytes,state
            ) VALUES('event-immutable',?,1,CURRENT_TIMESTAMP,'create','run',?,3,'ready')
            """,
            (binding_id, hashlib.sha256(b"run").hexdigest()),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE queued_turns SET payload_inline='changed' WHERE event_key='event-immutable'"
            )
        self.db.execute(
            """
            UPDATE queued_turns SET state='cancelled',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key='event-immutable'
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                UPDATE queued_turns SET state='ready',terminal_at=NULL
                WHERE event_key='event-immutable'
                """
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "DELETE FROM queued_turns WHERE event_key='event-immutable'"
            )

    def test_inline_content_digest_mismatch_fails_validation(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        self.db.execute(
            """
            INSERT INTO queued_turns(
              event_key,binding_id,binding_generation,ordered_at,mutation_kind,
              payload_inline,payload_sha256,payload_bytes,state
            ) VALUES('event-bad-content',?,1,CURRENT_TIMESTAMP,'create','run',?,999,'ready')
            """,
            (binding_id, "0" * 64),
        )
        self.assertIn(
            "queued_turn_inline_content_mismatch",
            self.schema.invariant_violations(self.db),
        )

    def test_one_open_lease_and_incarnation_fence_are_enforced(self):
        endpoint_id = self.endpoint()
        first = self.binding(endpoint_id, "1")
        second = self.binding(endpoint_id, "2")
        self.db.commit()
        self.db.execute("BEGIN")
        self.db.execute("PRAGMA defer_foreign_keys=ON")
        for attempt_id, binding_id, fence in (
            ("att_first000000000000000001", first, 1),
            ("att_second00000000000000002", second, 2),
        ):
            if attempt_id.startswith("att_second"):
                self.db.execute(
                    "UPDATE endpoints SET next_lease_fence=2 WHERE endpoint_id=?",
                    (endpoint_id,),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    self.db.execute(
                        """
                        INSERT INTO endpoint_leases(
                          attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
                        ) VALUES(?,?,1,?,datetime('now','+30 minutes'))
                        """,
                        (attempt_id, endpoint_id, fence),
                    )
                break
            self.db.execute(
                """
                INSERT INTO endpoint_leases(
                  attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
                ) VALUES(?,?,1,?,datetime('now','+30 minutes'))
                """,
                (attempt_id, endpoint_id, fence),
            )
            self.prepared_attempt(endpoint_id, binding_id, attempt_id, 1)
        self.db.rollback()

    def test_incarnation_change_invalidates_siblings_and_rejects_open_lease(self):
        endpoint_id = self.endpoint()
        bindings = [self.binding(endpoint_id, str(index)) for index in (1, 2)]
        self.db.execute(
            "UPDATE endpoints SET incarnation=incarnation+1 WHERE endpoint_id=?",
            (endpoint_id,),
        )
        states = self.db.execute(
            "SELECT state,error_code FROM thread_bindings ORDER BY binding_id"
        ).fetchall()
        self.assertEqual(
            [(row[0], row[1]) for row in states],
            [("rebind_required", "endpoint_incarnation_changed")] * 2,
        )

        self.db.execute(
            """
            UPDATE thread_bindings SET state='active',error_code=NULL,
              generation=generation+1 WHERE binding_id=?
            """,
            (bindings[0],),
        )
        self.db.execute("UPDATE endpoints SET next_lease_fence=2 WHERE endpoint_id=?", (endpoint_id,))
        self.db.commit()
        self.db.execute("BEGIN")
        self.db.execute("PRAGMA defer_foreign_keys=ON")
        attempt_id = "att_open0000000000000000001"
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,2,2,datetime('now','+30 minutes'))
            """,
            (attempt_id, endpoint_id),
        )
        self.prepared_attempt(endpoint_id, bindings[0], attempt_id, 2)
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE endpoints SET incarnation=incarnation+1 WHERE endpoint_id=?",
                (endpoint_id,),
            )
        self.db.rollback()

    def test_terminal_attempt_and_released_lease_cannot_be_reopened(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        attempt_id = "att_terminal000000000000001"
        self.db.commit()
        self.db.execute("BEGIN")
        self.db.execute("PRAGMA defer_foreign_keys=ON")
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,1,1,datetime('now','+30 minutes'))
            """,
            (attempt_id, endpoint_id),
        )
        event_key = self.prepared_attempt(endpoint_id, binding_id, attempt_id, 1)
        cancel_request_id = f"cancel-{attempt_id}"
        cancel_request_hash = hashlib.sha256(cancel_request_id.encode()).hexdigest()
        self.db.execute(
            """
            UPDATE native_attempts SET cancel_request_id=?,cancel_request_hash=?
            WHERE attempt_id=?
            """,
            (cancel_request_id, cancel_request_hash, attempt_id),
        )
        receipt_id, cursor = self.receipt(
            attempt_id, endpoint_id, 1, 1, "cancelled", operation="cancel"
        )
        self.db.execute(
            """
            UPDATE native_attempts SET state='cancelled',terminal_at=CURRENT_TIMESTAMP,
              last_driver_receipt_id=?,last_driver_sequence=1,
              receipt_cursor=?
            WHERE attempt_id=?
            """,
            (receipt_id, cursor, attempt_id),
        )
        self.db.execute(
            """
            UPDATE queued_turns SET state='cancelled',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key=?
            """,
            (event_key,),
        )
        self.db.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='cancelled' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.commit()

        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE native_attempts SET state='accepted',terminal_at=NULL WHERE attempt_id=?",
                (attempt_id,),
            )

    def test_cancel_not_started_receipt_is_bound_to_cancel_request(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "cancel-not-started")
        attempt_id = "att-cancel-not-started-00001"
        event_key = self.open_attempt(endpoint_id, binding_id, attempt_id)
        cancel_id = "cancel-request-1"
        cancel_hash = hashlib.sha256(b"cancel-request-1").hexdigest()
        self.db.execute(
            """
            UPDATE native_attempts SET cancel_request_id=?,cancel_request_hash=?
            WHERE attempt_id=?
            """,
            (cancel_id, cancel_hash, attempt_id),
        )
        self.db.execute(
            """
            INSERT INTO driver_receipts(
              receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
              driver_kind,driver_incarnation,operation,request_id,request_hash,
              watch_cursor,state,observed_at
            ) VALUES('receipt-cancel-not-started',?,?,1,1,'detached_native',
                     'process-fixture','cancel',?,?,'cursor-cancel-not-started',
                     'not_started',CURRENT_TIMESTAMP)
            """,
            (attempt_id, endpoint_id, cancel_id, cancel_hash),
        )
        self.db.execute(
            """
            UPDATE native_attempts SET state='cancelled',terminal_at=CURRENT_TIMESTAMP,
              last_driver_receipt_id='receipt-cancel-not-started',
              last_driver_sequence=1,receipt_cursor='cursor-cancel-not-started'
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.execute(
            """
            UPDATE queued_turns SET state='cancelled',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key=?
            """,
            (event_key,),
        )
        self.db.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='cancelled_not_started' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.commit()
        self.schema.require_valid(self.db)
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE endpoint_leases SET released_at=NULL,release_reason=NULL WHERE attempt_id=?",
                (attempt_id,),
            )

    def test_cancel_receipt_cannot_impersonate_execution_or_stop_accepted_work(self):
        endpoint_id = self.endpoint(key="cancel-receipt-boundary")
        binding_id = self.binding(endpoint_id, "cancel-receipt-boundary")
        attempt_id = "att-cancel-receipt-boundary"
        self.open_attempt(endpoint_id, binding_id, attempt_id)
        self.db.execute(
            """
            UPDATE native_attempts SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.execute(
            """
            UPDATE native_attempts SET state='accepted',accepted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        cancel_id = "cancel-boundary-request"
        cancel_hash = hashlib.sha256(cancel_id.encode()).hexdigest()
        self.db.execute(
            """
            UPDATE native_attempts SET cancel_request_id=?,cancel_request_hash=?
            WHERE attempt_id=?
            """,
            (cancel_id, cancel_hash, attempt_id),
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO driver_receipts(
                  receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                  driver_kind,driver_incarnation,operation,request_id,request_hash,
                  watch_cursor,state,observed_at
                ) VALUES('receipt-wrong-cancel',?,?,1,1,'detached_native','driver',
                         'cancel','wrong-request',?,'cursor-wrong','cancelled',
                         CURRENT_TIMESTAMP)
                """,
                (attempt_id, endpoint_id, hashlib.sha256(b"wrong").hexdigest()),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO driver_receipts(
                  receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                  driver_kind,driver_incarnation,operation,request_id,request_hash,
                  watch_cursor,state,observed_at
                ) VALUES('receipt-cancel-no-reply',?,?,1,1,'detached_native','driver',
                         'cancel',?,?,'cursor-invalid','no_reply',CURRENT_TIMESTAMP)
                """,
                (attempt_id, endpoint_id, cancel_id, cancel_hash),
            )

        receipt_id, cursor = self.receipt(
            attempt_id,
            endpoint_id,
            1,
            1,
            "not_started",
            operation="cancel",
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "durable execution proof"):
            self.db.execute(
                """
                UPDATE native_attempts SET state='cancelled',terminal_at=CURRENT_TIMESTAMP,
                  last_driver_receipt_id=?,last_driver_sequence=1,receipt_cursor=?
                WHERE attempt_id=?
                """,
                (receipt_id, cursor, attempt_id),
            )

    def test_driver_receipts_are_fenced_ordered_and_stop_at_terminal(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        attempt_id = "att_receipts0000000000000001"
        self.db.commit()
        self.db.execute("BEGIN")
        self.db.execute("PRAGMA defer_foreign_keys=ON")
        self.db.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,1,1,datetime('now','+30 minutes'))
            """,
            (attempt_id, endpoint_id),
        )
        event_key = self.prepared_attempt(endpoint_id, binding_id, attempt_id, 1)
        self.db.execute(
            """
            UPDATE native_attempts SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.execute(
            """
            UPDATE native_attempts SET state='accepted',accepted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        receipt_1, cursor_1 = self.receipt(
            attempt_id, endpoint_id, 1, 1, "accepted"
        )
        self.db.execute(
            """
            UPDATE native_attempts SET last_driver_receipt_id=?,
              last_driver_sequence=1,receipt_cursor=? WHERE attempt_id=?
            """,
            (receipt_1, cursor_1, attempt_id),
        )
        self.assertEqual(self.schema.invariant_violations(self.db), [])

        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO driver_receipts(
                  receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                  driver_kind,driver_incarnation,operation,request_id,request_hash,
                  watch_cursor,state,observed_at
                ) VALUES('receipt-3',?,?,1,3,'detached_native','process-1',
                         'submit',?,?, 'cursor-3','running',CURRENT_TIMESTAMP)
                """,
                (
                    attempt_id,
                    endpoint_id,
                    f"submit-{attempt_id}",
                    hashlib.sha256(f"submit-{attempt_id}".encode()).hexdigest(),
                ),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO driver_receipts(
                  receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                  driver_kind,driver_incarnation,operation,request_id,request_hash,
                  watch_cursor,state,observed_at
                ) VALUES('receipt-wrong-driver',?,?,1,2,'zellij_pane','process-1',
                         'submit',?,?, 'cursor-2','running',CURRENT_TIMESTAMP)
                """,
                (
                    attempt_id,
                    endpoint_id,
                    f"submit-{attempt_id}",
                    hashlib.sha256(f"submit-{attempt_id}".encode()).hexdigest(),
                ),
            )

        receipt_2, cursor_2 = self.receipt(
            attempt_id, endpoint_id, 1, 2, "no_reply"
        )
        self.db.execute(
            """
            UPDATE native_attempts SET last_driver_receipt_id=?,
              last_driver_sequence=2,receipt_cursor=?,state='no_reply',
              terminal_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (receipt_2, cursor_2, attempt_id),
        )
        self.db.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='no_reply' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        self.db.execute(
            """
            UPDATE queued_turns SET state='completed',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key=?
            """,
            (event_key,),
        )
        self.db.commit()
        self.assertEqual(self.schema.invariant_violations(self.db), [])

        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                """
                INSERT INTO driver_receipts(
                  receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                  driver_kind,driver_incarnation,watch_cursor,state,observed_at
                ) VALUES('receipt-after-terminal',?,?,1,3,'detached_native',
                         'process-1','cursor-3','no_reply',CURRENT_TIMESTAMP)
                """,
                (attempt_id, endpoint_id),
            )

    def test_security_descriptor_is_immutable_and_ref_change_needs_incarnation(self):
        endpoint_id = self.endpoint()
        binding_id = self.binding(endpoint_id, "1")
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE endpoints SET workspace_id='T87654321' WHERE endpoint_id=?",
                (endpoint_id,),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE endpoints SET source_json='{\"run\":\"new\"}' WHERE endpoint_id=?",
                (endpoint_id,),
            )
        self.db.execute(
            """
            UPDATE endpoints SET source_json='{"run":"new"}',
              incarnation=incarnation+1 WHERE endpoint_id=?
            """,
            (endpoint_id,),
        )
        binding = self.db.execute(
            "SELECT state,error_code FROM thread_bindings WHERE binding_id=?",
            (binding_id,),
        ).fetchone()
        self.assertEqual(tuple(binding), ("rebind_required", "endpoint_incarnation_changed"))


if __name__ == "__main__":
    unittest.main()
