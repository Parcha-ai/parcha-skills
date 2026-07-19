"""Unit tests for parable.py — config merge, validation, argv construction, event parsing."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "skills" / "parable" / "scripts" / "parable.py"
spec = importlib.util.spec_from_file_location("parable", SCRIPT)
parable = importlib.util.module_from_spec(spec)
sys.modules["parable"] = parable
spec.loader.exec_module(parable)


class TestMerge(unittest.TestCase):
    def test_executor_merges_per_field(self):
        base = {"executors": {"kimi": {"model": "m1", "effort": "high", "tags": ["a"]}}}
        overlay = {"executors": {"kimi": {"effort": "low"}}}
        out = parable.merge_configs(base, overlay)
        self.assertEqual(out["executors"]["kimi"]["effort"], "low")
        self.assertEqual(out["executors"]["kimi"]["model"], "m1")

    def test_new_executor_added(self):
        out = parable.merge_configs({"executors": {}}, {"executors": {"glm": {"model": "g"}}})
        self.assertIn("glm", out["executors"])

    def test_routing_is_whole_table_overwrite_per_key(self):
        base = {"routing": {"feature": ["sonnet"], "review": ["opus"]}}
        overlay = {"routing": {"feature": ["kimi"]}}
        out = parable.merge_configs(base, overlay)
        self.assertEqual(out["routing"]["feature"], ["kimi"])
        self.assertEqual(out["routing"]["review"], ["opus"])

    def test_builtin_defaults_survive_partial_overlay(self):
        out = parable.merge_configs(parable.BUILTIN_DEFAULTS, {"parable": {"default_executor": "kimi"}})
        self.assertEqual(out["parable"]["default_executor"], "kimi")
        self.assertIn("sonnet", out["executors"])

    def test_claude_session_merges_per_field(self):
        base = {"claude": {"base_url": "http://127.0.0.1:1", "brain_model": "old"}}
        overlay = {"claude": {"brain_model": "gpt-5.6-sol"}}
        out = parable.merge_configs(base, overlay)
        self.assertEqual(out["claude"]["base_url"], "http://127.0.0.1:1")
        self.assertEqual(out["claude"]["brain_model"], "gpt-5.6-sol")


class TestValidation(unittest.TestCase):
    def cfg(self, **kw):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        for key, val in kw.items():
            cfg = parable.merge_configs(cfg, {key: val}) if isinstance(val, dict) else cfg
        return cfg

    def test_builtin_defaults_valid(self):
        self.assertEqual(parable.validate_config(parable.BUILTIN_DEFAULTS), [])

    def test_unknown_provider_type_fails_loud(self):
        cfg = self.cfg(providers={"tau": {"type": "tau"}})
        problems = parable.validate_config(cfg)
        self.assertTrue(any("unknown type 'tau'" in p for p in problems))

    def test_pi_provider_requires_fields(self):
        cfg = self.cfg(providers={"fw": {"type": "pi"}})
        problems = parable.validate_config(cfg)
        self.assertTrue(any("type=pi requires base_url" in p for p in problems))
        self.assertTrue(any("type=pi requires env_key" in p for p in problems))

    def test_pi_provider_api_enum(self):
        cfg = self.cfg(providers={"fw": {"type": "pi", "base_url": "https://x", "env_key": "K", "api": "grpc"}})
        problems = parable.validate_config(cfg)
        self.assertTrue(any("api='grpc'" in p for p in problems))

    def test_effort_off_is_pi_only(self):
        cfg = self.cfg(providers={"fw": {"type": "pi", "base_url": "https://x", "env_key": "K"}})
        cfg["executors"]["m"] = {"provider": "fw", "model": "mm", "effort": "off"}
        self.assertEqual([p for p in parable.validate_config(cfg) if "effort" in p], [])
        cfg2 = self.cfg(providers={"cx": {"type": "codex", "base_url": "https://x", "env_key": "K", "wire_api": "responses"}})
        cfg2["executors"]["m"] = {"provider": "cx", "model": "mm", "effort": "off"}
        problems = parable.validate_config(cfg2)
        self.assertTrue(any("effort='off'" in p for p in problems))

    def test_effort_max_everywhere_ultra_codex_only(self):
        # "max" is legal for both harnesses.
        cfg = self.cfg(providers={"cx": {"type": "codex", "base_url": "https://x", "env_key": "K", "wire_api": "responses"},
                                  "fw": {"type": "pi", "base_url": "https://x", "env_key": "K"}})
        cfg["executors"]["a"] = {"provider": "cx", "model": "mm", "effort": "max"}
        cfg["executors"]["b"] = {"provider": "fw", "model": "mm", "effort": "max"}
        self.assertEqual([p for p in parable.validate_config(cfg) if "effort" in p], [])
        # "ultra" is codex-only (proactive multi-agent delegation); pi rejects it.
        cfg["executors"]["a"]["effort"] = "ultra"
        self.assertEqual([p for p in parable.validate_config(cfg) if "effort" in p], [])
        cfg["executors"]["b"]["effort"] = "ultra"
        problems = parable.validate_config(cfg)
        self.assertTrue(any("effort='ultra'" in p for p in problems))

    def test_codex_provider_requires_responses(self):
        cfg = self.cfg(providers={"fw": {"type": "codex", "base_url": "https://x", "env_key": "K", "wire_api": "chat"}})
        problems = parable.validate_config(cfg)
        self.assertTrue(any("only supports 'responses'" in p for p in problems))

    def test_routing_unknown_executor(self):
        cfg = self.cfg()
        cfg["routing"]["feature"] = ["ghost"]
        problems = parable.validate_config(cfg)
        self.assertTrue(any("routing.feature: unknown executor 'ghost'" in p for p in problems))

    def test_executor_unknown_provider(self):
        cfg = self.cfg()
        cfg["executors"]["x"] = {"provider": "nope", "model": "m"}
        problems = parable.validate_config(cfg)
        self.assertTrue(any("unknown provider 'nope'" in p for p in problems))

    def test_claude_session_requires_loopback_and_env_name_not_secret(self):
        cfg = self.cfg(claude={
            "base_url": "https://proxy.example.com",
            "auth_token_env": "not-valid-name!",
            "brain_model": "gpt-5.6-sol",
            "auth_token": "must-never-be-configured",
        })
        problems = parable.validate_config(cfg)
        self.assertTrue(any("must be an http(s) loopback URL" in p for p in problems))
        self.assertTrue(any("auth_token_env must name" in p for p in problems))
        self.assertTrue(any("unknown field(s): auth_token" in p for p in problems))

    def test_valid_claude_session_and_ipv6_loopback(self):
        for url in ("http://127.0.0.1:8317", "http://localhost:8317/v1",
                    "http://[::1]:8317"):
            with self.subTest(url=url):
                cfg = self.cfg(claude={
                    "base_url": url,
                    "auth_token_env": "CLIPROXY_API_KEY",
                    "brain_model": "gpt-5.6-sol",
                })
                self.assertEqual(parable.validate_config(cfg), [])


class TestClaudeLaunch(unittest.TestCase):
    def cfg(self):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        cfg["claude"] = {
            "base_url": "http://127.0.0.1:8317",
            "auth_token_env": "CLIPROXY_API_KEY",
            "brain_model": "gpt-5.6-sol",
        }
        cfg["executors"]["kimi"] = {
            "provider": "claude",
            "model": "kimi-k3",
            "use_for": "Independent implementation.",
        }
        return cfg

    def test_launch_env_is_per_process_and_scrubs_heterogeneous_override(self):
        source = {
            "PATH": "/bin",
            "CLIPROXY_API_KEY": "local-token",
            "ANTHROPIC_API_KEY": "direct-key",
            "CLAUDE_CODE_OAUTH_TOKEN": "native-token",
            "CLAUDE_CODE_SUBAGENT_MODEL": "gpt-5.6-sol",
        }
        argv, env = parable.build_claude_launch(
            self.cfg(), ["--", "--print", "hello"], source
        )
        self.assertEqual(
            argv, ["claude", "--model", "gpt-5.6-sol", "--print", "hello"]
        )
        self.assertEqual(env["ANTHROPIC_BASE_URL"], "http://127.0.0.1:8317")
        self.assertEqual(env["ANTHROPIC_AUTH_TOKEN"], "local-token")
        self.assertNotIn("CLIPROXY_API_KEY", env)
        self.assertNotIn("ANTHROPIC_API_KEY", env)
        self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", env)
        self.assertNotIn("CLAUDE_CODE_SUBAGENT_MODEL", env)
        self.assertIn("CLAUDE_CODE_SUBAGENT_MODEL", source)

    def test_forwarded_model_override_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "parable.toml owns"):
            parable.build_claude_launch(
                self.cfg(), ["--model", "opus"], {"CLIPROXY_API_KEY": "x"}
            )

    def test_custom_model_agent_is_namespaced_and_exact(self):
        cfg = self.cfg()
        self.assertEqual(list(parable.custom_claude_executors(cfg)), ["kimi"])
        rendered = parable.render_claude_agent("kimi", cfg["executors"]["kimi"])
        self.assertIn("name: parable-kimi", rendered)
        self.assertIn('model: "kimi-k3"', rendered)
        self.assertNotIn("CLIPROXY_API_KEY", rendered)
        self.assertEqual(
            parable.claude_required_models(cfg), ["gpt-5.6-sol", "kimi-k3"]
        )

    def test_builtin_aliases_do_not_generate_agents(self):
        cfg = self.cfg()
        del cfg["executors"]["kimi"]
        for index, model in enumerate((
            "inherit", "sonnet", "opus", "haiku", "best",
            "sonnet[1m]", "opus[1m]", "opusplan",
        )):
            cfg["executors"][f"native-{index}"] = {
                "provider": "claude", "model": model
            }
        self.assertEqual(parable.custom_claude_executors(cfg), {})

    def test_slug_collisions_fail_closed(self):
        cfg = self.cfg()
        cfg["executors"]["ki_mi"] = {
            "provider": "claude", "model": "other-model"
        }
        cfg["executors"]["ki-mi"] = {
            "provider": "claude", "model": "another-model"
        }
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "both map"):
                parable.sync_claude_agents(Path(tmp), cfg)


class TestArgv(unittest.TestCase):
    def make_cfg(self):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        cfg["providers"]["fireworks"] = {
            "type": "codex",
            "base_url": "https://api.fireworks.ai/inference/v1",
            "env_key": "FIREWORKS_API_KEY",
            "wire_api": "responses",
        }
        cfg["executors"]["kimi"] = {
            "provider": "fireworks",
            "model": "accounts/fireworks/models/kimi-k2p7-code",
            "effort": "high",
        }
        return cfg

    def test_run_argv_shape(self):
        argv, overrides = parable.build_run_argv(self.make_cfg(), "kimi", Path("/w"), Path("/l.txt"))
        joined = " ".join(argv)
        self.assertIn("codex exec --yolo --json -C /w", joined)
        self.assertIn('model_providers.parable_fireworks.base_url="https://api.fireworks.ai/inference/v1"', joined)
        self.assertIn('model_providers.parable_fireworks.wire_api="responses"', joined)
        self.assertIn('model_provider="parable_fireworks"', joined)
        self.assertIn('model="accounts/fireworks/models/kimi-k2p7-code"', joined)
        self.assertIn('model_reasoning_effort="high"', joined)
        self.assertEqual(argv[-1], "-")  # plan arrives on stdin
        # overrides are exactly the replayable flags, embedded intact in argv
        self.assertIn(" ".join(overrides), joined)
        self.assertTrue(all(o in argv for o in overrides))

    def test_effort_always_pinned(self):
        cfg = self.make_cfg()
        del cfg["executors"]["kimi"]["effort"]
        argv, _ = parable.build_run_argv(cfg, "kimi", Path("/w"), Path("/l.txt"))
        self.assertIn('model_reasoning_effort="high"', " ".join(argv))

    def test_codex_native_has_no_provider_overrides(self):
        cfg = self.make_cfg()
        cfg["providers"]["openai"] = {"type": "codex-native"}
        cfg["executors"]["gpt55"] = {"provider": "openai", "model": "gpt-5.5"}
        argv, _ = parable.build_run_argv(cfg, "gpt55", Path("/w"), Path("/l.txt"))
        joined = " ".join(argv)
        self.assertNotIn("model_providers", joined)
        self.assertIn('model="gpt-5.5"', joined)


class TestEventParsing(unittest.TestCase):
    def write_events(self, tmp, events):
        p = Path(tmp) / "harness.jsonl"
        p.write_text("\n".join(json.dumps(e) for e in events))
        return p

    def test_parse_stream(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write_events(tmp, [
                {"type": "thread.started", "thread_id": "abc-123"},
                {"type": "turn.started"},
                {"type": "item.completed", "item": {"type": "command_execution", "command": "ls"}},
                {"type": "item.completed", "item": {"type": "agent_message", "text": "done, output is 5"}},
                {"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 20}},
            ])
            facts = parable.parse_events(p)
        self.assertEqual(facts["session_id"], "abc-123")
        self.assertEqual(facts["phase"], "complete")
        self.assertEqual(facts["turns"], 1)
        self.assertEqual(facts["tool_calls"], 1)
        self.assertEqual(facts["last_message"], "done, output is 5")
        self.assertEqual(facts["usage"]["input_tokens"], 100)

    def test_non_json_lines_skipped(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "h.jsonl"
            p.write_text("warning: some banner\n" + json.dumps({"type": "thread.started", "thread_id": "x"}))
            facts = parable.parse_events(p)
        self.assertEqual(facts["session_id"], "x")

    def test_error_items_collected(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write_events(tmp, [
                {"type": "thread.started", "thread_id": "y"},
                {"type": "item.completed", "item": {"type": "error", "message": "stream disconnected"}},
            ])
            facts = parable.parse_events(p)
        self.assertEqual(facts["errors"], ["stream disconnected"])


PI_PROV = {"type": "pi", "base_url": "https://api.fw.test/v1", "env_key": "FW_KEY"}
PI_EX = {
    "provider": "fw", "model": "accounts/fw/models/mini", "effort": "medium",
    "cost": {"in": 0.30, "out": 1.20, "cache_in": 0.06}, "context_ktok": 512,
}


class TestPiModelsJson(unittest.TestCase):
    def test_mapping(self):
        out = parable.build_pi_models_json("fw", PI_PROV, PI_EX, "minimax")
        prov = out["providers"]["parable_fw"]
        self.assertEqual(prov["baseUrl"], "https://api.fw.test/v1")
        self.assertEqual(prov["apiKey"], "$FW_KEY")
        self.assertEqual(prov["api"], "openai-completions")
        m = prov["models"][0]
        self.assertEqual(m["id"], "accounts/fw/models/mini")
        self.assertEqual(m["name"], "minimax")
        self.assertEqual(m["cost"], {"input": 0.30, "output": 1.20, "cacheRead": 0.06, "cacheWrite": 0})
        self.assertEqual(m["contextWindow"], 512000)
        self.assertTrue(m["reasoning"])

    def test_defaults_and_overrides(self):
        ex = {"provider": "fw", "model": "m", "reasoning": False,
              "model_overrides": {"maxTokens": 9000, "reasoning": True}}
        m = parable.build_pi_models_json("fw", PI_PROV, ex, "x")["providers"]["parable_fw"]["models"][0]
        self.assertEqual(m["cost"]["input"], 0)
        self.assertNotIn("contextWindow", m)
        self.assertEqual(m["maxTokens"], 9000)
        self.assertTrue(m["reasoning"])  # model_overrides merge wins

    def test_no_key_material(self):
        out = json.dumps(parable.build_pi_models_json("fw", PI_PROV, PI_EX, "x"))
        self.assertNotIn("sk-", out)
        self.assertIn("$FW_KEY", out)


class TestPiArgv(unittest.TestCase):
    def make_cfg(self):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        cfg["providers"]["fw"] = dict(PI_PROV)
        cfg["executors"]["minimax"] = dict(PI_EX)
        return cfg

    def test_shape(self):
        argv, overrides = parable.build_pi_argv(self.make_cfg(), "minimax", Path("/r"), "sid-1", Path("/r/plan.md"))
        joined = " ".join(argv)
        self.assertTrue(joined.startswith("pi -p --mode json"))
        # provider and model are separate flags: slashed model ids mis-split
        # in the combined provider/id form
        self.assertIn("--provider parable_fw", joined)
        self.assertIn("--model accounts/fw/models/mini", joined)
        self.assertIn("--thinking medium", joined)
        self.assertIn("--session-dir /r/sessions --session-id sid-1", joined)
        self.assertIn("--no-extensions --no-skills --no-prompt-templates --no-approve", joined)
        self.assertEqual(argv[-1], "@/r/plan.md")
        self.assertEqual(argv, ["pi", "-p"] + overrides + ["@/r/plan.md"])

    def test_effort_pinned_when_absent(self):
        cfg = self.make_cfg()
        del cfg["executors"]["minimax"]["effort"]
        argv, _ = parable.build_pi_argv(cfg, "minimax", Path("/r"), "s", Path("/r/plan.md"))
        self.assertIn("--thinking high", " ".join(argv))


class TestPiEventParsing(unittest.TestCase):
    def write(self, tmp, events):
        p = Path(tmp) / "h.jsonl"
        p.write_text("\n".join(json.dumps(e) for e in events))
        return p

    def assistant_end(self, text, cost=0.001, stop="stop", **usage):
        u = {"input": usage.get("inp", 100), "output": usage.get("out", 10),
             "cacheRead": usage.get("cached", 50), "cacheWrite": 0,
             "cost": {"total": cost}}
        return {"type": "message_end", "message": {
            "role": "assistant", "stopReason": stop,
            "content": [{"type": "thinking", "thinking": "..."}, {"type": "text", "text": text}],
            "usage": u}}

    def test_parse_normalizes_to_codex_keys(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write(tmp, [
                {"type": "session", "id": "pid-9"},
                {"type": "turn_start"},
                {"type": "tool_execution_start", "toolName": "bash"},
                self.assistant_end("done, PASS"),
                {"type": "agent_end"},
            ])
            facts = parable.parse_pi_events(p)
        self.assertEqual(facts["session_id"], "pid-9")
        self.assertEqual(facts["phase"], "complete")
        self.assertEqual(facts["turns"], 1)
        self.assertEqual(facts["tool_calls"], 1)
        self.assertEqual(facts["last_message"], "done, PASS")
        self.assertEqual(facts["usage"]["input_tokens"], 100)
        self.assertEqual(facts["usage"]["cached_input_tokens"], 50)
        self.assertEqual(facts["usage"]["output_tokens"], 10)
        self.assertAlmostEqual(facts["usage"]["cost"], 0.001)

    def test_error_stop_reason(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write(tmp, [
                {"type": "session", "id": "x"},
                {"type": "message_end", "message": {"role": "assistant", "stopReason": "error",
                                                    "errorMessage": "boom", "content": [], "usage": {}}},
                {"type": "agent_end"},
            ])
            facts = parable.parse_pi_events(p)
        self.assertEqual(facts["phase"], "error")  # agent_end must not mask the error
        self.assertEqual(facts["errors"], ["boom"])

    def test_merge_with_codex_facts(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write(tmp, [{"type": "session", "id": "s"}, {"type": "turn_start"},
                                 self.assistant_end("fixed", cost=0.002), {"type": "agent_end"}])
            pi_facts = parable.parse_pi_events(p)
        codex_facts = {"session_id": None, "turns": 2, "tool_calls": 3, "last_message": "old",
                       "errors": [], "usage": {"input_tokens": 500}, "phase": "complete"}
        merged = parable.merge_facts([codex_facts, pi_facts])
        self.assertEqual(merged["turns"], 3)
        self.assertEqual(merged["last_message"], "fixed")
        self.assertEqual(merged["usage"]["input_tokens"], 600)

    def test_harness_dispatch(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write(tmp, [{"type": "session", "id": "abc"}])
            self.assertEqual(parable.parse_harness_events("pi", p)["session_id"], "abc")
            self.assertIsNone(parable.parse_harness_events("codex", p)["session_id"])


class TestPiResumeArgv(unittest.TestCase):
    def test_reconstruction(self):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        cfg["providers"]["fw"] = dict(PI_PROV)
        cfg["executors"]["minimax"] = dict(PI_EX)
        _, overrides = parable.build_pi_argv(cfg, "minimax", Path("/r"), "sid", Path("/r/plan.md"))
        resume = ["pi", "-p"] + overrides + ["fix it"]
        joined = " ".join(resume)
        self.assertIn("--session-id sid", joined)
        self.assertNotIn("@/r/plan.md", joined)
        self.assertEqual(resume[-1], "fix it")


class TestEnabledFlag(unittest.TestCase):
    def test_disabled_status(self):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        cfg["executors"]["sonnet"]["enabled"] = False
        self.assertEqual(parable.env_key_status(cfg, "sonnet"), "disabled")
        self.assertEqual(parable.env_key_status(cfg, "opus"), "subagent")


class TestResearchConfig(unittest.TestCase):
    def test_default_is_grep_ai(self):
        self.assertEqual(parable.BUILTIN_DEFAULTS["research"]["provider"], "grep.ai")
        self.assertEqual(parable.validate_config(parable.BUILTIN_DEFAULTS), [])

    def test_claude_opt_out_valid(self):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        cfg["research"]["provider"] = "claude"
        self.assertEqual(parable.validate_config(cfg), [])

    def test_unknown_provider_fails_loud(self):
        cfg = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        cfg["research"]["provider"] = "bing"
        problems = parable.validate_config(cfg)
        self.assertTrue(any("[research] provider='bing'" in p for p in problems))

    def test_overlay_overrides_default(self):
        out = parable.merge_configs(parable.BUILTIN_DEFAULTS,
                                    {"research": {"provider": "claude"}})
        self.assertEqual(out["research"]["provider"], "claude")


class TestConfigPrecedence(unittest.TestCase):
    def test_order_lowest_first(self):
        import os as _os
        old = _os.environ.pop("PARABLE_CONFIG", None)
        try:
            paths = parable.config_paths(Path("/repo"))
            self.assertEqual(paths[0], Path.home() / ".config" / "parable" / "parable.toml")
            self.assertEqual(paths[1], Path("/repo/parable.toml"))
            self.assertEqual(paths[2], Path("/repo/.claude/parable.toml"))
            self.assertEqual(len(paths), 3)
            _os.environ["PARABLE_CONFIG"] = "/x/custom.toml"
            paths = parable.config_paths(Path("/repo"))
            self.assertEqual(paths[-1], Path("/x/custom.toml"))  # env wins (loaded last)
        finally:
            _os.environ.pop("PARABLE_CONFIG", None)
            if old is not None:
                _os.environ["PARABLE_CONFIG"] = old

    def test_invalid_toml_fails_loud(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "parable.toml"
            bad.write_text("[parable\nversion = ")
            import os as _os
            old = _os.environ.get("PARABLE_CONFIG")
            _os.environ["PARABLE_CONFIG"] = str(bad)
            try:
                with self.assertRaises(SystemExit) as ctx:
                    parable.load_config(Path(tmp))
                self.assertIn("invalid TOML", str(ctx.exception))
            finally:
                if old is None:
                    _os.environ.pop("PARABLE_CONFIG", None)
                else:
                    _os.environ["PARABLE_CONFIG"] = old


class TestPartitionUntracked(unittest.TestCase):
    def test_no_paths_lists_everything(self):
        include, listed = parable.partition_untracked(["a.py", "b/c.ts"], ".parable", [])
        self.assertEqual(include, [])
        self.assertEqual(listed, ["a.py", "b/c.ts"])

    def test_scoped_includes_only_in_scope(self):
        include, listed = parable.partition_untracked(
            ["src/new.py", "scripts/other.py"], ".parable", ["src"])
        self.assertEqual(include, ["src/new.py"])
        self.assertEqual(listed, ["scripts/other.py"])

    def test_log_dir_fully_hidden(self):
        include, listed = parable.partition_untracked(
            [".parable/runs/x/harness.jsonl", "src/a.py"], ".parable", ["src"])
        self.assertEqual(include, ["src/a.py"])
        self.assertEqual(listed, [])

    def test_secretish_never_included_even_in_scope(self):
        files = ["src/.env.local", "src/service.pem", "src/aws_credentials.json", "src/ok.py"]
        include, listed = parable.partition_untracked(files, ".parable", ["src"])
        self.assertEqual(include, ["src/ok.py"])
        self.assertEqual(sorted(listed), sorted(files[:3]))


class TestMergeFacts(unittest.TestCase):
    def test_aggregates_across_streams(self):
        merged = parable.merge_facts([
            {"session_id": "s1", "turns": 2, "tool_calls": 3, "last_message": "first",
             "errors": ["e1"], "usage": {"input_tokens": 100}, "phase": "complete"},
            {"session_id": None, "turns": 1, "tool_calls": 1, "last_message": "fixed",
             "errors": [], "usage": {"input_tokens": 50}, "phase": "complete"},
        ])
        self.assertEqual(merged["session_id"], "s1")
        self.assertEqual(merged["turns"], 3)
        self.assertEqual(merged["tool_calls"], 4)
        self.assertEqual(merged["last_message"], "fixed")
        self.assertEqual(merged["errors"], ["e1"])
        self.assertEqual(merged["usage"]["input_tokens"], 150)

    def test_empty_resume_keeps_prior_message(self):
        merged = parable.merge_facts([
            {"session_id": "s", "turns": 1, "tool_calls": 0, "last_message": "done",
             "errors": [], "usage": {}, "phase": "complete"},
            {"session_id": None, "turns": 0, "tool_calls": 0, "last_message": "",
             "errors": [], "usage": {}, "phase": "unknown"},
        ])
        self.assertEqual(merged["last_message"], "done")
        self.assertEqual(merged["phase"], "complete")


class TestFailureLines(unittest.TestCase):
    def test_grep_extraction(self):
        check = {"grep": r"error TS\d+"}
        res = {"output": "junk\nsrc/a.ts(3,1): error TS2304: Cannot find name\nmore junk"}
        lines = parable.failure_lines(check, res)
        self.assertEqual(len(lines), 1)
        self.assertIn("TS2304", lines[0])

    def test_tail_fallback(self):
        check = {"tail_lines": 2}
        res = {"output": "a\nb\nc\nd"}
        self.assertEqual(parable.failure_lines(check, res), ["c", "d"])


CURSOR_PROV = {"type": "cursor"}
CURSOR_EX = {"provider": "cur", "model": "composer-2.5", "tags": ["mechanical"]}


class TestCursorValidation(unittest.TestCase):
    def cfg(self):
        c = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        c["providers"]["cur"] = dict(CURSOR_PROV)
        c["executors"]["composer"] = dict(CURSOR_EX)
        return c

    def test_cursor_provider_is_valid(self):
        self.assertEqual(parable.validate_config(self.cfg()), [])

    def test_cursor_rejects_base_url(self):
        c = self.cfg()
        c["providers"]["cur"]["base_url"] = "https://x"
        self.assertTrue(any("type=cursor takes no base_url" in p for p in parable.validate_config(c)))

    def test_cursor_effort_is_advisory_not_gated(self):
        # cursor pins effort in the slug, so any effort string is accepted (no enum gate).
        c = self.cfg()
        c["executors"]["composer"]["effort"] = "high"      # a codex enum value
        self.assertEqual([p for p in parable.validate_config(c) if "effort" in p], [])
        c["executors"]["composer"]["effort"] = "banana"    # not in any enum — still fine for cursor
        self.assertEqual([p for p in parable.validate_config(c) if "effort" in p], [])

    def test_cursor_env_status(self):
        import os as _os
        c = self.cfg()
        old = _os.environ.pop("CURSOR_API_KEY", None)
        try:
            self.assertEqual(parable.env_key_status(c, "composer"), "missing CURSOR_API_KEY")
            _os.environ["CURSOR_API_KEY"] = "x"
            self.assertEqual(parable.env_key_status(c, "composer"), "ready")
        finally:
            _os.environ.pop("CURSOR_API_KEY", None)
            if old is not None:
                _os.environ["CURSOR_API_KEY"] = old


class TestCursorArgv(unittest.TestCase):
    def cfg(self):
        c = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        c["providers"]["cur"] = dict(CURSOR_PROV)
        c["executors"]["composer"] = dict(CURSOR_EX)
        return c

    def test_run_argv_shape(self):
        argv, overrides = parable.build_cursor_argv(self.cfg(), "composer", Path("/w"))
        self.assertEqual(argv[:2], ["cursor-agent", "-p"])
        joined = " ".join(argv)
        self.assertIn("--output-format stream-json", joined)
        self.assertIn("--force", joined)
        self.assertIn("--model composer-2.5", joined)
        self.assertIn("--workspace /w", joined)
        self.assertNotIn("--resume", joined)  # fresh run has no chat id

    def test_resume_replays_overrides_plus_chat(self):
        _, overrides = parable.build_cursor_argv(self.cfg(), "composer", Path("/w"))
        resume = ["cursor-agent", "-p"] + overrides + ["--resume", "chat-77"]
        joined = " ".join(resume)
        self.assertIn("--model composer-2.5", joined)   # model survives the resume
        self.assertIn("--resume chat-77", joined)


class TestCursorEventParsing(unittest.TestCase):
    def write(self, tmp, events):
        p = Path(tmp) / "h.jsonl"
        p.write_text("\n".join(json.dumps(e) for e in events))
        return p

    def test_parse_normalizes_to_codex_keys(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write(tmp, [
                {"type": "system", "subtype": "init", "session_id": "cur-1", "model": "Composer 2.5"},
                {"type": "tool_call", "subtype": "started", "call_id": "t1"},
                {"type": "tool_call", "subtype": "completed", "call_id": "t1"},
                {"type": "assistant", "message": {"role": "assistant",
                    "content": [{"type": "text", "text": "interim"}]}},
                {"type": "result", "subtype": "success", "is_error": False,
                 "result": "Done. Created file.", "session_id": "cur-1",
                 "usage": {"inputTokens": 8014, "outputTokens": 147,
                           "cacheReadTokens": 24388, "cacheWriteTokens": 0}},
            ])
            facts = parable.parse_cursor_events(p)
        self.assertEqual(facts["session_id"], "cur-1")
        self.assertEqual(facts["phase"], "complete")
        self.assertEqual(facts["turns"], 1)
        self.assertEqual(facts["tool_calls"], 1)   # only 'started' counts, not 'completed'
        self.assertEqual(facts["last_message"], "Done. Created file.")  # result text wins over interim
        self.assertEqual(facts["usage"]["input_tokens"], 8014)
        self.assertEqual(facts["usage"]["cached_input_tokens"], 24388)
        self.assertEqual(facts["usage"]["output_tokens"], 147)

    def test_error_result(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write(tmp, [
                {"type": "system", "subtype": "init", "session_id": "e1"},
                {"type": "result", "subtype": "error", "is_error": True,
                 "result": "model refused", "session_id": "e1"},
            ])
            facts = parable.parse_cursor_events(p)
        self.assertEqual(facts["phase"], "error")
        self.assertEqual(facts["errors"], ["model refused"])

    def test_harness_dispatch(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = self.write(tmp, [{"type": "system", "subtype": "init", "session_id": "z"}])
            self.assertEqual(parable.parse_harness_events("cursor", p)["session_id"], "z")


class TestPoolsInConfig(unittest.TestCase):
    def test_maps_provider_types_to_pools(self):
        c = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))  # subagent-only -> claude
        self.assertEqual(parable.pools_in_config(c), ["claude"])
        c["providers"]["oai"] = {"type": "codex-native"}
        c["executors"]["terra"] = {"provider": "oai", "model": "gpt-5.6-terra"}
        c["providers"]["cur"] = dict(CURSOR_PROV)
        c["executors"]["composer"] = dict(CURSOR_EX)
        self.assertEqual(parable.pools_in_config(c), ["claude", "codex", "cursor"])

    def test_metered_providers_have_no_pool(self):
        c = json.loads(json.dumps(parable.BUILTIN_DEFAULTS))
        c["providers"]["fw"] = dict(PI_PROV)
        c["executors"]["minimax"] = dict(PI_EX)
        # pi/codex (API-key) providers are not subscription pools -> only claude remains
        self.assertEqual(parable.pools_in_config(c), ["claude"])


class TestUsageProbe(unittest.TestCase):
    """parable_usage fails soft — a missing credential is 'unknown', never an exception."""
    def setUp(self):
        import importlib, sys as _sys
        _sys.path.insert(0, str(Path(parable.__file__).resolve().parent))
        self.pu = importlib.import_module("parable_usage")

    def test_missing_credentials_are_unknown_not_error(self):
        import os as _os
        saved = {k: _os.environ.pop(k, None) for k in ("CURSOR_API_KEY", "CLAUDE_CONFIG_DIR", "CODEX_HOME")}
        _os.environ["CLAUDE_CONFIG_DIR"] = "/nonexistent-xyz"
        _os.environ["CODEX_HOME"] = "/nonexistent-xyz"
        try:
            # use_cache=False so a real machine's warm cache can't serve a stale good read
            r = {x["pool"]: x for x in self.pu.probe_all(use_cache=False)}
            self.assertEqual(r["claude"]["status"], "unknown")
            self.assertEqual(r["codex"]["status"], "unknown")
            self.assertEqual(r["cursor"]["status"], "unknown")
            # a soft failure still produces a formattable report
            self.assertIn("unknown", self.pu.format_report(list(r.values())))
        finally:
            _os.environ.pop("CLAUDE_CONFIG_DIR", None)
            _os.environ.pop("CODEX_HOME", None)
            for k, v in saved.items():
                if v is not None:
                    _os.environ[k] = v

    def test_worst_used_pct_picks_tightest_window(self):
        report = {"pool": "codex", "status": "ok",
                  "windows": [{"window": "5h", "used_pct": 12.0},
                              {"window": "7d", "used_pct": 88.0}]}
        self.assertEqual(self.pu.worst_used_pct(report), 88.0)
        self.assertIsNone(self.pu.worst_used_pct({"pool": "x", "windows": []}))

    def test_claude_limits_array_surfaces_scoped_bucket(self):
        # The newer limits[] array carries a per-model weekly_scoped bucket that the flat
        # five_hour/seven_day fields omit; claude_windows must prefer it and label by model.
        windows = self.pu.claude_windows({
            "five_hour": {"utilization": 35},
            "seven_day": {"utilization": 25},
            "limits": [
                {"kind": "session", "percent": 35, "severity": "normal"},
                {"kind": "weekly_all", "percent": 25, "severity": "normal"},
                {"kind": "weekly_scoped", "percent": 41, "severity": "normal",
                 "scope": {"model": {"display_name": "Fable"}}},
            ],
        })
        labels = {w["window"]: w["used_pct"] for w in windows}
        self.assertEqual(labels, {"5h": 35.0, "7d": 25.0, "7d-fable": 41.0})
        self.assertEqual(self.pu.worst_used_pct({"windows": windows}), 41.0)

    def test_claude_windows_falls_back_to_flat_fields(self):
        # No limits[] array (older server / other account) -> flat fields still parse.
        windows = self.pu.claude_windows({
            "five_hour": {"utilization": 12, "resets_at": None},
            "seven_day": {"utilization": 60, "resets_at": None},
        })
        self.assertEqual({w["window"]: w["used_pct"] for w in windows},
                         {"5h": 12.0, "7d": 60.0})

    def test_claude_billing_preserves_current_meter_not_fake_week(self):
        billing = self.pu.claude_billing({
            "extra_usage": {"is_enabled": True, "used_credits": 136915,
                            "currency": "USD", "decimal_places": 2,
                            "daily": None, "weekly": None},
            "spend": {"enabled": True,
                      "used": {"amount_minor": 136915, "currency": "USD", "exponent": 2},
                      "limit": None},
        })
        self.assertEqual(billing["used"], 1369.15)
        self.assertEqual(billing["period"], "current")
        self.assertIsNone(billing["daily"])
        self.assertIsNone(billing["weekly"])
        report = self.pu.format_report([
            {"pool": "claude", "status": "ok", "plan": "max", "windows": [],
             "billing": billing},
        ])
        self.assertIn("extra=$1,369.15 current", report)

    def test_codex_billing_preserves_credit_and_spend_controls(self):
        billing = self.pu.codex_billing({
            "credits": {"has_credits": False, "balance": "0", "unlimited": False,
                        "overage_limit_reached": False},
            "spend_control": {"reached": False, "individual_limit": None},
        })
        self.assertFalse(billing["has_credits"])
        self.assertFalse(billing["spend_control_reached"])
        report = self.pu.format_report([
            {"pool": "codex", "status": "ok", "plan": "pro", "windows": [],
             "billing": billing},
        ])
        self.assertIn("credits=none", report)

    def test_codex_window_uses_duration_not_primary_key_name(self):
        windows = self.pu.codex_windows({
            "rate_limit": {
                "primary_window": {"used_percent": 84, "limit_window_seconds": 604800},
                "secondary_window": None,
            },
        })
        self.assertEqual(windows[0]["window"], "7d")
        self.assertEqual(windows[0]["used_pct"], 84.0)

    def test_probes_do_not_drop_provider_billing_fields(self):
        import os as _os
        import tempfile
        original_get = self.pu._get_json
        old_claude = _os.environ.get("CLAUDE_CONFIG_DIR")
        old_codex = _os.environ.get("CODEX_HOME")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / ".credentials.json").write_text(json.dumps({
                    "claudeAiOauth": {"accessToken": "test", "subscriptionType": "max"},
                }))
                (root / "auth.json").write_text(json.dumps({
                    "tokens": {"access_token": "test", "account_id": "acct"},
                }))
                _os.environ["CLAUDE_CONFIG_DIR"] = tmp
                _os.environ["CODEX_HOME"] = tmp

                def fake_get(url, headers, data=None):
                    if "anthropic" in url:
                        return {"extra_usage": {"is_enabled": True, "used_credits": 2500,
                                                "currency": "USD", "decimal_places": 2}}
                    return {"plan_type": "pro", "rate_limit": {},
                            "credits": {"has_credits": True, "balance": "12.5"},
                            "spend_control": {"reached": False}}

                self.pu._get_json = fake_get
                self.assertEqual(self.pu.probe_claude()["billing"]["used"], 25.0)
                self.assertTrue(self.pu.probe_codex()["billing"]["has_credits"])
        finally:
            self.pu._get_json = original_get
            if old_claude is None:
                _os.environ.pop("CLAUDE_CONFIG_DIR", None)
            else:
                _os.environ["CLAUDE_CONFIG_DIR"] = old_claude
            if old_codex is None:
                _os.environ.pop("CODEX_HOME", None)
            else:
                _os.environ["CODEX_HOME"] = old_codex


class TestUsageCache(unittest.TestCase):
    """The disk cache exists because the usage endpoints throttle rapid polling
    (Claude's /api/oauth/usage trips a multi-minute 429). It must (a) reuse a fresh
    read without re-hitting the endpoint and (b) serve the last good read stale when
    a live probe fails, rather than dropping the pool to unknown mid-batch."""
    def setUp(self):
        import importlib, sys as _sys, tempfile, os
        _sys.path.insert(0, str(Path(parable.__file__).resolve().parent))
        self.pu = importlib.import_module("parable_usage")
        # isolate the cache file to this test
        self._orig_path = self.pu._CACHE_PATH
        self.pu._CACHE_PATH = Path(tempfile.mkdtemp()) / "cache.json"
        self._orig_probe = self.pu._probe_one
        self.calls = []

    def tearDown(self):
        self.pu._CACHE_PATH = self._orig_path
        self.pu._probe_one = self._orig_probe

    def _stub(self, results):
        """results: dict name -> report to return; records each call."""
        def stub(name, cursor_env_key):
            self.calls.append(name)
            return results[name]
        self.pu._probe_one = stub

    def test_fresh_entry_reused_within_ttl(self):
        good = {"pool": "codex", "status": "ok", "plan": "pro", "windows": []}
        self._stub({"codex": good})
        self.pu.probe_all(["codex"], ttl=45)          # live hit, populates cache
        self.pu.probe_all(["codex"], ttl=45)          # should reuse cache
        self.assertEqual(self.calls, ["codex"])       # only ONE live probe
        r = self.pu.probe_all(["codex"], ttl=45)[0]
        self.assertTrue(r.get("cached"))

    def test_stale_served_when_live_probe_fails(self):
        good = {"pool": "claude", "status": "ok", "plan": "max", "windows": []}
        self._stub({"claude": good})
        self.pu.probe_all(["claude"], ttl=0)          # ttl=0 always re-probes; seeds cache
        # now the live probe starts failing (simulate HTTP 429)
        self._stub({"claude": self.pu._unknown("claude", "HTTP 429")})
        r = self.pu.probe_all(["claude"], ttl=0)[0]
        self.assertEqual(r["status"], "ok")           # served the prior good read
        self.assertTrue(r.get("cached"))
        self.assertIn("stale_seconds", r)
        self.assertEqual(r["live_probe"], "HTTP 429")

    def test_no_prior_read_stays_unknown(self):
        self._stub({"cursor": self.pu._unknown("cursor", "$CURSOR_API_KEY not set")})
        r = self.pu.probe_all(["cursor"], ttl=0)[0]
        self.assertEqual(r["status"], "unknown")      # nothing to serve stale -> honest unknown


if __name__ == "__main__":
    unittest.main()
