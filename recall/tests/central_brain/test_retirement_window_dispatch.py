"""Dispatch only changes the proof strategy; both owners keep the same bounds."""
import time
import unittest
from unittest.mock import MagicMock, patch

from server.recall_server import chunk_retirement as owner
from server.recall_server import parent_chunk_window as window


class WindowDispatchTests(unittest.TestCase):
    def arguments(self, records, batch_documents=256):
        return dict(tenant_id="tenant", source_id="source", native_parent_id="parent",
            apply=True, reviewed_plan={"manifest": {"record_count": records}},
            limits=owner.ParentRetirementLimits(batch_documents=batch_documents),
            deadline_at=time.monotonic()+60, required_scope_epoch=7,
            owner_principal_id="principal", should_stop=lambda: False,
            metadata_mode="singleton")

    def test_small_catalog_delegates_all_fences_and_budgets_unchanged(self):
        for records, batch_documents in ((1,256),(100,256),(2,2)):
            with self.subTest(records=records, batch_documents=batch_documents):
                arguments=self.arguments(records,batch_documents)
                store,archive=MagicMock(),MagicMock()
                expected={"status":"partial","complete":False}
                with patch.object(owner,"retire_parent_chunks",return_value=expected) as full, \
                     patch.object(window,"retire_window") as subset:
                    actual=window.retire_parent_windows(store,archive,**arguments)
                self.assertIs(actual,expected)
                full.assert_called_once_with(store,archive,**arguments)
                subset.assert_not_called()
                store.connect.assert_not_called()

    def test_large_and_over_batch_catalogs_keep_window_path(self):
        for records,batch_documents in ((101,256),(50_000,256),(3,2)):
            with self.subTest(records=records,batch_documents=batch_documents):
                arguments=self.arguments(records,batch_documents)
                with patch.object(owner,"retire_parent_chunks") as full, \
                     patch.object(window,"retire_window",return_value={"status":"stopped","complete":False}) as subset:
                    actual=window.retire_parent_windows(MagicMock(),MagicMock(),**arguments)
                self.assertEqual(actual["status"],"stopped")
                full.assert_not_called()
                subset.assert_called_once()
                self.assertEqual(subset.call_args.kwargs["deadline_at"],arguments["deadline_at"])
                self.assertEqual(subset.call_args.kwargs["required_scope_epoch"],7)
                self.assertEqual(subset.call_args.kwargs["limits"],arguments["limits"])


if __name__=="__main__":
    unittest.main()
