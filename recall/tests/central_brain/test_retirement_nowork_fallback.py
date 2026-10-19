"""An optional probe timeout is not permission to retry an uncertain commit."""
import os
import time
import unittest
from unittest.mock import MagicMock, patch

from server.recall_server import parent_chunk_window as owner
from server.recall_server.chunk_retirement import ChunkRetirementError, ParentRetirementLimits
from server.recall_server.db import SearchDeadlineExceeded


class NoWorkFallbackGuards(unittest.TestCase):
    def test_refusals_unknown_commit_and_expired_parent_do_not_start_archive_proof(self):
        cases = (
            ('no_work_sql', ChunkRetirementError('parent_retirement_window_forgotten'), False),
            ('no_work_sql', ChunkRetirementError('parent_retirement_owner_required'), False),
            ('no_work_sql', ConnectionError('unknown'), False),
            ('no_work_commit_ack', SearchDeadlineExceeded('unknown commit'), False),
            ('no_work_sql', SearchDeadlineExceeded('parent expired'), True),
        )
        for phase, error, expired in cases:
            with self.subTest(phase=phase, error=type(error).__name__, expired=expired):
                store = MagicMock(search_plane='turbopuffer', verified_singleton_receipts=True)
                deadline = time.monotonic() + (-1 if expired else 30)
                def refuse(*args, **kwargs):
                    kwargs['diagnostic']['phase'] = phase
                    raise error
                with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'), \
                     patch.object(owner, '_advance_without_physical_work', side_effect=refuse), \
                     patch.object(owner, 'prepare_window') as proof:
                    with self.assertRaises(type(error)) as raised:
                        owner.retire_window(store, MagicMock(), scope=('t', 's', 'p'),
                            principal_id='owner', limits=ParentRetirementLimits(),
                            deadline_at=deadline, reviewed_plan={}, metadata_mode='singleton',
                            diagnostic={})
                self.assertIs(raised.exception, error)
                proof.assert_not_called()


if __name__ == '__main__':
    unittest.main()
