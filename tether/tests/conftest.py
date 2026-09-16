"""Suite-wide isolation: the test process may itself run inside a Herdr pane on a machine
with a live Herdr session and a Codex daemon. Nothing here may reach either."""

import os

os.environ.setdefault("TETHER_HERDR", "off")
