# ── In your admin/__init__.py add these imports ───────────────────────

from . import live_staffs          # core routes: CRUD, AI docs, exports
from . import live_staffs_crons    # cron core: sync-docs, generate-cv/interview/appform/pcc, passport, certs 1
from . import live_staffs_crons2   # cert sync crons part 2: garda → fire-safety
from . import live_staffs_crons3   # cert sync crons part 3: qqi-level5 → end + staff API

# All 4 files register routes on the same admin_bp blueprint.
# Flask merges them automatically — no extra register_blueprint() needed.

# File sizes:
#   live_staffs.py          ~4,900 lines  (core)
#   live_staffs_crons.py    ~5,966 lines  (cron core + cert crons 1)
#   live_staffs_crons2.py   ~5,900 lines  (cert crons 2: garda → fire-safety)
#   live_staffs_crons3.py   ~6,300 lines  (cert crons 3: qqi-level5 → end)
