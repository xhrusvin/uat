# =============================================================================
#  OPTIMISED /shift-users/list-multi
#
#  HOW TO APPLY
#  ------------
#  1. Add the imports in section A to the top of your module.
#  2. Paste section B (helpers) anywhere ABOVE the list-multi endpoint
#     (e.g. right after `_get_user_exclusion_tags`).
#  3. Replace the existing `list_shift_users_multi` function (section C) with
#     the one below. Its signature / request model / response keys are
#     unchanged, so `/list-multi/export` keeps working untouched.
#
#  Everything else in the file (/list, /assign, /confirm, ...) is unchanged.
# =============================================================================


# ── A. extra imports (top of file) ───────────────────────────────────────────
import time
from datetime import timedelta
import httpx


# ── B. helpers ───────────────────────────────────────────────────────────────

# Upstream available-staff-list cache: paging through results re-calls the
# upstream API on every request; a short TTL removes that repeated cost.
_UPSTREAM_TTL = 60                     # seconds
_UPSTREAM_CACHE: dict = {}             # xn_shift_id -> (monotonic_ts, data)

# Cache of distinct Enabled designations, lets us replace the slow
# case-insensitive anchored $regex with an index-friendly exact $in.
_DESIGNATION_TTL = 300                 # seconds
_DESIGNATION_MIN_REFRESH = 30          # seconds, floor between "missing type" refreshes
_DESIGNATION_CACHE: dict = {"at": 0.0, "values": []}

# Exclusion batch size (users per set of batched queries)
_EXCL_BATCH_SIZE = 200

# Keeps references to fire-and-forget tasks so they are not garbage collected
_BG_TASKS: set = set()

_EXCLUDED_TAG_NAMES = {
    "last-resort booking",
    "avoid booking",
    "temporarily unavailable",
    "no calls or emails",
    "no bulk emails",
    "no calls",
    "direct bookings only",
    "contact on request only",
}
_BAD_DOC_STATUSES = ["expired", "pending", "not_approved"]


def _spawn_bg(coro) -> None:
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


async def _fetch_upstream_staff(xn_shift_ids: list) -> dict:
    """
    available-staff-list for several upstream shift ids, in parallel, with a
    short in-process TTL cache. Returns {xn_shift_id: [staff, ...]}.
    """
    now = time.monotonic()
    result: dict = {}
    todo: list = []

    for x in xn_shift_ids:
        hit = _UPSTREAM_CACHE.get(x)
        if hit and now - hit[0] < _UPSTREAM_TTL:
            result[x] = hit[1]
        else:
            todo.append(x)

    if not todo:
        return result

    url = f"{settings.SHIFT_URL.rstrip('/')}/ai/shifts/available-staff-list"
    headers = {
        "Api-Key":      settings.SHIFT_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0)) as client:
        async def _one(x):
            try:
                r = await client.post(url, json={"shift_id": x}, headers=headers)
                if r.status_code == 200:
                    data = r.json().get("data") or []
                    _UPSTREAM_CACHE[x] = (time.monotonic(), data)
                    return x, data
            except Exception as e:
                logger.warning(f"[list-multi] upstream {x}: {e}")
            return x, []

        for x, data in await asyncio.gather(*[_one(x) for x in todo]):
            result[x] = data

    # keep the cache bounded
    if len(_UPSTREAM_CACHE) > 500:
        cutoff = time.monotonic() - _UPSTREAM_TTL
        for k in [k for k, v in _UPSTREAM_CACHE.items() if v[0] < cutoff]:
            _UPSTREAM_CACHE.pop(k, None)

    return result


async def _resolve_designations(db, types: list) -> list:
    """
    Real `users.designation` values that equal any of `types`
    (case-insensitive, exact) — same semantics as the old ^type$ /i regex,
    but the query becomes a plain $in that can use an index.
    """
    wanted = {str(t).strip().lower() for t in types if t}
    if not wanted:
        return []

    async def _load():
        vals = await db["users"].distinct("designation", {"status": "Enabled"})
        _DESIGNATION_CACHE["values"] = [v for v in vals if isinstance(v, str)]
        _DESIGNATION_CACHE["at"] = time.monotonic()

    if time.monotonic() - _DESIGNATION_CACHE["at"] >= _DESIGNATION_TTL:
        await _load()

    def _match():
        return [v for v in _DESIGNATION_CACHE["values"] if v.lower() in wanted]

    matched = _match()
    # A type with no match may be a brand-new designation, but refresh at most
    # once per _DESIGNATION_MIN_REFRESH seconds (never on every request).
    if ({m.lower() for m in matched} != wanted
            and time.monotonic() - _DESIGNATION_CACHE["at"] >= _DESIGNATION_MIN_REFRESH):
        await _load()
        matched = _match()
    return matched


def _static_exclusion_tags(u: dict, target_shift: dict) -> list:
    """Steps 0 and 0b of the original exclusion logic (pure, no DB)."""
    tags: list = []

    for tag in (u.get("tags") or []):
        tag_name = (tag.get("name", "") if isinstance(tag, dict) else str(tag)).lower().strip()
        if tag_name in _EXCLUDED_TAG_NAMES:
            tags.append(f"tag:{tag.get('name', tag_name) if isinstance(tag, dict) else tag_name}")
        if tag_name == "premium shifts only" and not target_shift.get("is_premium"):
            tags.append("tag:Premium Shifts Only")

    banned = u.get("banned_clients") or []
    if banned:
        shift_client_id = str(target_shift.get("client_id", ""))
        for bc in banned:
            bc_id = str(bc.get("id", "")) if isinstance(bc, dict) else str(bc)
            if bc_id and bc_id == shift_client_id:
                tags.insert(0, "Client Banned Staff")
                break
    return tags


def _has_gap_conflict(existing_shifts: list, target_date, target_start: str, target_end: str) -> bool:
    """Rule 6: same-calendar-day gap (>0 and <120 min) to an existing shift."""
    for es in existing_shifts:
        es_slots = es.get("slots") or []
        time_ranges = []
        if es_slots:
            for sl in es_slots:
                time_ranges.append({
                    "date":  sl.get("date"),
                    "start": sl.get("start_time", ""),
                    "end":   sl.get("end_time", ""),
                })
        else:
            time_ranges.append({
                "date":  es.get("date"),
                "start": es.get("start_time", ""),
                "end":   es.get("end_time", ""),
            })

        for tr in time_ranges:
            tr_date, tr_start, tr_end = tr["date"], tr["start"], tr["end"]
            if not tr_date or not target_date:
                continue
            try:
                tr_day  = tr_date.date() if hasattr(tr_date, "date") else None
                tgt_day = target_date.date() if hasattr(target_date, "date") else None
                if not tr_day or not tgt_day or tr_day != tgt_day:
                    continue
            except Exception:
                continue
            if not tr_start or not tr_end:
                continue

            gap1 = _gap_minutes(tr_end, target_start)
            gap2 = _gap_minutes(target_end, tr_start)
            if (0 < gap1 < 120) or (0 < gap2 < 120):
                return True
    return False


async def _batch_exclusion_tags(db, users: list, target_shift: dict) -> dict:
    """
    Same verdicts as `_get_user_exclusion_tags`, but for MANY users with a
    constant number of queries (1 for level-1 docs + 1 for existing shifts)
    instead of 2 queries per user.

    Returns {user._id: [tags]}.
    """
    out: dict = {}
    pending: list = []

    # 0 / 0b — pure python
    for u in users:
        if not u.get("email"):
            out[u["_id"]] = []
            continue
        tags = _static_exclusion_tags(u, target_shift)
        if tags:
            out[u["_id"]] = tags          # original returns early here
        else:
            pending.append(u)

    if not pending:
        return out

    # 0c — Level-1 documents, one query for everybody
    oids = [u["_id"] for u in pending]
    bad_doc: dict = {}
    async for d in db["documents_new"].find(
        {"user_id": {"$in": oids}, "level": 1, "status": {"$in": _BAD_DOC_STATUSES}},
        {"user_id": 1, "status": 1},
    ):
        bad_doc.setdefault(d["user_id"], d.get("status", "invalid"))

    still: list = []
    for u in pending:
        status = bad_doc.get(u["_id"])
        if status:
            out[u["_id"]] = [f"level1_doc_{status}"]
        else:
            still.append(u)

    if not still:
        return out

    # Rule 6 — needs a fully specified target shift
    target_date  = target_shift.get("date")
    target_start = target_shift.get("start_time", "")
    target_end   = target_shift.get("end_time", "")
    if not target_date or not target_start or not target_end:
        for u in still:
            out[u["_id"]] = []
        return out

    try:
        if hasattr(target_date, "date"):
            td = target_date
        else:
            td = datetime.strptime(str(target_date)[:10], "%Y-%m-%d")
        date_filter = {"$gte": td - timedelta(days=1), "$lte": td + timedelta(days=1)}
    except Exception:
        for u in still:
            out[u["_id"]] = []
        return out

    emails = list({u["email"] for u in still})
    by_email: dict = {}
    async for s in db["shifts"].find(
        {"staff_email": {"$in": emails}, "date": date_filter, "upstream_status": "Upcoming"},
        {"staff_email": 1, "date": 1, "start_time": 1, "end_time": 1, "slots": 1},
    ):
        by_email.setdefault(s.get("staff_email"), []).append(s)

    for u in still:
        existing = by_email.get(u["email"], [])
        if existing and _has_gap_conflict(existing, target_date, target_start, target_end):
            out[u["_id"]] = ["under_6h_gap"]
        else:
            out[u["_id"]] = []
    return out


async def _noop():
    return None


# ── C. replacement endpoint ──────────────────────────────────────────────────

@router.post(
    "/list-multi",
    summary="List users for multiple shifts with same enrichment as /list",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def list_shift_users_multi(request: Request, payload: ListMultiShiftUsersRequest):
    """
    Body: { "shift_ids": ["<id1>", "<id2>", ...], "page": 1, "per_page": 20, ... }

    Candidates are Enabled users whose designation matches any of the shifts'
    user_type values (exact, case-insensitive), narrowed to the union of the
    upstream available-staff-lists.

    Exclusion is evaluated against the FIRST shift_id (the primary).

    Performance notes
    -----------------
    * independent look-ups run concurrently (asyncio.gather)
    * upstream staff lists are cached for 60 s
    * exclusion is batched (constant #queries per chunk, not per user)
    * during a filtered/sorted walk only the fields that the filter/sort
      needs are computed; everything else is computed for the final page only
    """
    db = _get_db()

    if not payload.shift_ids:
        raise HTTPException(status_code=400, detail="shift_ids must not be empty")

    shift_oids = []
    for sid in payload.shift_ids:
        if not ObjectId.is_valid(str(sid)):
            raise HTTPException(status_code=422, detail=f"Invalid shift_id: {sid}")
        shift_oids.append(ObjectId(str(sid)))

    primary_oid = shift_oids[0]
    per_page    = min(max(int(payload.per_page or 1), 1), _MAX_SCAN)
    skip        = max(0, (payload.page - 1) * per_page)
    limit       = per_page

    order_by = payload.order_by or "name"
    reverse  = (payload.sort or "asc").lower() == "desc"

    # ── Shifts — ONE query (also gives us the primary/target shift) ──────────
    shift_docs = await db["shifts"].find(
        {"_id": {"$in": shift_oids}},
        {"user_type": 1, "requested_staff_list": 1, "shift_id": 1,
         "date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1,
         "shift_type": 1, "slots": 1, "client_id": 1, "is_premium": 1}
    ).to_list(length=100)

    if not shift_docs:
        raise HTTPException(status_code=404, detail="No shifts found for the given shift_ids")

    shift_user_types = list({s["user_type"] for s in shift_docs if s.get("user_type")})
    target_shift = next((s for s in shift_docs if s["_id"] == primary_oid), None) or {}
    xn_ids = [s.get("shift_id") for s in shift_docs if s.get("shift_id")]

    # ── Independent look-ups, concurrently ───────────────────────────────────
    async def _group_ids():
        if payload.group_id and ObjectId.is_valid(str(payload.group_id)):
            return [ObjectId(str(payload.group_id))]
        return [g["_id"] async for g in
                db["shifts_group"].find({"shift_ids": {"$in": shift_oids}}, {"_id": 1})]

    designations_coro = (
        _resolve_designations(db, shift_user_types)
        if (not payload.user_type_multiple and shift_user_types) else _noop()
    )

    try:
        upstream_by_shift, client_data, group_oids, matched_designations = await asyncio.gather(
            _fetch_upstream_staff(xn_ids) if xn_ids else _noop(),
            _get_shift_client_coords(db, primary_oid),
            _group_ids(),
            designations_coro,
        )
    except Exception as _e:
        logger.warning(f"[list-multi] pre-fetch failed: {_e}")
        raise

    upstream_xn_ids:       set  = set()
    upstream_distance_map: dict = {}
    for batch in (upstream_by_shift or {}).values():
        for s in batch:
            uid = str(s.get("id", ""))
            if uid:
                upstream_xn_ids.add(uid)
                if s.get("staff_shift_distance") is not None:
                    upstream_distance_map[uid] = s.get("staff_shift_distance")

    client_coords = (client_data["latitude"], client_data["longitude"]) if client_data else None
    client_location = {
        "latitude":  client_coords[0],
        "longitude": client_coords[1],
    } if client_coords else None

    # ── Build the Mongo filter as $and clauses ───────────────────────────────
    clauses: list = [{"status": "Enabled"}]

    # shifts.user_type == users.designation (exact, case-insensitive)
    if not payload.user_type_multiple and shift_user_types:
        clauses.append({"designation": {"$in": matched_designations or []}})

    if upstream_xn_ids:
        clauses.append({"xn_user_id": {"$in": list(upstream_xn_ids)}})

    if payload.gender_multiple:
        _gids = [str(g).strip() for g in payload.gender_multiple if g]
        if _gids:
            clauses.append({"gender_id": {"$in": _gids}})
    elif payload.gender_id:
        clauses.append({"gender_id": payload.gender_id.strip()})

    if payload.visa_type_id:
        clauses.append({"visa_type_id": payload.visa_type_id.strip()})

    if payload.qqi_status_number is not None:
        clauses.append({"qqi_status_number": payload.qqi_status_number})

    if payload.user_sub_type_multiple:
        sub_oids_m = [ObjectId(str(i)) for i in payload.user_sub_type_multiple
                      if ObjectId.is_valid(str(i))]
        if sub_oids_m:
            clauses.append({"user_sub_type_oids": {"$in": sub_oids_m}})

    if payload.county_multiple:
        county_values = []
        for c in payload.county_multiple:
            c_str = str(c)
            county_values.append(c_str)
            if ObjectId.is_valid(c_str):
                county_values.append(ObjectId(c_str))
        if county_values:
            clauses.append({"county_id": {"$in": county_values}})

    if payload.user_type_multiple:
        valid_type_oids = [ObjectId(str(t)) for t in payload.user_type_multiple
                           if ObjectId.is_valid(str(t))]
        if valid_type_oids:
            type_names = []
            async for ut in db["user_types"].find({"_id": {"$in": valid_type_oids}}, {"name": 1}):
                if ut.get("name"):
                    type_names.append(ut["name"])
            clauses.append({"$or": [
                {"user_type_id": {"$in": valid_type_oids}},
                {"designation":  {"$in": type_names}},
            ]})

    if payload.search and payload.search.strip():
        _s    = payload.search.strip()
        _rx   = re.escape(_s)
        parts = _s.split()
        conds = [
            {"first_name": {"$regex": _rx, "$options": "i"}},
            {"last_name":  {"$regex": _rx, "$options": "i"}},
            {"email":      {"$regex": _rx, "$options": "i"}},
            {"phone":      {"$regex": _rx, "$options": "i"}},
        ]
        if len(parts) >= 2:
            conds.append({"$and": [
                {"first_name": {"$regex": re.escape(parts[0]),  "$options": "i"}},
                {"last_name":  {"$regex": re.escape(parts[-1]), "$options": "i"}},
            ]})
        clauses.append({"$or": conds})

    user_filter = clauses[0] if len(clauses) == 1 else {"$and": clauses}

    # ── by_designation filter (ignores user filters) ─────────────────────────
    desig_clauses: list = [{"status": "Enabled"}]
    if shift_user_types:
        desig_clauses.append({"designation": {"$in": shift_user_types}})
    if upstream_xn_ids:
        desig_clauses.append({"xn_user_id": {"$in": list(upstream_xn_ids)}})
    desig_filter = desig_clauses[0] if len(desig_clauses) == 1 else {"$and": desig_clauses}

    async def _by_designation():
        rows = []
        async for row in db["users"].aggregate([
            {"$match": desig_filter},
            {"$group": {
                "_id":          {"$ifNull": ["$designation", "Unknown"]},
                "user_type_id": {"$first": "$user_type_id"},
                "count":        {"$sum": 1},
            }},
            {"$sort": {"count": -1}},
        ]):
            rows.append(row)
        return rows

    # count + by_designation are independent → run together
    db_total, desig_rows = await asyncio.gather(
        db["users"].count_documents(user_filter),
        _by_designation(),
    )

    # ── Exclusion cache (per user, per PRIMARY shift) ────────────────────────
    _cache_writes: list = []
    _shift_key    = str(primary_oid)
    _now          = datetime.now(timezone.utc)
    _sem          = asyncio.Semaphore(4)       # concurrent exclusion batches

    def _cached_tags(u: dict):
        entry = (u.get("exclusion_cache_by_shift") or {}).get(_shift_key)
        if not isinstance(entry, dict):
            return None
        at = entry.get("at")
        if at is None:
            return None
        if getattr(at, "tzinfo", None) is None:
            at = at.replace(tzinfo=timezone.utc)
        if (_now - at).total_seconds() > _EXCLUSION_CACHE_TTL_SECONDS:
            return None
        tags = entry.get("tags")
        return tags if isinstance(tags, list) else None

    async def _excl_batch(batch: list) -> dict:
        async with _sem:
            return await _batch_exclusion_tags(db, batch, target_shift)

    async def _exclusion_map(chunk: list) -> dict:
        res: dict = {}
        to_compute: list = []
        for u in chunk:
            cached = _cached_tags(u)
            if cached is not None:
                res[u["_id"]] = cached
            elif not (u.get("email") and target_shift):
                res[u["_id"]] = []
            else:
                to_compute.append(u)

        if to_compute:
            batches = [to_compute[i:i + _EXCL_BATCH_SIZE]
                       for i in range(0, len(to_compute), _EXCL_BATCH_SIZE)]
            for part in await asyncio.gather(*[_excl_batch(b) for b in batches]):
                res.update(part)
            for u in to_compute:
                _cache_writes.append(UpdateOne(
                    {"_id": u["_id"]},
                    {"$set": {f"exclusion_cache_by_shift.{_shift_key}":
                              {"tags": res.get(u["_id"], []), "at": _now}}},
                ))
        return res

    async def _pool_ids(oids: list) -> set:
        ids = {
            str(p["user_id"])
            async for p in db["shifts_pool"].find(
                {"shift_id": {"$in": shift_oids}, "user_id": {"$in": oids}},
                {"user_id": 1}
            )
        }
        if group_oids:
            async for gp in db["shifts_group_pool"].find(
                {"group_id": {"$in": group_oids}, "user_id": {"$in": oids}},
                {"user_id": 1}
            ):
                ids.add(str(gp["user_id"]))
        return ids

    async def _last_contacted(oids: list) -> dict:
        """Latest call per user, resolved server-side ($group/$first)."""
        lc_map: dict = {}

        async for r in db["shifts_users"].aggregate([
            {"$match": {"user_id": {"$in": oids},
                        "shift_id": {"$in": shift_oids},
                        "call_processed_at": {"$ne": None}}},
            {"$sort": {"call_processed_at": -1}},
            {"$group": {"_id": "$user_id",
                        "dt": {"$first": "$call_processed_at"},
                        "channel": {"$first": "$channel"}}},
        ], allowDiskUse=True):
            lc_map[str(r["_id"])] = (r.get("dt"), r.get("channel") or "")

        gu_match = {"user_id": {"$in": oids}, "call_processed_at": {"$ne": None}}
        if group_oids:
            gu_match["group_id"] = {"$in": group_oids}
        async for r in db["shifts_group_users"].aggregate([
            {"$match": gu_match},
            {"$sort": {"call_processed_at": -1}},
            {"$group": {"_id": "$user_id",
                        "dt": {"$first": "$call_processed_at"},
                        "channel": {"$first": "$channel"}}},
        ], allowDiskUse=True):
            uid = str(r["_id"])
            dt  = r.get("dt")
            cur = lc_map.get(uid)
            if cur is None or (dt and cur[0] and dt > cur[0]):
                lc_map[uid] = (dt, r.get("channel") or "")
        return lc_map

    async def _annotate(chunk: list, *, excl: bool, pool: bool, lc: bool) -> None:
        """
        Always sets distance. Exclusion / pool / last-contacted are computed
        only when requested, so a walk only pays for what its filter or sort
        really needs.
        """
        if not chunk:
            return
        oids = [u["_id"] for u in chunk]

        excl_map, pool_ids, lc_map = await asyncio.gather(
            _exclusion_map(chunk) if excl else _noop(),
            _pool_ids(oids)       if pool else _noop(),
            _last_contacted(oids) if lc   else _noop(),
        )

        for u in chunk:
            uid_str = str(u["_id"])
            if excl:
                tags = excl_map.get(u["_id"], [])
                u["_excl_tags"] = tags
                u["_excluded"]  = 1 if tags else 0
            if pool:
                u["_in_pool"] = 1 if uid_str in pool_ids else 0
            if lc:
                u["_lc"] = lc_map.get(uid_str)

            ucoords = _user_location_coords(u)
            dist = None
            if client_coords and ucoords:
                dist = _haversine_km(client_coords[0], client_coords[1],
                                     ucoords[0], ucoords[1])
            xn = str(u.get("xn_user_id", ""))
            if xn and upstream_distance_map.get(xn) is not None:
                dist = upstream_distance_map[xn]
            u["_coords"]      = ucoords
            u["_distance_km"] = dist

    def _keep(u: dict) -> bool:
        if payload.excluded is not None and u.get("_excluded", 0) != payload.excluded:
            return False
        if payload.in_pool is not None and u.get("_in_pool", 0) != payload.in_pool:
            return False
        if payload.radius is not None and client_coords:
            d = u.get("_distance_km")
            if d is not None and d > payload.radius:
                return False
        return True

    # ── Selection ────────────────────────────────────────────────────────────
    _has_local_filter = (payload.excluded is not None
                         or payload.in_pool is not None
                         or payload.radius is not None)
    _local_sort = order_by in ("distance_km", "rating", "last_contacted")

    # what the walk itself needs (everything else is done for the page only)
    walk_excl = payload.excluded is not None
    walk_pool = payload.in_pool is not None
    walk_lc   = order_by == "last_contacted"

    # _id as tie-breaker → stable, repeatable pagination
    db_sort        = [("first_name", -1 if (order_by == "name" and reverse) else 1), ("_id", 1)]
    scan_truncated = False

    # With several designations ($in) Mongo cannot return rows in index order,
    # so sorting full documents is slow / can exceed the in-memory sort limit.
    # Sort on a tiny projection first, then load full docs for those ids only.
    _multi_desig = len(matched_designations or []) > 1 or bool(payload.user_type_multiple)

    async def _fetch_sorted(offset: int, take: int) -> list:
        if take <= 0:
            return []
        if not _multi_desig:
            return await db["users"].find(user_filter, _LIST_PROJECTION) \
                .sort(db_sort).skip(offset).limit(take).to_list(length=take)
        ids = [d["_id"] async for d in
               db["users"].find(user_filter, {"first_name": 1})
                 .sort(db_sort).allow_disk_use(True).skip(offset).limit(take)]
        if not ids:
            return []
        docs = {d["_id"]: d async for d in
                db["users"].find({"_id": {"$in": ids}}, _LIST_PROJECTION)}
        return [docs[i] for i in ids if i in docs]

    if not _has_local_filter and not _local_sort:
        # Fast path: DB pagination is exact.
        users = await _fetch_sorted(skip, limit)
        filtered_total = db_total
        scanned        = len(users)
    else:
        need     = skip + limit
        scan_cap = min(_MAX_SCAN, 3000 if _local_sort else max(need * 20, 500))
        # A local sort needs every candidate anyway → one query, no repeated skip().
        chunk_size = scan_cap if _local_sort else max(min(limit, 200) * 5, 100)

        matched: list = []
        scanned = offset = 0

        while scanned < scan_cap:
            take  = min(chunk_size, scan_cap - scanned)
            chunk = await _fetch_sorted(offset, take)
            if not chunk:
                break
            offset  += len(chunk)
            scanned += len(chunk)

            await _annotate(chunk, excl=walk_excl, pool=walk_pool, lc=walk_lc)
            matched.extend([u for u in chunk if _keep(u)])

            if not _local_sort and len(matched) >= need:
                break

        scan_truncated = scanned >= scan_cap and scanned < db_total

        if order_by == "distance_km":
            matched.sort(
                key=lambda u: u.get("_distance_km") if u.get("_distance_km") is not None
                else float("inf"),
                reverse=reverse,
            )
        elif order_by == "rating":
            matched.sort(key=lambda u: u.get("rating") or 0, reverse=reverse)
        elif order_by == "last_contacted":
            _epoch = datetime.min.replace(tzinfo=timezone.utc)

            def _lc_key(u):
                lc = u.get("_lc")
                if not lc or not lc[0]:
                    return _epoch
                dt = lc[0]
                if getattr(dt, "tzinfo", None) is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt

            matched.sort(key=_lc_key, reverse=reverse)

        filtered_total = len(matched)
        users          = matched[skip: skip + limit]

    # Fill in whatever the walk did not need — for the page only
    await _annotate(users, excl=not walk_excl, pool=not walk_pool, lc=not walk_lc)

    logger.info(
        f"[list-multi] shifts={len(shift_oids)} types={shift_user_types} "
        f"db_total={db_total} scanned={scanned} matched={filtered_total} page={len(users)}"
    )

    # ══════════════════════════════════════════════════════════════════════════
    #  Everything below touches the current page only.
    # ══════════════════════════════════════════════════════════════════════════
    user_oids_page = [u["_id"] for u in users]

    requested_user_ids: set = {
        str(rs.get("staff_id", ""))
        for s in shift_docs
        for rs in (s.get("requested_staff_list") or [])
        if rs.get("staff_id")
    }

    # ── Name look-ups, all concurrently ──────────────────────────────────────
    all_sub_oids = [
        ObjectId(str(oid))
        for u in users
        for oid in (u.get("user_sub_type_oids") or [])
        if oid and ObjectId.is_valid(str(oid))
    ]
    _vt_oids = list({ObjectId(str(u["visa_type_id"])) for u in users
                     if u.get("visa_type_id") and ObjectId.is_valid(str(u["visa_type_id"]))})

    users_needing_county = [u for u in users if not u.get("county_id") and u.get("country_id")]
    users_needing_type   = [u for u in users if not u.get("user_type_id") and u.get("designation")]

    country_oids = list({ObjectId(str(u["country_id"])) for u in users_needing_county
                         if ObjectId.is_valid(str(u["country_id"]))})
    existing_county_oids = list({
        ObjectId(str(u["county_id"])) for u in users
        if u.get("county_id") and ObjectId.is_valid(str(u["county_id"]))
    })
    all_county_oids = list(set(country_oids) | set(existing_county_oids))

    designations = list({u["designation"] for u in users_needing_type if u.get("designation")})
    existing_type_oids = list({
        ObjectId(str(u["user_type_id"])) for u in users
        if u.get("user_type_id") and ObjectId.is_valid(str(u["user_type_id"]))
    })

    async def _name_map(coll: str, oids: list) -> dict:
        if not oids:
            return {}
        return {str(d["_id"]): d.get("name", "")
                async for d in db[coll].find({"_id": {"$in": oids}}, {"name": 1})}

    async def _designation_ids() -> dict:
        if not designations:
            return {}
        return {ut["name"]: str(ut["_id"])
                async for ut in db["user_types"].find({"name": {"$in": designations}},
                                                      {"_id": 1, "name": 1})}

    async def _prior_shifts() -> dict:
        if not user_oids_page:
            return {}
        res = {}
        async for ps in db["shifts_users"].aggregate([
            {"$match": {"user_id": {"$in": user_oids_page}, "availability": 1}},
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        ]):
            res[str(ps["_id"])] = ps["count"]
        return res

    (sub_type_name_map, visa_type_name_map, county_oid_to_name,
     type_id_to_name, designation_to_type_id, prior_shifts_map) = await asyncio.gather(
        _name_map("user_sub_types", all_sub_oids),
        _name_map("visa_types", _vt_oids),
        _name_map("county", all_county_oids),
        _name_map("user_types", existing_type_oids),
        _designation_ids(),
        _prior_shifts(),
    )

    # country_id → county mapping (only for ids that actually exist in `county`)
    county_name_to_id: dict = {str(o): str(o) for o in country_oids
                               if str(o) in county_oid_to_name}

    # ── Build response rows ──────────────────────────────────────────────────
    results = []
    _backfill: list = []

    for u in users:
        uid_str = str(u["_id"])
        ucoords = u.get("_coords")

        staff_tags = [
            {"id": str(t.get("id", "")), "name": t.get("name", "")} if isinstance(t, dict)
            else {"id": "", "name": str(t)}
            for t in (u.get("tags") or [])
        ]

        last_contacted = None
        lc_entry = u.get("_lc")
        if lc_entry:
            lc_dt, lc_channel = lc_entry
            last_contacted = _format_time_ago(lc_dt)
            if last_contacted and lc_channel:
                last_contacted = f"{last_contacted} · {lc_channel}"

        # county
        county_id = county_name = None
        if u.get("county_id"):
            county_id   = str(u["county_id"])
            county_name = county_oid_to_name.get(county_id)
        elif u.get("country_id"):
            cid_str = str(u["country_id"])
            if cid_str in county_name_to_id:
                county_id   = county_name_to_id[cid_str]
                county_name = county_oid_to_name.get(county_id)
                _backfill.append(UpdateOne(
                    {"_id": u["_id"]}, {"$set": {"county_id": ObjectId(county_id)}}
                ))

        # user type — designation is the source of truth for the display name
        user_type_id = user_type_name = None
        if u.get("designation"):
            user_type_name = u["designation"]
            if u["designation"] in designation_to_type_id:
                user_type_id = designation_to_type_id[u["designation"]]
                if not u.get("user_type_id") or str(u["user_type_id"]) != user_type_id:
                    _backfill.append(UpdateOne(
                        {"_id": u["_id"]}, {"$set": {"user_type_id": ObjectId(user_type_id)}}
                    ))
            elif u.get("user_type_id"):
                user_type_id = str(u["user_type_id"])
        elif u.get("user_type_id"):
            user_type_id   = str(u["user_type_id"])
            user_type_name = type_id_to_name.get(user_type_id)

        # visa hours
        visa_used  = u.get("visa_hours_used")
        visa_total = u.get("visa_hours_total")
        consumed   = u.get("consumed_hours")
        visa_hours_remaining = consumed if consumed is not None else (
            f"{visa_used}/{visa_total}" if visa_used is not None and visa_total else None
        )

        # work history
        prior_shifts = prior_shifts_map.get(uid_str, 0)
        _plural      = "s" if prior_shifts != 1 else ""
        if prior_shifts > 0 and last_contacted:
            work_history = f"{prior_shifts} Shift{_plural} · {last_contacted}"
        elif prior_shifts > 0:
            work_history = f"{prior_shifts} Shift{_plural}"
        elif last_contacted:
            work_history = last_contacted
        else:
            work_history = None

        sub_oids = u.get("user_sub_type_oids") or []
        results.append({
            "id":                    uid_str,
            "xn_user_id":            u.get("xn_user_id"),
            "name":                  " ".join(filter(None, [u.get("first_name", ""),
                                                            u.get("last_name", "")])).strip() or "—",
            "email":                 u.get("email"),
            "phone":                 u.get("phone"),
            "designation":           u.get("designation"),
            "rating":                u.get("rating"),
            "channel":               "Phone",
            "staff_tags":            staff_tags,
            "last_contacted":        last_contacted,
            "visa_hours_remaining":  visa_hours_remaining,
            "work_permit_exemption": u.get("work_permit_exemption"),
            "consumed_hours":        u.get("consumed_hours"),
            "qqi_status_number":     u.get("qqi_status_number"),
            "gender_id":             str(u["gender_id"]) if u.get("gender_id") else None,
            "user_sub_type_ids":     u.get("user_sub_type_ids") or [],
            "user_sub_type_oids":    [str(oid) for oid in sub_oids],
            "user_sub_types":        ([{"id": str(oid), "name": sub_type_name_map.get(str(oid), "")}
                                       for oid in sub_oids if ObjectId.is_valid(str(oid))]
                                      or [{"id": None, "name": n}
                                          for n in (u.get("user_sub_type_ids") or []) if n]),
            "visa_type_id":          u.get("visa_type_id"),
            "visa_type_name":        visa_type_name_map.get(str(u.get("visa_type_id", "")))
                                     if u.get("visa_type_id") else None,
            "prior_shifts":          prior_shifts,
            "work_history":          work_history,
            "status":                u.get("status"),
            "county_id":             county_id,
            "county":                county_name,
            "user_type_id":          user_type_id,
            "user_type":             user_type_name,
            "user_latitude":         ucoords[0] if ucoords else None,
            "user_longitude":        ucoords[1] if ucoords else None,
            "distance_km":           u.get("_distance_km"),
            "excluded":              u.get("_excluded", 0),
            "exclusion_tags":        u.get("_excl_tags") or [],
            "requested":             1 if uid_str in requested_user_ids else 0,
            "in_pool":               u.get("_in_pool", 0),
        })

    if order_by == "name":
        results.sort(key=lambda r: r["name"].lower(), reverse=reverse)

    # ── by_designation (already computed concurrently above) ─────────────────
    designation_list  = []
    designation_total = 0
    for row in desig_rows:
        designation_list.append({
            "designation":  row["_id"],
            "user_type_id": str(row["user_type_id"]) if row.get("user_type_id") else None,
            "count":        row["count"],
        })
        designation_total += row["count"]

    # ── Flush writes in the background — don't make the caller wait ──────────
    exclusions_computed = len(_cache_writes)

    async def _flush(cache_writes: list, backfill: list):
        if cache_writes:
            try:
                await db["users"].bulk_write(cache_writes, ordered=False)
            except Exception as e:
                logger.warning(f"[list-multi] exclusion cache write failed: {e}")
        if backfill:
            try:
                await db["users"].bulk_write(backfill, ordered=False)
            except Exception as e:
                logger.warning(f"[list-multi] county/user_type backfill failed: {e}")

    if _cache_writes or _backfill:
        _spawn_bg(_flush(list(_cache_writes), list(_backfill)))

    return {
        "success":             True,
        "total":               designation_total,
        "db_total":            db_total,
        "filtered_total":      filtered_total,
        "scanned":             scanned,
        "scan_truncated":      scan_truncated,
        "exclusions_computed": exclusions_computed,
        "page":                payload.page,
        "per_page":            per_page,
        "shift_ids":           [str(o) for o in shift_oids],
        "primary_shift_id":    str(primary_oid),
        "shift_user_types":    shift_user_types,
        "group_ids":           [str(g) for g in group_oids],
        "client_location":     client_location,
        "radius":              payload.radius,
        "order_by":            order_by,
        "sort":                payload.sort or "asc",
        "by_designation":      designation_list,
        "data":                results,
    }