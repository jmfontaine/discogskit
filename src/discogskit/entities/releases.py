"""Releases entity definition: Arrow schemas and ``<release>`` record parsing.

The XML has deeply nested structure (release → tracks → artists). Denormalizing into one flat table would create massive
row duplication. 12 normalized tables with release_id foreign keys keep storage compact and enable efficient joins.
The schema mirrors the XML structure. The chunk worker that drives ``append_record`` is shared; see ``_worker.py``.
"""

from __future__ import annotations

import pyarrow as pa
from lxml import etree

from discogskit.entities._def import (
    VIDEO_ATTRS,
    Cols,
    ElementValueError,
    EntityDef,
    check_attrs,
    check_leaf,
    check_text,
    child_elements,
    leaf_text,
    leaf_texts,
    parse_bool,
    second_copy,
)

_ArtistRec = tuple[int | None, str | None, str | None, str | None]
_ExtraArtistRec = tuple[int | None, str | None, str | None, str | None, str | None]
# track_idx, parent_idx, position, title, duration, artists, extra artists.
_TrackRec = tuple[
    int,
    int | None,
    str | None,
    str | None,
    str | None,
    list[_ArtistRec],
    list[_ExtraArtistRec],
]
# Track-level extra artists have no tracks column, so a <tracks> value there fails instead of vanishing (#202).
_NO_TRACK_TRACKS = "only release-level extra artists have a tracks column"
# The attributes each element the parsers read attributes of has; --strict reports any other.
_FORMAT_ATTRS = frozenset({"name", "qty", "text"})
_IDENTIFIER_ATTRS = frozenset({"description", "type", "value"})
_LABEL_ATTRS = frozenset({"catno", "id", "name"})
_MASTER_ID_ATTRS = frozenset({"is_main_release"})
_RELEASE_ATTRS = frozenset({"id", "status"})

# ------------------------------------------------------------------------------------------------------------------------
# Arrow schemas (single source of truth for column definitions; the first table is the root table)
#
# These schemas serve two purposes:
#   1. RecordBatch construction: pa.RecordBatch.from_pydict(cols, schema=...) enforces types and catches mismatches
#      at serialization time.
#   2. DDL generation: the PostgreSQL writer derives CREATE TABLE DDL from these schemas so column types stay in sync
#      automatically.
#
# Arrow types map to PostgreSQL types:
#   pa.int32() → INTEGER    pa.bool_() → BOOLEAN    pa.utf8() → TEXT    pa.list_(utf8()) → TEXT[]
# ------------------------------------------------------------------------------------------------------------------------

SCHEMAS = {
    "releases": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("country", pa.utf8()),
            pa.field("data_quality", pa.utf8()),
            pa.field("genres", pa.list_(pa.utf8())),
            pa.field("is_main_release", pa.bool_()),
            pa.field("master_id", pa.int32()),
            pa.field("notes", pa.utf8()),
            pa.field("released", pa.utf8()),
            pa.field("status", pa.utf8()),
            pa.field("styles", pa.list_(pa.utf8())),
            pa.field("title", pa.utf8()),
        ]
    ),
    "release_artists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8()),
            pa.field("join", pa.utf8()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "release_extraartists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8()),
            pa.field("name", pa.utf8()),
            pa.field("role", pa.utf8()),
            pa.field("tracks", pa.utf8()),
        ]
    ),
    "release_labels": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("label_id", pa.int32()),
            pa.field("catno", pa.utf8()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "release_series": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("series_id", pa.int32()),
            pa.field("catno", pa.utf8()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "release_formats": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("descriptions", pa.list_(pa.utf8())),
            pa.field("name", pa.utf8()),
            pa.field("qty", pa.utf8()),
            pa.field("text", pa.utf8()),
        ]
    ),
    "release_identifiers": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("description", pa.utf8()),
            pa.field("type", pa.utf8()),
            pa.field("value", pa.utf8()),
        ]
    ),
    "release_videos": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("description", pa.utf8()),
            pa.field("duration", pa.int32()),
            pa.field("embed", pa.bool_()),
            pa.field("src", pa.utf8()),
            pa.field("title", pa.utf8()),
        ]
    ),
    "release_companies": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("company_id", pa.int32()),
            pa.field("catno", pa.utf8()),
            pa.field("entity_type_name", pa.utf8()),
            pa.field("entity_type", pa.utf8()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "release_tracks": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("track_idx", pa.int32(), nullable=False),
            pa.field("parent_idx", pa.int32()),
            pa.field("duration", pa.utf8()),
            pa.field("position", pa.utf8()),
            pa.field("title", pa.utf8()),
        ]
    ),
    "release_track_artists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("track_idx", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8()),
            pa.field("join", pa.utf8()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "release_track_extraartists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("track_idx", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8()),
            pa.field("name", pa.utf8()),
            pa.field("role", pa.utf8()),
        ]
    ),
}

# Relative flush time per table (%), profiled on the full 19M releases dataset. Used by _split_table_groups in
# the PostgreSQL writer to balance work across write workers so no single group becomes the bottleneck.
# If the schema changes, re-profile with --profile and update these weights.
TABLE_WEIGHTS = {
    "release_tracks": 22.3,
    "release_extraartists": 15.0,
    "release_track_extraartists": 13.5,
    "releases": 13.1,
    "release_track_artists": 7.3,
    "release_companies": 6.0,
    "release_artists": 5.2,
    "release_identifiers": 4.6,
    "release_formats": 4.6,
    "release_videos": 4.1,
    "release_labels": 3.7,
    "release_series": 0.5,
}

# ------------------------------------------------------------------------------------------------------------------------
# XML parsing helpers
#
# Uses manual iteration over child elements (``for ch in elem``) rather than XPath/find() because it's faster for
# elements with many children: we visit each child exactly once instead of scanning the child list per query.
# ------------------------------------------------------------------------------------------------------------------------


def _parse_artist_children(
    parent_elem: etree._Element, parent_path: str, unknown: set[str] | None
) -> list[_ArtistRec]:
    """Parse the <artist> children of an <artists> element; ``parent_path`` is that element's parent's path."""
    path = "" if unknown is None else f"{parent_path}{parent_elem.tag}/artist/"
    result = []
    for a in child_elements(parent_elem, "artist", parent_path, unknown):
        if unknown is not None:
            check_attrs(a, path, unknown)
            check_text(a, path, unknown)
        aid_text = None
        name = None
        anv = None
        join_text = None
        for ch in a:
            t = ch.tag
            if t == "id":
                if aid_text is not None:
                    raise second_copy(ch)
                aid_text = ch.text or ""
            elif t == "name":
                if name is not None:
                    raise second_copy(ch)
                name = ch.text or ""
            elif t == "anv":
                if anv is not None:
                    raise second_copy(ch)
                anv = ch.text or ""
            elif t == "join":
                if join_text is not None:
                    raise second_copy(ch)
                join_text = ch.text or ""
            else:
                if unknown is not None:
                    unknown.add(f"{path}{t}")
                continue
            # Every child handled above is text-only.
            if unknown is not None:
                check_leaf(ch, path, unknown)
        result.append((int(aid_text) if aid_text else None, name, anv, join_text))
    return result


def _parse_extraartist_children(
    parent_elem: etree._Element, parent_path: str, unknown: set[str] | None
) -> tuple[list[_ExtraArtistRec], etree._Element | None]:
    """Parse the <artist> children of an <extraartists> element; ``parent_path`` is that element's parent's path.

    Also returns the first <tracks> element with text, for track-level callers to reject: only release-level extra
    artists have a tracks column.
    """
    path = "" if unknown is None else f"{parent_path}{parent_elem.tag}/artist/"
    result = []
    first_tracks = None
    for a in child_elements(parent_elem, "artist", parent_path, unknown):
        if unknown is not None:
            check_attrs(a, path, unknown)
            check_text(a, path, unknown)
        aid_text = None
        name = None
        anv = None
        role = None
        tracks_text = None
        for ch in a:
            t = ch.tag
            if t == "id":
                if aid_text is not None:
                    raise second_copy(ch)
                aid_text = ch.text or ""
            elif t == "name":
                if name is not None:
                    raise second_copy(ch)
                name = ch.text or ""
            elif t == "anv":
                if anv is not None:
                    raise second_copy(ch)
                anv = ch.text or ""
            elif t == "role":
                if role is not None:
                    raise second_copy(ch)
                role = ch.text or ""
            elif t == "tracks":
                if tracks_text is not None:
                    raise second_copy(ch)
                tracks_text = ch.text or ""
                if first_tracks is None and tracks_text:
                    first_tracks = ch
            else:
                if unknown is not None:
                    unknown.add(f"{path}{t}")
                continue
            # Every child handled above is text-only.
            if unknown is not None:
                check_leaf(ch, path, unknown)
        result.append(
            (int(aid_text) if aid_text else None, name, anv, role, tracks_text)
        )
    return result, first_tracks


def _parse_single_track(
    track_elem: etree._Element,
    path: str,
    unknown: set[str] | None,
) -> tuple[
    str | None,
    str | None,
    str | None,
    list[_ArtistRec],
    list[_ExtraArtistRec],
    etree._Element | None,
    etree._Element | None,
]:
    """Parse a <track> element; ``path`` is its own path relative to the record, ending in ``/``.

    Returns its fields, its <sub_tracks> element and the first <tracks> with text in its extra artists, if any.
    """
    position = None
    title = None
    duration = None
    artists: list[_ArtistRec] | None = None
    extraartists: list[_ExtraArtistRec] | None = None
    sub_tracks_elem = None
    tracks_elem = None
    if unknown is not None:
        check_attrs(track_elem, path, unknown)
        check_text(track_elem, path, unknown)
    for ch in track_elem:
        tag = ch.tag
        if tag == "position":
            if position is not None:
                raise second_copy(ch)
            position = ch.text or ""
        elif tag == "title":
            if title is not None:
                raise second_copy(ch)
            title = ch.text or ""
        elif tag == "duration":
            if duration is not None:
                raise second_copy(ch)
            duration = ch.text or ""
        elif tag == "artists":
            if artists is not None:
                raise second_copy(ch)
            artists = _parse_artist_children(ch, path, unknown)
            continue
        elif tag == "extraartists":
            if extraartists is not None:
                raise second_copy(ch)
            extraartists, tracks_elem = _parse_extraartist_children(ch, path, unknown)
            continue
        elif tag == "sub_tracks":
            if sub_tracks_elem is not None:
                raise second_copy(ch)
            sub_tracks_elem = ch
            continue
        else:
            if unknown is not None:
                unknown.add(f"{path}{tag}")
            continue
        # Only the text-only children above get here.
        if unknown is not None:
            check_leaf(ch, path, unknown)
    return (
        position,
        title,
        duration,
        artists or [],
        extraartists or [],
        sub_tracks_elem,
        tracks_elem,
    )


def append_record(
    cols: Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse a <release> element and append all data to column accumulators."""
    id_text = record_id(elem)
    if not id_text:
        raise ValueError("missing or empty id")
    rid = int(id_text)
    status = elem.get("status")
    if unknown is not None:
        check_attrs(elem, "", unknown, _RELEASE_ATTRS)
        check_text(elem, "", unknown)

    title = None
    country = None
    released = None
    notes = None
    data_quality = None
    master_id = None
    master_id_text = None
    is_main_release = None
    genres = None
    styles = None

    # The containers a release holds one of stay None until read, so a second copy is told apart from an empty first.
    r_artists: list[_ArtistRec] | None = None
    r_extraartists: list[_ExtraArtistRec] | None = None
    r_labels = []
    r_series = []
    r_formats = []
    r_identifiers = []
    r_videos = []
    r_companies = []
    r_tracks: list[_TrackRec] | None = None

    for child in elem:
        tag = child.tag
        if tag == "title":
            if title is not None:
                raise second_copy(child)
            title = leaf_text(child, "", unknown)
        elif tag == "country":
            if country is not None:
                raise second_copy(child)
            country = leaf_text(child, "", unknown)
        elif tag == "released":
            if released is not None:
                raise second_copy(child)
            released = leaf_text(child, "", unknown)
        elif tag == "notes":
            if notes is not None:
                raise second_copy(child)
            notes = leaf_text(child, "", unknown)
        elif tag == "data_quality":
            if data_quality is not None:
                raise second_copy(child)
            data_quality = leaf_text(child, "", unknown)
        elif tag == "master_id":
            if master_id_text is not None:
                raise second_copy(child)
            master_id_text = child.text or ""
            if unknown is not None:
                check_leaf(child, "", unknown, _MASTER_ID_ATTRS)
            master_id = int(master_id_text) if master_id_text else None
            is_main_release = parse_bool(
                "is_main_release", child.get("is_main_release")
            )
        elif tag == "genres":
            if genres is not None:
                raise second_copy(child)
            genres = leaf_texts(child, "genre", "", unknown)
        elif tag == "styles":
            if styles is not None:
                raise second_copy(child)
            styles = leaf_texts(child, "style", "", unknown)
        elif tag == "artists":
            if r_artists is not None:
                raise second_copy(child)
            r_artists = _parse_artist_children(child, "", unknown)
        elif tag == "extraartists":
            if r_extraartists is not None:
                raise second_copy(child)
            r_extraartists, _ = _parse_extraartist_children(child, "", unknown)
        elif tag == "labels":
            for la in child_elements(child, "label", "", unknown):
                if unknown is not None:
                    check_leaf(la, "labels/", unknown, _LABEL_ATTRS)
                    check_text(la, "labels/label/", unknown)
                la_id_str = la.get("id")
                r_labels.append(
                    (
                        int(la_id_str) if la_id_str else None,
                        la.get("name"),
                        la.get("catno"),
                    )
                )
        elif tag == "series":
            for sr in child_elements(child, "series", "", unknown):
                if unknown is not None:
                    check_leaf(sr, "series/", unknown, _LABEL_ATTRS)
                    check_text(sr, "series/series/", unknown)
                sr_id_str = sr.get("id")
                r_series.append(
                    (
                        int(sr_id_str) if sr_id_str else None,
                        sr.get("name"),
                        sr.get("catno"),
                    )
                )
        elif tag == "formats":
            for ff in child_elements(child, "format", "", unknown):
                descs_elems = list(
                    child_elements(
                        ff, "descriptions", "formats/", unknown, _FORMAT_ATTRS
                    )
                )
                if len(descs_elems) > 1:
                    raise second_copy(descs_elems[1])
                descs = (
                    leaf_texts(
                        descs_elems[0], "description", "formats/format/", unknown
                    )
                    if descs_elems
                    else None
                )
                r_formats.append(
                    (
                        ff.get("name"),
                        ff.get("qty"),
                        ff.get("text"),
                        descs,
                    )
                )
        elif tag == "identifiers":
            for ident in child_elements(child, "identifier", "", unknown):
                if unknown is not None:
                    check_leaf(ident, "identifiers/", unknown, _IDENTIFIER_ATTRS)
                    check_text(ident, "identifiers/identifier/", unknown)
                r_identifiers.append(
                    (
                        ident.get("type"),
                        ident.get("description"),
                        ident.get("value"),
                    )
                )
        elif tag == "videos":
            for vid in child_elements(child, "video", "", unknown):
                if unknown is not None:
                    check_attrs(vid, "videos/video/", unknown, VIDEO_ATTRS)
                    check_text(vid, "videos/video/", unknown)
                dur_str = vid.get("duration")
                vid_dur = None if dur_str is None else int(dur_str)
                vid_embed = parse_bool("embed", vid.get("embed"))
                vid_title = None
                vid_desc = None
                for vch in vid:
                    if vch.tag == "title":
                        if vid_title is not None:
                            raise second_copy(vch)
                        vid_title = vch.text or ""
                    elif vch.tag == "description":
                        if vid_desc is not None:
                            raise second_copy(vch)
                        vid_desc = vch.text or ""
                    else:
                        if unknown is not None:
                            unknown.add(f"videos/video/{vch.tag}")
                        continue
                    # Every child handled above is text-only.
                    if unknown is not None:
                        check_leaf(vch, "videos/video/", unknown)
                r_videos.append(
                    (
                        vid.get("src"),
                        vid_dur,
                        vid_title,
                        vid_desc,
                        vid_embed,
                    )
                )
        elif tag == "companies":
            for co in child_elements(child, "company", "", unknown):
                if unknown is not None:
                    check_attrs(co, "companies/company/", unknown)
                    check_text(co, "companies/company/", unknown)
                cid_text = None
                cname = None
                ccatno = None
                cetype = None
                cetype_name = None
                for cch in co:
                    ct = cch.tag
                    if ct == "id":
                        if cid_text is not None:
                            raise second_copy(cch)
                        cid_text = cch.text or ""
                    elif ct == "name":
                        if cname is not None:
                            raise second_copy(cch)
                        cname = cch.text or ""
                    elif ct == "catno":
                        if ccatno is not None:
                            raise second_copy(cch)
                        ccatno = cch.text or ""
                    elif ct == "entity_type":
                        if cetype is not None:
                            raise second_copy(cch)
                        cetype = cch.text or ""
                    elif ct == "entity_type_name":
                        if cetype_name is not None:
                            raise second_copy(cch)
                        cetype_name = cch.text or ""
                    else:
                        if unknown is not None:
                            unknown.add(f"companies/company/{ct}")
                        continue
                    # Every child handled above is text-only.
                    if unknown is not None:
                        check_leaf(cch, "companies/company/", unknown)
                r_companies.append(
                    (
                        int(cid_text) if cid_text else None,
                        cname,
                        ccatno,
                        cetype,
                        cetype_name,
                    )
                )
        elif tag == "tracklist":
            if r_tracks is not None:
                raise second_copy(child)
            r_tracks = []
            # Tracks use a flat index (track_idx) with an optional parent_idx for sub-tracks. This avoids nested
            # arrays in PG while preserving the hierarchical structure for queries.
            track_idx = 0
            for tr in child_elements(child, "track", "", unknown):
                pos, ttl, dur, t_art, t_ea, sub_elem, t_tracks = _parse_single_track(
                    tr, "tracklist/track/", unknown
                )
                if t_tracks is not None:
                    # The worker adds the <tracks> element's line.
                    raise ElementValueError(
                        f"<tracks> {t_tracks.text!r} in an extra artist of track {pos!r}; {_NO_TRACK_TRACKS}",
                        t_tracks,
                    )
                parent = track_idx
                r_tracks.append((track_idx, None, pos, ttl, dur, t_art, t_ea))
                track_idx += 1
                if sub_elem is not None:
                    for sub in child_elements(
                        sub_elem, "track", "tracklist/track/", unknown
                    ):
                        s_pos, s_ttl, s_dur, s_art, s_ea, nested, s_tracks = (
                            _parse_single_track(
                                sub, "tracklist/track/sub_tracks/track/", unknown
                            )
                        )
                        if nested is not None:
                            # Only one level of sub-tracks is parsed, so fail rather than drop the deeper ones. The
                            # worker adds the nested element's line.
                            raise ElementValueError(
                                f"<sub_tracks> nested in sub-track {s_pos!r} of track {pos!r}",
                                nested,
                            )
                        if s_tracks is not None:
                            raise ElementValueError(
                                f"<tracks> {s_tracks.text!r} in an extra artist of sub-track {s_pos!r} of track"
                                f" {pos!r}; {_NO_TRACK_TRACKS}",
                                s_tracks,
                            )
                        r_tracks.append(
                            (track_idx, parent, s_pos, s_ttl, s_dur, s_art, s_ea)
                        )
                        track_idx += 1
        elif unknown is not None:
            unknown.add(tag)

    # -- Append to accumulators --

    r = cols["releases"]
    r["id"].append(rid)
    r["status"].append(status)
    r["title"].append(title)
    r["country"].append(country)
    r["released"].append(released)
    r["notes"].append(notes)
    r["data_quality"].append(data_quality)
    r["master_id"].append(master_id)
    r["is_main_release"].append(is_main_release)
    r["genres"].append(genres)
    r["styles"].append(styles)

    a = cols["release_artists"]
    for aid, name, anv, join_text in r_artists or ():
        a["release_id"].append(rid)
        a["artist_id"].append(aid)
        a["name"].append(name)
        a["anv"].append(anv)
        a["join"].append(join_text)

    ea = cols["release_extraartists"]
    for aid, name, anv, role, tracks_text in r_extraartists or ():
        ea["release_id"].append(rid)
        ea["artist_id"].append(aid)
        ea["name"].append(name)
        ea["anv"].append(anv)
        ea["role"].append(role)
        ea["tracks"].append(tracks_text)

    lb = cols["release_labels"]
    for lid, lname, lcatno in r_labels:
        lb["release_id"].append(rid)
        lb["label_id"].append(lid)
        lb["name"].append(lname)
        lb["catno"].append(lcatno)

    sr = cols["release_series"]
    for sid, sname, scatno in r_series:
        sr["release_id"].append(rid)
        sr["series_id"].append(sid)
        sr["name"].append(sname)
        sr["catno"].append(scatno)

    fm = cols["release_formats"]
    for fname, fqty, ftext, fdescs in r_formats:
        fm["release_id"].append(rid)
        fm["name"].append(fname)
        fm["qty"].append(fqty)
        fm["text"].append(ftext)
        fm["descriptions"].append(fdescs)

    ident = cols["release_identifiers"]
    for itype, idesc, ival in r_identifiers:
        ident["release_id"].append(rid)
        ident["type"].append(itype)
        ident["description"].append(idesc)
        ident["value"].append(ival)

    v = cols["release_videos"]
    for vsrc, vdur, vtitle, vdesc, vembed in r_videos:
        v["release_id"].append(rid)
        v["src"].append(vsrc)
        v["duration"].append(vdur)
        v["title"].append(vtitle)
        v["description"].append(vdesc)
        v["embed"].append(vembed)

    co = cols["release_companies"]
    for cid, cname, ccatno, cetype, cetype_name in r_companies:
        co["release_id"].append(rid)
        co["company_id"].append(cid)
        co["name"].append(cname)
        co["catno"].append(ccatno)
        co["entity_type"].append(cetype)
        co["entity_type_name"].append(cetype_name)

    t = cols["release_tracks"]
    ta = cols["release_track_artists"]
    te = cols["release_track_extraartists"]
    for tidx, pidx, tpos, ttitle, tdur, t_artists, t_extraartists in r_tracks or ():
        t["release_id"].append(rid)
        t["track_idx"].append(tidx)
        t["parent_idx"].append(pidx)
        t["position"].append(tpos)
        t["title"].append(ttitle)
        t["duration"].append(tdur)

        for aid, name, anv, join_text in t_artists:
            ta["release_id"].append(rid)
            ta["track_idx"].append(tidx)
            ta["artist_id"].append(aid)
            ta["name"].append(name)
            ta["anv"].append(anv)
            ta["join"].append(join_text)

        # The tracklist loop rejected any <tracks> with text here, so the discarded value is None or "".
        for aid, name, anv, role, _tracks in t_extraartists:
            te["release_id"].append(rid)
            te["track_idx"].append(tidx)
            te["artist_id"].append(aid)
            te["name"].append(name)
            te["anv"].append(anv)
            te["role"].append(role)


def record_id(elem: etree._Element) -> str | None:
    """Return a <release> element's id as written in the XML, or None when it has none."""
    return elem.get("id")


ENTITY = EntityDef(
    append_record=append_record,
    fk_column="release_id",
    name="releases",
    record_id=record_id,
    root_tag="release",
    schemas=SCHEMAS,
    table_weights=TABLE_WEIGHTS,
)
