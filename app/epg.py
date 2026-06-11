import csv
import difflib
import gzip
import io
import os
import re
import time
import urllib.request
import json
import shutil
import threading
import bisect
from pathlib import Path
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone


IPTV_ORG_CHANNELS_CSV = "https://raw.githubusercontent.com/iptv-org/database/master/data/channels.csv"
CHANNELS_DB_CACHE = Path(__file__).with_name("channels_db_cache.csv")
EPG_MERGED_CACHE = Path(__file__).with_name("epg_merged_cache.xml.gz")


@dataclass
class EpgMatch:
    tvg_id: str | None
    confidence: float
    method: str


class EpgService:
    def __init__(self):
        urls_env = os.environ.get("EPG_URLS", "").strip()
        if urls_env:
            self.epg_urls = [u.strip() for u in urls_env.split(",") if u.strip()]
        else:
            self.epg_urls = [
                "https://iptv-epg.org/files/epg-gb.xml.gz",
                "https://iptv-epg.org/files/epg-ca.xml.gz",
                "https://epg.pw/xmltv/epg_US.xml.gz",
            ]
        self.refresh_seconds = int(os.environ.get("EPG_REFRESH_SECONDS", "21600"))
        self.enable_fuzzy = os.environ.get("EPG_ENABLE_FUZZY", "FALSE").upper() == "TRUE"
        self.enable_heavy_fallback = os.environ.get("EPG_ENABLE_HEAVY_FALLBACK", "TRUE").upper() == "TRUE"
        self.heavy_min_channels_with_programmes = int(os.environ.get("EPG_HEAVY_MIN_CHANNELS_WITH_PROGRAMMES", "200"))
        self.heavy_cooldown_seconds = int(os.environ.get("EPG_HEAVY_COOLDOWN_SECONDS", "21600"))
        self.download_timeout_seconds = int(os.environ.get("EPG_DOWNLOAD_TIMEOUT_SECONDS", "45"))
        self.heavy_max_mb = int(os.environ.get("EPG_HEAVY_MAX_MB", "60"))
        self.min_free_mem_mb = int(os.environ.get("EPG_MIN_FREE_MEM_MB", "300"))
        self._last_refresh = 0.0
        self._channels_by_norm: dict[str, str] = {}
        self._epg_channels: set[str] = set()
        self._channels_with_programmes: set[str] = set()
        self._epg_tree: ET.ElementTree | None = None
        self._match_cache: dict[str, EpgMatch] = {}
        self._epg_ready = False
        self._last_heavy_refresh = 0.0
        self._last_refresh_meta = {"heavy_ran": False, "base_programme_channels": 0, "total_programme_channels": 0, "heavy_urls": 0}
        self._channels_db_retry_after = 0.0
        self._overrides = {}
        self._refresh_lock = threading.Lock()
        self._refreshing = False
        self._programmes_by_channel = {}
        self._load_channels_db_cache()
        try:
            op = Path(__file__).with_name("epg_overrides.json")
            if op.exists():
                self._overrides = json.loads(op.read_text())
        except Exception:
            self._overrides = {}
        self._refresh_lock = threading.Lock()
        self._refreshing = False
        self._programmes_by_channel = {}

    @staticmethod
    def _norm(name: str) -> str:
        s = name.lower().replace("&", " and ")
        s = re.sub(r"([a-zA-Z])(\d)", r"\1 \2", s)
        s = re.sub(r"(\d)([a-zA-Z])", r"\1 \2", s)
        s = re.sub(r"\b(usa|us|uk|hd|fhd|4k|sd|tv|channel|live)\b", " ", s)
        s = re.sub(r"[^a-z0-9]+", " ", s)
        return re.sub(r"\s+", " ", s).strip()


    def _load_channels_db_cache(self):
        try:
            if not CHANNELS_DB_CACHE.exists():
                return
            raw = CHANNELS_DB_CACHE.read_text(encoding="utf-8", errors="ignore").splitlines()
            for row in csv.DictReader(raw):
                name = (row.get("name") or "").strip()
                cid = (row.get("id") or "").strip()
                if not name or not cid:
                    continue
                n = self._norm(name)
                if n and n not in self._channels_by_norm:
                    self._channels_by_norm[n] = cid
        except Exception:
            return

    def _save_channels_db_cache(self, raw_lines: list[str]):
        try:
            CHANNELS_DB_CACHE.write_text("\n".join(raw_lines), encoding="utf-8")
        except Exception:
            return

    def _ensure_channels_db(self):
        now = time.time()
        if self._channels_by_norm:
            return
        if now < self._channels_db_retry_after:
            return
        try:
            with urllib.request.urlopen(IPTV_ORG_CHANNELS_CSV, timeout=20) as r:
                raw_lines = r.read().decode("utf-8", "ignore").splitlines()
            self._save_channels_db_cache(raw_lines)
            for row in csv.DictReader(raw_lines):
                name = (row.get("name") or "").strip()
                cid = (row.get("id") or "").strip()
                if not name or not cid:
                    continue
                n = self._norm(name)
                if n and n not in self._channels_by_norm:
                    self._channels_by_norm[n] = cid
        except Exception:
            # fallback to cached channels db if online fetch fails
            self._load_channels_db_cache()
            self._channels_db_retry_after = now + 600


    def _get_free_mem_mb(self) -> int:
        try:
            with open('/proc/meminfo', 'r', encoding='utf-8', errors='ignore') as f:
                txt = f.read()
            m = re.search(r'^MemAvailable:\s+(\d+) kB$', txt, re.M)
            if not m:
                return 0
            return int(m.group(1)) // 1024
        except Exception:
            return 0

    def _load_merged_cache_if_fresh(self, now: float) -> bool:
        try:
            if not EPG_MERGED_CACHE.exists():
                return False
            age = now - EPG_MERGED_CACHE.stat().st_mtime
            if age > self.refresh_seconds:
                return False
            raw = gzip.decompress(EPG_MERGED_CACHE.read_bytes())
            tree = ET.parse(io.BytesIO(raw))
            root = tree.getroot()
            chs = set()
            with_prog = set()
            for ch in root.findall('channel'):
                cid = ch.attrib.get('id', '').strip()
                if cid:
                    chs.add(cid)
            for pr in root.findall('programme'):
                cid = pr.attrib.get('channel', '').strip()
                if cid:
                    with_prog.add(cid)
            self._epg_tree = tree
            self._epg_channels = chs
            self._channels_with_programmes = with_prog
            self._rebuild_programme_index(root)
            self._last_refresh = now
            self._epg_ready = True
            self._last_refresh_meta = {
                'loaded_from_cache': True,
                'heavy_ran': False,
                'base_programme_channels': len(with_prog),
                'total_programme_channels': len(with_prog),
                'heavy_urls': 0,
                'last_heavy_refresh': self._last_heavy_refresh,
                'free_mem_mb': self._get_free_mem_mb(),
            }
            return True
        except Exception:
            return False

    def _rebuild_programme_index(self, root):
        self._rebuild_programme_index(root)

    def _save_merged_cache(self, root):
        try:
            raw = ET.tostring(root, encoding='utf-8')
            EPG_MERGED_CACHE.write_bytes(gzip.compress(raw, compresslevel=5))
        except Exception:
            return

    def ensure_refresh_async(self):
        now = time.time()
        if self._epg_tree is not None and (now - self._last_refresh) < self.refresh_seconds:
            self._refreshing = False
            return
        if self._refreshing:
            return

        def _run():
            try:
                self.refresh()
            except Exception:
                self._refreshing = False

        self._refreshing = True
        threading.Thread(target=_run, daemon=True).start()

    def _download_xml_bytes(self, url: str, max_bytes: int | None = None) -> bytes:
        with urllib.request.urlopen(url, timeout=self.download_timeout_seconds) as r:
            cl = r.headers.get('Content-Length')
            if max_bytes and cl and cl.isdigit() and int(cl) > max_bytes:
                raise ValueError('feed too large')
            chunks = []
            total = 0
            while True:
                b = r.read(1024 * 1024)
                if not b:
                    break
                total += len(b)
                if max_bytes and total > max_bytes:
                    raise ValueError('feed exceeded max_bytes')
                chunks.append(b)
            data = b''.join(chunks)
        if url.endswith(".gz"):
            return gzip.decompress(data)
        return data


    def _split_epg_urls(self):
        heavy = []
        base = []
        for u in self.epg_urls:
            ul = u.lower()
            if "epgshare01" in ul or "all_sources" in ul:
                heavy.append(u)
            else:
                base.append(u)
        return base, heavy

    def _merge_feed_into_root(self, src_root, root, channel_seen, programme_seen):
        for ch in src_root.findall("channel"):
            cid = ch.attrib.get("id", "").strip()
            if not cid or cid in channel_seen:
                continue
            channel_seen.add(cid)
            root.append(ch)

        for p in src_root.findall("programme"):
            cid = p.attrib.get("channel", "").strip()
            start = p.attrib.get("start", "").strip()
            if not cid:
                continue
            key = f"{cid}|{start}|{(p.findtext('title') or '').strip()}"
            if key in programme_seen:
                continue
            programme_seen.add(key)
            root.append(p)

    def refresh(self):
        if self._refresh_lock.locked():
            return
        with self._refresh_lock:
            now = time.time()
        if self._epg_tree is not None and (now - self._last_refresh) < self.refresh_seconds:
            self._refreshing = False
            return
        if self._epg_tree is None and self._load_merged_cache_if_fresh(now):
            self._refreshing = False
            return

        root = ET.Element("tv")
        channel_seen = set()
        programme_seen = set()

        base_urls, heavy_urls = self._split_epg_urls()

        for url in base_urls:
            try:
                xml_bytes = self._download_xml_bytes(url)
                tree = ET.parse(io.BytesIO(xml_bytes))
                src = tree.getroot()
                self._merge_feed_into_root(src, root, channel_seen, programme_seen)
            except Exception:
                continue

        channels_with_programmes = set()
        for p in root.findall("programme"):
            cid = p.attrib.get("channel", "").strip()
            if cid:
                channels_with_programmes.add(cid)

        base_programme_channels = len(channels_with_programmes)

        free_mem_mb = self._get_free_mem_mb()
        should_load_heavy = (
            self.enable_heavy_fallback
            and heavy_urls
            and len(channels_with_programmes) < self.heavy_min_channels_with_programmes
            and (now - self._last_heavy_refresh) >= self.heavy_cooldown_seconds
            and free_mem_mb >= self.min_free_mem_mb
        )

        heavy_ran = False
        if should_load_heavy:
            for url in heavy_urls:
                try:
                    xml_bytes = self._download_xml_bytes(url, max_bytes=self.heavy_max_mb * 1024 * 1024)
                    tree = ET.parse(io.BytesIO(xml_bytes))
                    src = tree.getroot()
                    self._merge_feed_into_root(src, root, channel_seen, programme_seen)
                except Exception:
                    continue
            self._last_heavy_refresh = now
            heavy_ran = True
            channels_with_programmes = set()
            for p in root.findall("programme"):
                cid = p.attrib.get("channel", "").strip()
                if cid:
                    channels_with_programmes.add(cid)

        self._save_merged_cache(root)
        self._epg_tree = ET.ElementTree(root)
        self._epg_channels = channel_seen
        self._channels_with_programmes = channels_with_programmes
        self._last_refresh = now
        self._epg_ready = True
        self._rebuild_programme_index(root)

        self._last_refresh_meta = {
            "loaded_from_cache": False,
            "heavy_ran": heavy_ran,
            "base_programme_channels": base_programme_channels,
            "total_programme_channels": len(channels_with_programmes),
            "heavy_urls": len(heavy_urls),
            "last_heavy_refresh": self._last_heavy_refresh,
            "free_mem_mb": free_mem_mb,
        }
        self._refreshing = False

    def map_channel_name(self, name: str) -> EpgMatch:
        ov = self._overrides.get(name)
        if ov:
            m = EpgMatch(ov, 1.0, "override")
            self._match_cache[name] = m
            return m
        if name in self._match_cache:
            return self._match_cache[name]

        self._ensure_channels_db()
        if not self._channels_by_norm:
            m = EpgMatch(None, 0.0, "unavailable")
            self._match_cache[name] = m
            return m
        n = self._norm(name)
        if not n:
            m = EpgMatch(None, 0.0, "none")
            self._match_cache[name] = m
            return m

        if n in self._channels_by_norm:
            cid = self._channels_by_norm[n]
            m = EpgMatch(cid, 1.0, "exact")
            self._match_cache[name] = m
            return m

        if self.enable_fuzzy:
            close = difflib.get_close_matches(n, list(self._channels_by_norm.keys()), n=1, cutoff=0.93)
            if close:
                cid = self._channels_by_norm[close[0]]
                m = EpgMatch(cid, 0.92, "fuzzy")
                self._match_cache[name] = m
                return m

        m = EpgMatch(None, 0.0, "none")
        self._match_cache[name] = m
        return m

    @staticmethod
    def _parse_xmltv_dt(value: str) -> datetime | None:
        if not value:
            return None
        value = value.strip()
        for fmt in ["%Y%m%d%H%M%S %z", "%Y%m%d%H%M %z", "%Y%m%d%H%M%S", "%Y%m%d%H%M"]:
            try:
                dt = datetime.strptime(value, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
        return None

    def get_now_next(self, tvg_id: str):
        self.ensure_refresh_async()
        if not tvg_id:
            return {"now": None, "next": None}
        arr = self._programmes_by_channel.get(tvg_id)
        if not arr:
            return {"now": None, "next": None}
        now_ts = datetime.now(timezone.utc).timestamp()
        starts = [x[0] for x in arr]
        i = bisect.bisect_right(starts, now_ts) - 1
        current = None
        nxt = None
        if 0 <= i < len(arr):
            st, sp, title = arr[i]
            if st <= now_ts < sp:
                current = {"title": title, "start": datetime.fromtimestamp(st, timezone.utc).isoformat(), "stop": datetime.fromtimestamp(sp, timezone.utc).isoformat()}
                if i + 1 < len(arr):
                    nst, nsp, ntitle = arr[i+1]
                    nxt = {"title": ntitle, "start": datetime.fromtimestamp(nst, timezone.utc).isoformat(), "stop": datetime.fromtimestamp(nsp, timezone.utc).isoformat()}
                return {"now": current, "next": nxt}
        j = max(i + 1, 0)
        if j < len(arr):
            nst, nsp, ntitle = arr[j]
            nxt = {"title": ntitle, "start": datetime.fromtimestamp(nst, timezone.utc).isoformat(), "stop": datetime.fromtimestamp(nsp, timezone.utc).isoformat()}
        return {"now": current, "next": nxt}

    def get_upcoming_events(self, hours: int = 24, limit: int = 300):
        self.refresh()
        if self._epg_tree is None:
            return []
        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() + hours * 3600
        events = []
        for p in self._epg_tree.getroot().findall("programme"):
            start = self._parse_xmltv_dt(p.attrib.get("start", ""))
            if not start:
                continue
            ts = start.timestamp()
            if ts < now.timestamp() or ts > cutoff:
                continue
            events.append({
                "channel": p.attrib.get("channel", "").strip(),
                "title": (p.findtext("title") or "").strip(),
                "start": start.isoformat(),
                "category": (p.findtext("category") or "EPG").strip(),
            })
        events.sort(key=lambda e: e["start"])
        return events[:limit]

    def has_programme_data(self, tvg_id: str | None) -> bool:
        # non-blocking check for fast channel listing/status paths
        if not tvg_id:
            return False
        return tvg_id in self._channels_with_programmes

    def build_filtered_epg_xml(self, tvg_ids: set[str]) -> str:
        self.refresh()
        if self._epg_tree is None:
            return "<tv></tv>"
        src = self._epg_tree.getroot()
        root = ET.Element("tv")
        for ch in src.findall("channel"):
            cid = ch.attrib.get("id", "").strip()
            if cid in tvg_ids:
                root.append(ch)
        for p in src.findall("programme"):
            cid = p.attrib.get("channel", "").strip()
            if cid in tvg_ids:
                root.append(p)
        return ET.tostring(root, encoding="unicode")


    def debug_refresh_meta(self):
        d = dict(self._last_refresh_meta)
        d["refreshing"] = self._refreshing
        d["indexed_channels"] = len(self._programmes_by_channel)
        return d
