import base64
import json
import os
import re
from pathlib import Path
try:
    import reflex as rx
except Exception:
    rx = None
from pydantic import BaseModel
from urllib.parse import quote, urlparse, urljoin
import httpx
AsyncSession=httpx.AsyncClient
from typing import List
from .utils import encrypt, decrypt, urlsafe_base64, decode_bundle
from types import SimpleNamespace
config = SimpleNamespace(
    api_url=(os.environ.get("API_URL", "http://127.0.0.1:3000").strip()),
    proxy_content=(os.environ.get("PROXY_CONTENT", "TRUE").upper()=="TRUE"),
    socks5=(os.environ.get("SOCKS5", "").strip()),
)
import html


class Channel(BaseModel):
    id: str
    name: str
    tags: List[str]
    logo: str | None
    dead: bool = False
    tvg_id: str | None = None
    epg_has_data: bool = False


class StepDaddy:
    def __init__(self):
        socks5 = config.socks5
        if socks5 != "":
            self._session = AsyncSession(proxy="socks5://" + socks5)
        else:
            self._session = AsyncSession()
        base_url = os.environ.get("DLHD_BASE_URL", "https://dlhd.pk").strip()
        self._base_url = base_url.rstrip("/")
        self.channels = []
        meta_path = Path(__file__).with_name("meta.json")
        with open(meta_path, "r", encoding="utf-8") as f:
            self._meta = json.load(f)

    def _headers(self, referer: str = None, origin: str = None):
        if referer is None:
            referer = self._base_url
        headers = {
            "Referer": referer,
            "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0",
        }
        if origin:
            headers["Origin"] = origin
        return headers

    async def load_channels(self):
        channels = []
        try:
            response = await self._session.get(f"{self._base_url}/24-7-channels.php", headers=self._headers())
            matches = re.findall(
                r'<a class="card"\s+href="/watch\.php\?id=(\d+)"[^>]*>\s*<div class="card__title">(.*?)</div>',
                response.text,
                re.DOTALL
            )
            for channel_id, channel_name in matches:
                channel_name = html.unescape(channel_name.strip()).replace("#", "")
                meta = self._meta.get("18+" if channel_name.startswith("18+") else channel_name, {})
                logo = meta.get("logo", "")
                if logo:
                    logo = f"{config.api_url}/logo/{urlsafe_base64(logo)}"
                channels.append(Channel(id=channel_id, name=channel_name, tags=meta.get("tags", []), logo=logo))
        finally:
            self.channels = sorted(channels, key=lambda channel: (channel.name.startswith("18"), channel.name))

    async def stream(self, channel_id: str):
        def rewrite_playlist(m3u8_text: str, m3u8_url: str, referer_host: str):
            lines_out = []
            non_comment_count = 0
            for line in m3u8_text.split("\n"):
                line = line.strip()
                if line.startswith("#EXT-X-KEY:"):
                    uri_match = re.search(r'URI="(.*?)"', line)
                    if uri_match:
                        original_url = uri_match.group(1)
                        absolute_key_url = urljoin(m3u8_url, original_url)
                        line = line.replace(original_url, f"{config.api_url}/key/{encrypt(absolute_key_url)}/{encrypt(referer_host)}")
                elif line and not line.startswith("#"):
                    non_comment_count += 1
                    absolute_media_url = urljoin(m3u8_url, line)
                    if config.proxy_content:
                        line = f"{config.api_url}/content/{encrypt(absolute_media_url)}"
                    else:
                        line = absolute_media_url
                lines_out.append(line)

            has_extm3u = any(l.startswith("#EXTM3U") for l in lines_out)
            if not has_extm3u and non_comment_count == 1:
                media_line = next((l for l in lines_out if l and not l.startswith("#")), "")
                return f"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=8000000\n{media_line}\n"

            return "\n".join(lines_out).strip() + "\n"

        key = "CHANNEL_KEY"

        # New flow: watch page -> iframe -> clappr source(atob)
        watch_url = f"{self._base_url}/watch/stream-{channel_id}.php"
        watch_response = await self._session.get(watch_url, headers=self._headers())
        watch_iframe = re.search(r'iframe\s+src="([^"]+)"', watch_response.text)
        if watch_iframe:
            source_page_url = watch_iframe.group(1)
            source_page_response = await self._session.get(source_page_url, headers=self._headers(watch_url))
            source_b64 = re.search(r"source\s*:\s*window\.atob\('([^']+)'\)", source_page_response.text)
            if source_b64:
                m3u8_url = base64.b64decode(source_b64.group(1)).decode()
                m3u8_response = await self._session.get(m3u8_url, headers=self._headers(source_page_url))
                return rewrite_playlist(m3u8_response.text, m3u8_url, urlparse(source_page_url).netloc)

        # Legacy flow fallback
        url = f"{self._base_url}/stream/stream-{channel_id}.php"
        response = await self._session.get(url, headers=self._headers())
        matches = re.compile("iframe src=\"(.*)\" width").findall(response.text)
        if not matches:
            raise ValueError("Failed to find source URL for channel")

        source_url = matches[0]
        source_response = await self._session.get(source_url, headers=self._headers(url))
        channel_key = re.compile(rf"const\s+{re.escape(key)}\s*=\s*\"(.*?)\";").findall(source_response.text)[-1]

        data = decode_bundle(source_response.text)
        auth_ts = data.get("b_ts", "")
        auth_sig = data.get("b_sig", "")
        auth_rnd = data.get("b_rnd", "")
        auth_url = data.get("b_host", "")
        auth_request_url = f"{auth_url}auth.php?channel_id={channel_key}&ts={auth_ts}&rnd={auth_rnd}&sig={auth_sig}"
        auth_response = await self._session.get(auth_request_url, headers=self._headers(source_url))
        if auth_response.status_code != 200:
            raise ValueError("Failed to get auth response")
        key_url = urlparse(source_url)
        key_url = f"{key_url.scheme}://{key_url.netloc}/server_lookup.php?channel_id={channel_key}"
        key_response = await self._session.get(key_url, headers=self._headers(source_url))
        server_key = key_response.json().get("server_key")
        if not server_key:
            raise ValueError("No server key found in response")
        if server_key == "top1/cdn":
            m3u8_url = f"https://top1.newkso.ru/top1/cdn/{channel_key}/mono.m3u8"
        else:
            m3u8_url = f"https://{server_key}new.newkso.ru/{server_key}/{channel_key}/mono.m3u8"
        m3u8 = await self._session.get(m3u8_url, headers=self._headers(quote(str(source_url))))
        return rewrite_playlist(m3u8.text, m3u8_url, urlparse(source_url).netloc)

    async def key(self, url: str, host: str):
        url = decrypt(url)
        host = decrypt(host)
        response = await self._session.get(url, headers=self._headers(f"{host}/", host), timeout=60)
        if response.status_code != 200:
            raise Exception(f"Failed to get key")
        return response.content

    @staticmethod
    def content_url(path: str):
        return decrypt(path)

    def playlist(self, channels: List[Channel] | None = None):
        data = "#EXTM3U\n"
        items = channels if channels is not None else self.channels
        for channel in items:
            attrs = []
            if channel.tvg_id:
                attrs.append(f'tvg-id="{channel.tvg_id}"')
            if channel.logo:
                attrs.append(f'tvg-logo="{channel.logo}"')
            attrs_str = (" " + " ".join(attrs)) if attrs else ""
            data += f"#EXTINF:-1{attrs_str},{channel.name}\n{config.api_url}/stream/{channel.id}.m3u8\n"
        return data

    async def schedule(self):
        response = await self._session.get(f"{self._base_url}/schedule/schedule-generated.php", headers=self._headers())
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            return []
        try:
            return response.json()
        except Exception:
            return []
