from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Literal
from urllib.parse import urlparse
from uuid import uuid4

CoverDownloadStatus = Literal["downloaded", "skipped", "failed"]

_ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif"}


def _safe_slug_filename(slug: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", slug.strip())
    return normalized or "unknown-slug"


def _cover_extension_from_url(cover_url: str) -> str:
    parsed = urlparse(cover_url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in _ALLOWED_EXTENSIONS:
        return suffix
    return ".jpg"


class CoverImageDownloader:
    def __init__(
        self,
        *,
        fetch_binary: Callable[[str], bytes],
        output_dir: str | Path,
        overwrite: bool = False,
    ) -> None:
        self._fetch_binary = fetch_binary
        self.output_dir = Path(output_dir)
        self.overwrite = overwrite
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build_target_path(self, *, slug: str, cover_url: str) -> Path:
        filename = _safe_slug_filename(slug) + _cover_extension_from_url(cover_url)
        return self.output_dir / filename

    @staticmethod
    def _cleanup_tmp_path(tmp_path: Path) -> None:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass

    def download(self, *, slug: str, cover_url: str | None) -> CoverDownloadStatus:
        if not cover_url:
            return "skipped"

        target_path = self.build_target_path(slug=slug, cover_url=cover_url)
        if target_path.exists() and not self.overwrite:
            return "skipped"

        tmp_path = target_path.with_name(f"{target_path.name}.{uuid4().hex}.part")
        try:
            content = self._fetch_binary(cover_url)
            tmp_path.write_bytes(content)
            tmp_path.replace(target_path)
            return "downloaded"
        except InterruptedError:
            self._cleanup_tmp_path(tmp_path)
            raise
        except Exception:
            self._cleanup_tmp_path(tmp_path)
            return "failed"
