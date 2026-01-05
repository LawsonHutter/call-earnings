# dcf_transcripts_fast.py
"""
Fast scraper for DiscountingCashFlows transcripts via HTMX fragment endpoints (no Playwright).

Dependencies:
    pip install requests beautifulsoup4

Usage:
    from dcf_transcripts_fast import (
        get_transcript_text,
        save_transcript_txt,
        get_transcript_speaker_blocks,
        save_transcript_csv,
        ScrapeConfig,
    )
"""

from __future__ import annotations

import csv
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union
from typing import Sequence

import requests
from bs4 import BeautifulSoup


HTMX_PARAM_NAME = "org.htmx.cache-buster"
HTMX_PARAM_VALUE = "transcriptsContent"


@dataclass(frozen=True)
class SpeakerBlock:
    speaker: str
    text: str


@dataclass
class ScrapeConfig:
    """
    Provide only what you need. For most cases, cookies are not required.
    If you get 404/403 for fragment endpoints, pass csrftoken/cookies from your browser.

    - base_headers: merged into default headers
    - cookies: requests cookies dict
    - max_parts: how many /1/, /2/ pages to attempt
    - sleep_s: optional politeness delay (0 for max speed)
    """
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    cookies: Optional[Dict[str, str]] = None
    base_headers: Optional[Dict[str, str]] = None
    max_parts: int = 200
    sleep_s: float = 0.0
    timeout_s: int = 30


class TranscriptScrapeError(RuntimeError):
    pass


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)


def _normalize_whitespace(s: str) -> str:
    s = re.sub(r"\r\n|\r", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _build_headers(base_url: str, cfg: ScrapeConfig) -> Dict[str, str]:
    headers = {
        "Accept": "*/*",
        "User-Agent": cfg.user_agent,
        "Referer": base_url,
        # HTMX headers (important)
        "HX-Request": "true",
        "HX-Current-URL": base_url,
        "HX-Target": "transcriptsContent",
    }
    if cfg.base_headers:
        headers.update(cfg.base_headers)
    return headers


def _fetch_fragment(session: requests.Session, url: str, headers: Dict[str, str], cfg: ScrapeConfig) -> Optional[str]:
    params = {HTMX_PARAM_NAME: HTMX_PARAM_VALUE}
    r = session.get(url, params=params, headers=headers, timeout=cfg.timeout_s, allow_redirects=True)

    # Stop on 404 (no more parts)
    if r.status_code == 404:
        return None

    # Raise other errors explicitly
    if r.status_code >= 400:
        raise TranscriptScrapeError(f"HTTP {r.status_code} for {r.url}")

    body = r.text.strip()
    if not body:
        return None

    # If the site returns a full 404 template with status 200, detect and stop/fail
    if "Page Not Found" in body and "404" in body:
        # treat as missing fragment
        return None

    return body


def get_transcript_html(base_url: str, cfg: Optional[ScrapeConfig] = None) -> str:
    """
    Fetches HTMX transcript fragments quickly via HTTP and concatenates them.
    """
    cfg = cfg or ScrapeConfig()

    # Ensure trailing slash for consistent URL joining
    if not base_url.endswith("/"):
        base_url = base_url + "/"

    headers = _build_headers(base_url, cfg)

    with requests.Session() as s:
        if cfg.cookies:
            s.cookies.update(cfg.cookies)

        fragments: List[str] = []

        # 1) Try the base URL as the fragment endpoint (works for some quarters)
        first = _fetch_fragment(s, base_url, headers, cfg)
        if first:
            fragments.append(first)

        # 2) Then try numbered parts /1/, /2/, ... until missing
        for i in range(1, cfg.max_parts + 1):
            part_url = f"{base_url}{i}/"
            body = _fetch_fragment(s, part_url, headers, cfg)
            if body is None:
                # If we already got content and the next part is missing, stop.
                # If we got nothing at all, we’ll fall through to error below.
                if fragments:
                    break
                else:
                    continue
            fragments.append(body)
            if cfg.sleep_s:
                time.sleep(cfg.sleep_s)

    if not fragments:
        raise TranscriptScrapeError(
            "No transcript fragments retrieved. This may be gated (login/anti-bot) "
            "or the URL pattern changed. Try passing csrftoken/cookies from the browser."
        )

    return "\n".join(fragments)


def get_transcript_text(base_url: str, cfg: Optional[ScrapeConfig] = None) -> str:
    html = get_transcript_html(base_url, cfg)
    return _normalize_whitespace(_html_to_text(html))


def save_transcript_txt(
    base_url: str,
    output_path: Union[str, Path],
    cfg: Optional[ScrapeConfig] = None,
    output_dir: Optional[Union[str, Path]] = None,
) -> Path:
    output_path = Path(output_path)

    if output_dir is not None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / output_path.name
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    text = get_transcript_text(base_url, cfg)
    output_path.write_text(text, encoding="utf-8")
    return output_path


def _parse_speaker_blocks_from_text(full_text: str) -> List[SpeakerBlock]:
    """
    Fast, text-based parsing fallback.

    Heuristic:
    - Treat a line as a speaker label if:
        * it is relatively short
        * and the next lines form a paragraph
    This works reasonably well for many transcript formats but is not perfect.
    """
    lines = [ln.strip() for ln in full_text.splitlines()]
    lines = [ln for ln in lines if ln != ""]

    def looks_like_speaker(line: str) -> bool:
        if len(line) > 60:
            return False
        if re.search(r"\b(Fiscal|Quarter|FY|Download|Insights|Privacy|Terms|Disclaimer)\b", line, re.I):
            return False
        # speaker labels often have 2–4 words and no punctuation
        if any(ch in line for ch in ".!?"):
            return False
        w = line.split()
        return 1 <= len(w) <= 5

    blocks: List[SpeakerBlock] = []
    current_speaker: Optional[str] = None
    buf: List[str] = []

    def flush():
        nonlocal current_speaker, buf
        if current_speaker and buf:
            blocks.append(SpeakerBlock(current_speaker, _normalize_whitespace(" ".join(buf))))
        buf = []

    for ln in lines:
        if looks_like_speaker(ln):
            # If we already have a speaker and some text, flush
            flush()
            current_speaker = ln
            continue
        if current_speaker:
            buf.append(ln)

    flush()

    if not blocks:
        blocks = [SpeakerBlock("Unknown", full_text)]

    return blocks


def get_transcript_speaker_blocks(base_url: str, cfg: Optional[ScrapeConfig] = None) -> List[SpeakerBlock]:
    text = get_transcript_text(base_url, cfg)
    return _parse_speaker_blocks_from_text(text)


def save_transcript_csv(
    base_url: str,
    output_path: Union[str, Path],
    cfg: Optional[ScrapeConfig] = None,
    output_dir: Optional[Union[str, Path]] = None,
) -> Path:
    output_path = Path(output_path)

    if output_dir is not None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / output_path.name
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    blocks = get_transcript_speaker_blocks(base_url, cfg)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sequence", "speaker", "text"])
        for i, b in enumerate(blocks, start=1):
            w.writerow([i, b.speaker, b.text])

    return output_path


def _filename_base_from_url(base_url: str) -> str:
    """
    Example:
      https://discountingcashflows.com/company/AAPL/transcripts/2025/4/
      -> AAPL_2025_Q4
    """
    m = re.search(r"/company/([^/]+)/transcripts/(\d{4})/(\d+)/", base_url)
    if not m:
        return "transcript"
    ticker, year, quarter = m.groups()
    return f"{ticker}_{year}_Q{quarter}"

# --- Add to dcf_transcripts.py (no breaking changes; additive only) ---

_LIMIT_REACHED_PATTERNS = (
    "Request Limit Reached",
    "Forbidden - Request Limit Reached",
    "You seem to have reached your request limit",
)

def is_rate_limited_message(text: str) -> bool:
    t = (text or "").lower()
    return any(p.lower() in t for p in _LIMIT_REACHED_PATTERNS)

def transcript_text_from_html(html: str) -> str:
    return _normalize_whitespace(_html_to_text(html))

def speaker_blocks_from_text(full_text: str) -> List[SpeakerBlock]:
    # reuse your existing internal heuristic parser
    return _parse_speaker_blocks_from_text(full_text)

def save_transcript_txt_from_text(
    text: str,
    output_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
) -> Path:
    output_path = Path(output_path)
    if output_dir is not None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / output_path.name
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(text, encoding="utf-8")
    return output_path

def save_transcript_csv_from_blocks(
    blocks: Sequence[SpeakerBlock],
    output_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
) -> Path:
    output_path = Path(output_path)
    if output_dir is not None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / output_path.name
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sequence", "speaker", "text"])
        for i, b in enumerate(blocks, start=1):
            w.writerow([i, b.speaker, b.text])

    return output_path