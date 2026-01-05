# dcf_transcripts.py
"""
DiscountingCashFlows transcript scraper.

Features:
- Scrape transcript HTML fragments loaded via HTMX/XHR using Playwright.
- Return transcript as a single string.
- Save transcript as .txt.
- Parse transcript into sequential speaker blocks: [{"speaker": ..., "text": ...}, ...]
- Save speaker blocks as .csv.

Dependencies:
    pip install playwright beautifulsoup4
    playwright install

Notes:
- This module assumes transcript fragments are returned as HTML and include the cache-buster param
  org.htmx.cache-buster=transcriptsContent (as observed on discountingcashflows.com).
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


HTMX_MARKER_DEFAULT = "org.htmx.cache-buster=transcriptsContent"
TRANSCRIPT_CONTAINER_SELECTOR_DEFAULT = "#transcriptsContent"


@dataclass(frozen=True)
class SpeakerBlock:
    speaker: str
    text: str


class TranscriptScrapeError(RuntimeError):
    pass


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)


def _normalize_whitespace(s: str) -> str:
    s = re.sub(r"\r\n|\r", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _capture_htmx_fragments(
    url: str,
    *,
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_ms: int = 3_000,
    max_clicks: int = 50,
    transcript_container_selector: str = TRANSCRIPT_CONTAINER_SELECTOR_DEFAULT,
    htmx_marker: str = HTMX_MARKER_DEFAULT,
    extra_click_selectors: Optional[Sequence[str]] = None,
) -> Dict[int, str]:
    """
    Loads `url` in Playwright and captures HTMX XHR fragments whose response URLs contain `htmx_marker`.
    Returns {fragment_index: fragment_html}.
    """

    fragments: Dict[int, str] = {}

    # Extract numeric suffix (…/1/?…, …/2/?…) if present.
    # If absent, default index 0.
    frag_index_re = re.compile(
        r"/transcripts/\d{4}/\d+/(?:([0-9]+)/)?\?org\.htmx\.cache-buster=transcriptsContent"
    )

    def parse_fragment_index(resp_url: str) -> int:
        m = frag_index_re.search(resp_url)
        if m and m.group(1):
            try:
                return int(m.group(1))
            except ValueError:
                return 0
        return 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        def on_response(resp):
            try:
                u = resp.url
                if htmx_marker not in u:
                    return
                if resp.status != 200:
                    return

                body = resp.text()
                if not body:
                    return
                # Safety filter: sometimes the server returns a full 404 template
                if "Page Not Found" in body and "404" in body:
                    return

                idx = parse_fragment_index(u)
                fragments[idx] = body
            except Exception:
                # Keep listener robust: never crash scraping due to a single response issue.
                return

        page.on("response", on_response)

        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_selector(transcript_container_selector, timeout=timeout_ms)

        # Allow follow-on HTMX requests to complete
        page.wait_for_timeout(settle_ms)

        # Try to trigger additional HTMX payloads if paginated
        # Default heuristics + optional extra selectors
        selectors = list(extra_click_selectors or [])
        # Heuristic fallbacks (text based)
        selectors.extend(
            [
                "text=/^Next$/i",
                "text=/load more/i",
                "text=/more$/i",
                "text=/continue/i",
            ]
        )

        for _ in range(max_clicks):
            clicked = False
            for sel in selectors:
                loc = page.locator(sel).first
                try:
                    if loc.count() and loc.is_visible():
                        loc.click()
                        page.wait_for_timeout(1_250)
                        clicked = True
                        break
                except Exception:
                    # ignore and move to next selector
                    continue
            if not clicked:
                break

        browser.close()

    return fragments


def get_transcript_html(
    url: str,
    *,
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_ms: int = 3_000,
    max_clicks: int = 50,
    extra_click_selectors: Optional[Sequence[str]] = None,
) -> str:
    """
    Returns the combined transcript HTML (concatenated HTMX fragments in index order).
    """
    fragments = _capture_htmx_fragments(
        url,
        headless=headless,
        timeout_ms=timeout_ms,
        settle_ms=settle_ms,
        max_clicks=max_clicks,
        extra_click_selectors=extra_click_selectors,
    )

    if not fragments:
        raise TranscriptScrapeError(
            f"No HTMX transcript fragments captured for URL: {url}. "
            f"Either the markup changed or the transcript is gated."
        )

    ordered_html = "\n".join(fragments[k] for k in sorted(fragments.keys()))
    return ordered_html


def get_transcript_text(
    url: str,
    *,
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_ms: int = 3_000,
    max_clicks: int = 50,
    extra_click_selectors: Optional[Sequence[str]] = None,
) -> str:
    """
    Returns the transcript as a single normalized string.
    """
    html = get_transcript_html(
        url,
        headless=headless,
        timeout_ms=timeout_ms,
        settle_ms=settle_ms,
        max_clicks=max_clicks,
        extra_click_selectors=extra_click_selectors,
    )
    text = _html_to_text(html)
    return _normalize_whitespace(text)


def save_transcript_txt(
    url: str,
    output_path: Union[str, Path],
    *,
    encoding: str = "utf-8",
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_ms: int = 3_000,
    max_clicks: int = 50,
    extra_click_selectors: Optional[Sequence[str]] = None,
) -> Path:
    """
    Scrapes transcript and saves it to a .txt file. Returns the Path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    text = get_transcript_text(
        url,
        headless=headless,
        timeout_ms=timeout_ms,
        settle_ms=settle_ms,
        max_clicks=max_clicks,
        extra_click_selectors=extra_click_selectors,
    )

    output_path.write_text(text, encoding=encoding)
    return output_path


def _parse_speaker_blocks_from_html(html: str) -> List[SpeakerBlock]:
    """
    Attempts to parse sequential speaker blocks from transcript fragment HTML.

    Strategy:
    - Walk through the DOM in document order.
    - Treat "speaker" nodes as headings/bolded labels.
    - Accumulate following paragraphs/list items until next speaker.

    This is heuristic by necessity; you may want to customize once you see the exact fragment markup.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Candidate nodes that often represent speaker labels:
    # - headings
    # - strong/b tags at start of a block
    speaker_like_selectors = [
        "h1", "h2", "h3", "h4", "h5", "h6",
        "strong", "b",
    ]

    # Flatten content into an ordered list of "events"
    # We consider block-level elements in reasonable reading order.
    block_selectors = [
        "h1", "h2", "h3", "h4", "h5", "h6",
        "p", "li", "div", "section", "blockquote",
    ]
    blocks = soup.select(",".join(block_selectors))

    speaker_blocks: List[SpeakerBlock] = []
    current_speaker: Optional[str] = None
    current_text_parts: List[str] = []

    def flush():
        nonlocal current_speaker, current_text_parts
        if current_speaker and current_text_parts:
            text = _normalize_whitespace("\n".join(current_text_parts))
            if text:
                speaker_blocks.append(SpeakerBlock(speaker=current_speaker, text=text))
        current_text_parts = []

    def looks_like_speaker_label(text: str) -> bool:
        t = text.strip()
        if not t:
            return False
        # Heuristics: speaker names tend to be short, title-case, and not full sentences
        if len(t) > 60:
            return False
        if t.endswith((".", "?", "!", ":")) and len(t.split()) > 6:
            return False
        # Common transcript speaker patterns
        # e.g. "Timothy Cook", "Operator", "Suhasini Chandramouli"
        # Avoid treating dates/titles as speakers
        if re.search(r"\b(Fiscal|Quarter|FY|October|November|December|January|February|March|April|May|June|July|August|September)\b", t):
            return False
        # If it has multiple lines, less likely to be a speaker
        if "\n" in t:
            return False
        return True

    for el in blocks:
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue

        tag = el.name.lower()

        # Strong/b inside a paragraph often includes the speaker name.
        if tag in ["p", "div", "section", "blockquote"]:
            strong = el.find(["strong", "b"])
            if strong:
                strong_txt = strong.get_text(" ", strip=True)
                # If the strong text looks like a label, treat it as a speaker
                if looks_like_speaker_label(strong_txt):
                    flush()
                    current_speaker = strong_txt.rstrip(":").strip()
                    # Remove the strong label from the paragraph text and keep the remainder
                    remainder = txt.replace(strong_txt, "", 1).lstrip(" :–-").strip()
                    if remainder:
                        current_text_parts.append(remainder)
                    continue

        # Headings are a strong speaker signal
        if tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            if looks_like_speaker_label(txt):
                flush()
                current_speaker = txt.rstrip(":").strip()
                continue

        # Otherwise, this is content attributed to the current speaker if one exists
        if current_speaker:
            current_text_parts.append(txt)

    flush()

    # If we failed to detect speakers, fall back to one block with "Unknown"
    if not speaker_blocks:
        all_text = _normalize_whitespace(soup.get_text("\n", strip=True))
        if all_text:
            speaker_blocks = [SpeakerBlock(speaker="Unknown", text=all_text)]

    return speaker_blocks


def get_transcript_speaker_blocks(
    url: str,
    *,
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_ms: int = 3_000,
    max_clicks: int = 50,
    extra_click_selectors: Optional[Sequence[str]] = None,
) -> List[SpeakerBlock]:
    """
    Returns the transcript as sequential speaker blocks: [{"speaker": ..., "text": ...}, ...]
    """
    html = get_transcript_html(
        url,
        headless=headless,
        timeout_ms=timeout_ms,
        settle_ms=settle_ms,
        max_clicks=max_clicks,
        extra_click_selectors=extra_click_selectors,
    )
    return _parse_speaker_blocks_from_html(html)


def save_transcript_csv(
    url: str,
    output_path: Union[str, Path],
    *,
    encoding: str = "utf-8",
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_ms: int = 3_000,
    max_clicks: int = 50,
    extra_click_selectors: Optional[Sequence[str]] = None,
) -> Path:
    """
    Scrapes transcript speaker blocks and saves them to a .csv with columns:
      - sequence
      - speaker
      - text
    Returns the Path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    blocks = get_transcript_speaker_blocks(
        url,
        headless=headless,
        timeout_ms=timeout_ms,
        settle_ms=settle_ms,
        max_clicks=max_clicks,
        extra_click_selectors=extra_click_selectors,
    )

    with output_path.open("w", newline="", encoding=encoding) as f:
        w = csv.writer(f)
        w.writerow(["sequence", "speaker", "text"])
        for i, b in enumerate(blocks, start=1):
            w.writerow([i, b.speaker, b.text])

    return output_path
