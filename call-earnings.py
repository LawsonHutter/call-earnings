import re
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")


BASE_URL = "https://discountingcashflows.com"


@dataclass
class Match:
    line_no: int
    line: str
    context_before: List[str]
    context_after: List[str]


def build_transcript_url(ticker: str, year: int, quarter: int) -> str:
    # Pattern observed on the transcripts page:
    # /company/AAPL/transcripts/2025/4/  (ticker/year/quarter)  :contentReference[oaicite:1]{index=1}
    return f"{BASE_URL}/company/{ticker.upper()}/transcripts/{int(year)}/{int(quarter)}/"


def fetch_html(url: str, timeout_s: int = 30) -> str:
    # Use a realistic User-Agent; some sites block default python-requests UA.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=timeout_s)
    resp.raise_for_status()

    # print html for debugging
    print(resp.text)

    return resp.text


def html_to_text(html: str) -> str:
    """
    Best-effort extraction of transcript text from a page.
    Works even if the transcript is embedded inside a larger HTML response.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content noise
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Prefer obvious main containers if present
    # (site may return partial HTML into #transcriptsContent via htmx)
    preferred_selectors = [
        "#transcriptsContent",
        "#transcriptsContentWrapper",
        "main",
        ".prose",
        ".card-body",
    ]

    root = None
    for sel in preferred_selectors:
        candidate = soup.select_one(sel)
        if candidate and candidate.get_text(strip=True):
            root = candidate
            break

    if root is None:
        root = soup  # fallback: whole document

    text = root.get_text("\n", strip=True)

    # Normalize whitespace and remove repeated blank lines
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def find_keyword_in_text(
    text: str,
    keyword: str,
    context_lines: int = 2,
    whole_word: bool = False,
) -> List[Match]:
    """
    Returns matches with line numbers + context before/after.
    """
    if not keyword:
        raise ValueError("keyword must be non-empty")

    lines = text.splitlines()

    if whole_word:
        pattern = re.compile(rf"\b{re.escape(keyword)}\b", re.IGNORECASE)
    else:
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)

    hit_indices = [i for i, line in enumerate(lines) if pattern.search(line)]

    results: List[Match] = []
    for i in hit_indices:
        start = max(0, i - context_lines)
        end = min(len(lines), i + context_lines + 1)

        results.append(
            Match(
                line_no=i + 1,
                line=lines[i],
                context_before=lines[start:i],
                context_after=lines[i + 1 : end],
            )
        )

    return results


def search_transcript(
    ticker: str,
    year: int,
    quarter: int,
    keyword: str,
    context_lines: int = 2,
    whole_word: bool = False,
) -> tuple[str, str, List[Match]]:
    """
    Returns (url, transcript_text, matches).
    """
    url = build_transcript_url(ticker, year, quarter)
    html = fetch_html(url)
    # text = html_to_text(html)
    # matches = find_keyword_in_text(
    #     text=text,
    #     keyword=keyword,
    #     context_lines=context_lines,
    #     whole_word=whole_word,
    # )
    # return url, text, matches


def pretty_print_matches(url: str, matches: List[Match]) -> None:
    print(f"Source: {url}\n")
    if not matches:
        print("No matches found.")
        return

    for m in matches:
        print(f"--- line {m.line_no} ---")
        for l in m.context_before:
            print(f"  {l}")
        print(f"> {m.line}")
        for l in m.context_after:
            print(f"  {l}")
        print()


if __name__ == "__main__":
    # Example:
    # AAPL FY2025 Q4 keyword "margin"
    url, text, matches = search_transcript(
        ticker="AAPL",
        year=2025,
        quarter=3,
        keyword="margin",
        context_lines=2,
        whole_word=False,
    )
    # pretty_print_matches(url, matches)
