from __future__ import annotations

import csv
import random
import re
import time
from pathlib import Path
from typing import Iterable, Optional, Union

from dcf_transcripts import (
    ScrapeConfig,
    TranscriptScrapeError,
    get_transcript_html,
    get_transcript_text,
    is_rate_limited_message,
    save_transcript_csv_from_blocks,
    save_transcript_txt,
    save_transcript_csv,
    save_transcript_txt_from_text,
    speaker_blocks_from_text,
    transcript_text_from_html,
)

# ---------- Helpers ----------

_LIMIT_REACHED_PATTERNS = (
    "Request Limit Reached",
    "Forbidden - Request Limit Reached",
    "You seem to have reached your request limit",
)

def _is_limit_reached_message(text: str) -> bool:
    t = (text or "").lower()
    return any(p.lower() in t for p in _LIMIT_REACHED_PATTERNS)

def _sleep_with_jitter(base_s: float, jitter_s: float) -> None:
    if base_s <= 0 and jitter_s <= 0:
        return
    time.sleep(max(0.0, base_s + random.random() * max(0.0, jitter_s)))

def _backoff_delay_s(attempt: int, base: float = 15.0, cap: float = 600.0) -> float:
    # 15s, 30s, 60s, 120s, ... capped at 10m, plus jitter
    delay = min(cap, base * (2 ** attempt))
    return delay * (0.7 + random.random() * 0.6)


def _read_tickers_from_sandp_csv(csv_path: Union[str, Path]) -> list[str]:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Ticker file not found: {csv_path}")

    lines = [ln.strip() for ln in csv_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        return []

    header = lines[0].lower()
    if "act_symbol" not in header:
        raise ValueError(f"Expected header 'act_symbol' in {csv_path}, got: {lines[0]}")

    tickers: list[str] = []
    for ln in lines[1:]:
        t = ln.split(",")[0].strip()
        if t:
            tickers.append(t)
    return tickers


def _safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def _already_processed(ticker_dir: Path, stem: str, save_txt: bool, save_csv: bool) -> bool:
    """
    Determine whether a (ticker, year, quarter) has already been processed,
    based on existence of expected output files.
    """
    expected = []
    if save_txt:
        expected.append(ticker_dir / f"{stem}.txt")
    if save_csv:
        expected.append(ticker_dir / f"{stem}.csv")

    # If neither output is requested, treat as not processed
    if not expected:
        return False

    return all(p.exists() and p.stat().st_size > 0 for p in expected)


class RateLimitReached(RuntimeError):
    """Raised when the site returns the 'Request Limit Reached' message."""
    pass


# ---------- Main function ----------

def fetch_all_transcripts_for_year(
    year: int,
    output_dir: Union[str, Path],
    *,
    tickers_csv_path: Union[str, Path] = "tickers/sandp.csv",
    quarters: Iterable[int] = (1, 2, 3, 4),
    save_txt: bool = True,
    save_csv: bool = True,
    cfg: Optional[ScrapeConfig] = None,
    sleep_s: float = 0.0,
    # NEW (optional; does not break existing callers)
    jitter_s: float = 0.35,
    max_rate_limit_retries: int = 5,
) -> None:
    """
    Changes vs prior version:
    - Single network fetch per ticker/quarter (no refetch for txt/csv).
    - Exponential backoff retries if rate-limited; fails after retries.
    - Skips already-processed outputs as before.
    """
    cfg = cfg or ScrapeConfig()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tickers = _read_tickers_from_sandp_csv(tickers_csv_path)
    if not tickers:
        raise ValueError(f"No tickers found in {tickers_csv_path}")

    for raw_ticker in tickers:
        ticker_safe = _safe_filename(raw_ticker)
        ticker_dir = output_dir / ticker_safe
        ticker_dir.mkdir(parents=True, exist_ok=True)

        for q in quarters:
            url = f"https://discountingcashflows.com/company/{raw_ticker}/transcripts/{year}/{q}/"
            stem = f"{ticker_safe}_{year}_Q{q}"

            # Skip if already processed
            if _already_processed(ticker_dir, stem, save_txt, save_csv):
                print(f"[SKIP-DONE] {raw_ticker} {year} Q{q}")
                continue

            attempt = 0
            while True:
                try:
                    # ONE fetch only
                    html = get_transcript_html(url, cfg)
                    text = transcript_text_from_html(html)

                    # Detect rate limit page
                    if is_rate_limited_message(text):
                        if attempt >= max_rate_limit_retries:
                            raise RateLimitReached(
                                f"Rate limit reached (exhausted retries) at {raw_ticker} {year} Q{q}"
                            )
                        delay = _backoff_delay_s(attempt)
                        print(f"[RATE-LIMIT] {raw_ticker} {year} Q{q} -> sleeping {delay:.1f}s (attempt {attempt+1})")
                        time.sleep(delay)
                        attempt += 1
                        continue

                    # Save outputs without refetching
                    if save_txt:
                        save_transcript_txt_from_text(
                            text,
                            f"{stem}.txt",
                            output_dir=ticker_dir,
                        )

                    if save_csv:
                        blocks = speaker_blocks_from_text(text)
                        save_transcript_csv_from_blocks(
                            blocks,
                            f"{stem}.csv",
                            output_dir=ticker_dir,
                        )

                    print(f"[OK] {raw_ticker} {year} Q{q}")
                    break  # success

                except TranscriptScrapeError as e:
                    msg = str(e)
                    # Some rate-limit responses may surface as errors; treat similarly
                    if is_rate_limited_message(msg):
                        if attempt >= max_rate_limit_retries:
                            raise RateLimitReached(
                                f"Rate limit reached (exhausted retries) at {raw_ticker} {year} Q{q}"
                            ) from e
                        delay = _backoff_delay_s(attempt)
                        print(f"[RATE-LIMIT] {raw_ticker} {year} Q{q} -> sleeping {delay:.1f}s (attempt {attempt+1})")
                        time.sleep(delay)
                        attempt += 1
                        continue

                    print(f"[SKIP] {raw_ticker} {year} Q{q} -> {e}")
                    break  # skip missing/gated transcript

                except RateLimitReached:
                    # Fail the whole run as requested once retries are exhausted
                    raise

                except Exception as e:
                    # Unexpected errors: do not loop forever
                    print(f"[ERR] {raw_ticker} {year} Q{q} -> {type(e).__name__}: {e}")
                    break

            # Gentle pacing between successful (or skipped) items
            _sleep_with_jitter(sleep_s, jitter_s)