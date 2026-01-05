from dcf_transcripts import ScrapeConfig
from sandp_transcripts import fetch_all_transcripts_for_year  # adjust import

cfg = ScrapeConfig(
    # Add cookies if you get gated (often not needed):
    # cookies={"csrftoken": "YOUR_VALUE"}
)

fetch_all_transcripts_for_year(
    2025,
    output_dir="./data/2025-transcripts",
    cfg=ScrapeConfig(),
    sleep_s=0.6,
    jitter_s=0.8,
    max_rate_limit_retries=5,
)

