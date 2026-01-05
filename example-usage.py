from dcf_transcripts import (
    get_transcript_text,
    save_transcript_txt,
    get_transcript_speaker_blocks,
    save_transcript_csv,
    ScrapeConfig,
)

url = "https://discountingcashflows.com/company/AAPL/transcripts/2025/4/"

# Start with no cookies (fastest / simplest)
cfg = ScrapeConfig()

text = get_transcript_text(url, cfg)
print(text[:500])

save_transcript_txt(
    url,
    "AAPL_2025_Q4.txt",
    output_dir="./data/test-transcripts/raw"
)

blocks = get_transcript_speaker_blocks(url, cfg)
print(blocks[0])

save_transcript_csv(
    url,
    "AAPL_2025_Q4.csv",
    output_dir="./data/test-transcripts/parsed"
)
