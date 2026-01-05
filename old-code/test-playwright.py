from dcf_transcripts import (
    get_transcript_text,
    save_transcript_txt,
    get_transcript_speaker_blocks,
    save_transcript_csv,
)

url = "https://discountingcashflows.com/company/AAPL/transcripts/2025/4/"

# 1) Return as string
text = get_transcript_text(url)
print(text[:1000])

# 2) Save as .txt
save_transcript_txt(url, "apple_fy2025_q4.txt")

# 3) Return as speaker blocks
blocks = get_transcript_speaker_blocks(url)
print(blocks[0].speaker, blocks[0].text[:200])

# 4) Save as .csv
save_transcript_csv(url, "apple_fy2025_q4.csv")
