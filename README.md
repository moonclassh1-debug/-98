# MOON SOCIAL VIDEO DOWNLOAD

## Local run
1. Install Node 18+, ffmpeg and yt-dlp (`pip install -U yt-dlp`)
2. `npm start` then open http://localhost:8080

## Deploy (Google Cloud Run example)
gcloud run deploy moon --source . --region europe-west1 --allow-unauthenticated

Notes
- Keep yt-dlp updated; platforms change often.
- Only public videos from TikTok, Instagram, Facebook and X are accepted.
- Replace dmca@example.com and hello@example.com in public/index.html with your real emails.
