FROM node:20-slim
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip ffmpeg ca-certificates \
 && pip3 install --break-system-packages -U yt-dlp && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
ENV PORT=8080
EXPOSE 8080
CMD ["node","server.js"]
