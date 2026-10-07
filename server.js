import http from "node:http";
import {spawn} from "node:child_process";
import {readFile} from "node:fs/promises";
import path from "node:path";
import {fileURLToPath} from "node:url";

const dir = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 8080;
const YTDLP = process.env.YTDLP || "yt-dlp";
const HOSTS = {
  tiktok: /(^|\.)tiktok\.com$/,
  instagram: /(^|\.)instagram\.com$/,
  facebook: /(^|\.)(facebook\.com|fb\.watch)$/,
  x: /(^|\.)(x\.com|twitter\.com)$/
};
const NAMES = {tiktok: "TikTok", instagram: "Instagram", facebook: "Facebook", x: "X"};

// allow only the four supported platforms (also blocks SSRF to internal hosts)
function check(raw) {
  try {
    const u = new URL(raw);
    if (!/^https?:$/.test(u.protocol)) return null;
    const k = Object.keys(HOSTS).find(k => HOSTS[k].test(u.hostname));
    return k ? {href: u.href, k} : null;
  } catch { return null; }
}

const hits = new Map();
function limited(ip) {
  const n = Date.now(), a = (hits.get(ip) || []).filter(t => n - t < 60000);
  a.push(n); hits.set(ip, a);
  return a.length > 12; // 12 requests per minute per IP
}

function run(args, ms = 30000) {
  return new Promise((resolve, reject) => {
    const p = spawn(YTDLP, args, {stdio: ["ignore", "pipe", "pipe"]});
    let out = "", err = "";
    const t = setTimeout(() => p.kill("SIGKILL"), ms);
    p.stdout.on("data", d => { out += d; if (out.length > 20e6) p.kill("SIGKILL"); });
    p.stderr.on("data", d => { err += d; });
    p.on("close", code => {
      clearTimeout(t);
      if (code === 0) resolve(out);
      else { console.error("yt-dlp ERROR:", err.slice(-800)); reject(new Error("extract failed")); }
    });
    p.on("error", e => { console.error("yt-dlp SPAWN ERROR:", e.message); reject(e); });
  });
}

const json = (res, code, obj) => {
  res.writeHead(code, {"Content-Type": "application/json; charset=utf-8"});
  res.end(JSON.stringify(obj));
};

async function extract(req, res) {
  let body = "";
  for await (const c of req) { body += c; if (body.length > 4096) return json(res, 413, {error: "too large"}); }
  let url; try { url = JSON.parse(body).url; } catch { return json(res, 400, {error: "bad request"}); }
  const c = check(url);
  if (!c) return json(res, 400, {error: "unsupported link"});
  try {
    const info = JSON.parse(await run(["-J", "--no-playlist", "--no-warnings", c.href]));
    const heights = (info.formats || []).filter(f => f.vcodec && f.vcodec !== "none" && f.height).map(f => f.height);
    const max = Math.max(0, ...heights);
    const q = [1080, 720, 480, 360].filter(h => h <= max).slice(0, 3);
    if (!q.length && max) q.push(max);
    const enc = encodeURIComponent(c.href);
    const formats = q.map((h, i) => ({label: `MP4 ${h}p`, note: i === 0 && h >= 720 ? "HD" : "", url: `/api/download?url=${enc}&q=${h}`}));
    formats.push({label: "MP3", note: "მხოლოდ ხმა", url: `/api/download?url=${enc}&q=mp3`});
    const d = Math.round(info.duration || 0);
    json(res, 200, {
      platform: NAMES[c.k], title: info.title || NAMES[c.k],
      thumbnail: info.thumbnail || "", duration: d ? `${Math.floor(d / 60)}:${String(d % 60).padStart(2, "0")}` : "",
      formats
    });
  } catch { json(res, 422, {error: "video not found or not public"}); }
}

function download(req, res, params) {
  const c = check(params.get("url") || "");
  const q = params.get("q") || "";
  if (!c || !(q === "mp3" || /^\d{3,4}$/.test(q))) return json(res, 400, {error: "bad request"});
  const mp3 = q === "mp3";
  const fmt = mp3 ? "ba/b" : `b[height<=${q}][ext=mp4]/b[height<=${q}]/b`;
  const yt = spawn(YTDLP, ["-f", fmt, "--no-playlist", "--no-warnings", "-o", "-", c.href], {stdio: ["ignore", "pipe", "ignore"]});
  const procs = [yt]; let out = yt.stdout;
  if (mp3) {
    const ff = spawn("ffmpeg", ["-loglevel", "error", "-i", "pipe:0", "-vn", "-f", "mp3", "pipe:1"], {stdio: ["pipe", "pipe", "ignore"]});
    yt.stdout.pipe(ff.stdin); procs.push(ff); out = ff.stdout;
    ff.stdin.on("error", () => {});
  }
  res.writeHead(200, {
    "Content-Type": mp3 ? "audio/mpeg" : "video/mp4",
    "Content-Disposition": `attachment; filename="moon-${mp3 ? "audio.mp3" : "video.mp4"}"`
  });
  out.pipe(res);
  const kill = () => procs.forEach(p => p.kill("SIGKILL"));
  res.on("close", kill);
  setTimeout(kill, 5 * 60 * 1000);
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();
  try {
    if (u.pathname === "/api/health") return json(res, 200, {ok: true});
    if (u.pathname === "/api/extract" && req.method === "POST") return limited(ip) ? json(res, 429, {error: "too many requests"}) : extract(req, res);
    if (u.pathname === "/api/download" && req.method === "GET") return limited(ip) ? json(res, 429, {error: "too many requests"}) : download(req, res, u.searchParams);
    if (u.pathname === "/" || u.pathname === "/index.html") {
      res.writeHead(200, {"Content-Type": "text/html; charset=utf-8"});
      return res.end(await readFile(path.join(dir, "public/index.html")));
    }
    json(res, 404, {error: "not found"});
  } catch { json(res, 500, {error: "server error"}); }
}).listen(PORT, () => console.log(`MOON running on :${PORT}`));
