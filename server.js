import http from "node:http";
import {spawn, execFile} from "node:child_process";
import {readFile, readdir, rm} from "node:fs/promises";
import {copyFileSync, createReadStream} from "node:fs";
import os from "node:os";
import crypto from "node:crypto";
import path from "node:path";
import {fileURLToPath} from "node:url";

const dir = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 8080;
const YTDLP = process.env.YTDLP || "yt-dlp";
const PROXY = process.env.YTDLP_PROXY || "";            // optional: residential proxy for YouTube/Instagram/Facebook
const JS_RT = process.env.YTDLP_JS_RUNTIME ?? "node";   // YouTube needs a JS runtime in recent yt-dlp; this server runs on node. Set to "off" to disable
const MAX_JOBS = Number(process.env.MAX_JOBS || 3);     // parallel merge-to-file downloads
const MAX_YT_SEC = Number(process.env.MAX_YT_MIN || 60) * 60;
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

const HOSTS = {
  tiktok: /(^|\.)tiktok\.com$/,
  youtube: /(^|\.)(youtube\.com|youtu\.be|youtube-nocookie\.com)$/,
  instagram: /(^|\.)(instagram\.com|instagr\.am)$/,
  facebook: /(^|\.)(facebook\.com|fb\.watch|fb\.com)$/,
  x: /(^|\.)(x\.com|twitter\.com)$/
};
const NAMES = {tiktok: "TikTok", youtube: "YouTube", instagram: "Instagram", facebook: "Facebook", x: "X"};

// Instagram posts, Facebook posts and age/bot-gated YouTube videos usually need a logged-in session.
// Put a Netscape-format cookies.txt in a Render "Secret File" and set YTDLP_COOKIES to its path.
const COOKIES = path.join(os.tmpdir(), "moon-cookies.txt");
let hasCookies = false;
if (process.env.YTDLP_COOKIES) {
  try { copyFileSync(process.env.YTDLP_COOKIES, COOKIES); hasCookies = true; }  // copy: yt-dlp rewrites the file
  catch (e) { console.error("cookies not loaded:", e.message); }
}

// TikTok blocks plain requests from datacenter IPs; impersonating a real browser helps
const base = k => [
  ...(k === "tiktok" ? ["--impersonate", "chrome"] : []),
  ...(hasCookies ? ["--cookies", COOKIES] : []),
  ...(PROXY ? ["--proxy", PROXY] : []),
  ...(k === "youtube" && JS_RT && JS_RT !== "off" ? ["--js-runtimes", JS_RT] : [])
];

function ytVideo(u) {
  const p = u.pathname;
  if (/(^|\.)youtu\.be$/.test(u.hostname)) return p.length > 1;
  return (p === "/watch" && u.searchParams.has("v")) || /^\/(shorts|live|embed|v)\/[\w-]{6,}/.test(p);
}

// allow only the supported platforms (also blocks SSRF to internal hosts)
function check(raw) {
  try {
    const u = new URL(raw);
    if (!/^https?:$/.test(u.protocol)) return null;
    const k = Object.keys(HOSTS).find(k => HOSTS[k].test(u.hostname));
    if (!k) return null;
    if (k === "instagram") {            // m.instagram.com / instagr.am are not understood by yt-dlp; tracking params (igsh, utm) are useless
      u.hostname = "www.instagram.com"; u.search = ""; u.hash = "";
    }
    if (k === "facebook" && /(^|\.)fb\.com$/.test(u.hostname)) u.hostname = "www.facebook.com";
    if (k === "youtube" && !ytVideo(u)) return null;   // channels / playlists / home page are not single videos
    return {href: u.href, k};
  } catch { return null; }
}

// "Share" links from the Instagram/Facebook apps are redirects to the real post; follow them
// ourselves, and only through allowed hosts, so yt-dlp gets the canonical post/reel URL.
async function resolve(href, k) {
  if ((k !== "facebook" && k !== "instagram") || !/\/share\//.test(href)) return href;
  let cur = href;
  try {
    for (let i = 0; i < 4; i++) {
      const r = await fetch(cur, {redirect: "manual", headers: {"User-Agent": UA}, signal: AbortSignal.timeout(8000)});
      const loc = r.headers.get("location");
      if (r.status < 300 || r.status >= 400 || !loc) break;
      const next = new URL(loc, cur);
      if (!check(next.href) || /^\/(login|accounts\/login)/.test(next.pathname)) break;
      cur = next.href;
      if (!/\/share\//.test(cur)) break;
    }
  } catch {}
  return cur;
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
      else {
        console.error("yt-dlp ERROR:", err.slice(-800));
        const e = new Error("extract failed"); e.stderr = err; reject(e);
      }
    });
    p.on("error", e => { console.error("yt-dlp SPAWN ERROR:", e.message); reject(e); });
  });
}

const json = (res, code, obj) => {
  res.writeHead(code, {"Content-Type": "application/json; charset=utf-8"});
  res.end(JSON.stringify(obj));
};

// why did yt-dlp fail? the page shows a different message for each
function why(err = "") {
  if (/no video|not a video|does not contain a video/i.test(err)) return "novideo";
  if (/login|log in|logged[- ]in|sign in|cookies|rate-limit|not granting access|empty media|private|not a bot|confirm your age|restricted|checkpoint|authenticat/i.test(err)) {
    console.error("HINT: platform wants a login. Set YTDLP_COOKIES (and update yt-dlp).");
    return "login";
  }
  return "notfound";
}

const VIDEO_EXT = /^(mp4|webm|mov|m4v|mkv)$/i;
const heightsOf = e => (e.formats || []).filter(f => f.vcodec && f.vcodec !== "none" && (f.height || f.width))
  .map(f => Math.min(f.width || f.height, f.height || f.width));
const isVideo = e => heightsOf(e).length > 0 || ((e.formats || []).length > 0 && VIDEO_EXT.test(e.ext || ""));

// a post with several items (Instagram carousel, tweet with 2 videos) comes back as a playlist:
// keep only the items that are videos and remember their position for --playlist-items
function videosOf(info) {
  const list = info.entries ? info.entries.filter(Boolean) : [info];
  return list.map((e, i) => ({e, i: i + 1})).filter(v => isVideo(v.e));
}

async function extract(req, res) {
  let body = "";
  for await (const c of req) { body += c; if (body.length > 4096) return json(res, 413, {error: "too large"}); }
  let url; try { url = JSON.parse(body).url; } catch { return json(res, 400, {error: "bad request"}); }
  const c = check(url);
  if (!c) return json(res, 400, {error: "unsupported link"});
  try {
    c.href = await resolve(c.href, c.k);
    const info = JSON.parse(await run(["-J", "--no-playlist", "--playlist-end", "10", "--no-warnings", ...base(c.k), c.href], c.k === "youtube" ? 60000 : 30000));
    if (info.is_live) return json(res, 422, {error: "live", code: "live"});
    const vids = videosOf(info);
    if (!vids.length) return json(res, 422, {error: "no video", code: "novideo"});
    const first = vids[0].e;
    if (c.k === "youtube" && (first.duration || 0) > MAX_YT_SEC) return json(res, 422, {error: "too long", code: "toolong"});

    const enc = encodeURIComponent(c.href), multi = vids.length > 1, formats = [];
    for (const {e, i} of vids) {
      const max = Math.max(0, ...heightsOf(e));
      let q = [1080, 720, 480, 360].filter(h => h <= max * 1.04).slice(0, multi ? 1 : 3);
      if (!q.length) q = [max || "best"];
      const at = info.entries ? `&i=${i}` : "";
      q.forEach((h, n) => formats.push({
        label: h === "best" ? "MP4" : `MP4 ${h}p`,
        note: multi ? `ვიდეო ${formats.length + 1}` : (n === 0 && h >= 720 ? "HD" : ""),
        url: `/api/download?url=${enc}&q=${h}${at}`
      }));
    }
    if (!multi) formats.push({label: "MP3", note: "მხოლოდ ხმა", url: `/api/download?url=${enc}&q=mp3${info.entries ? `&i=${vids[0].i}` : ""}`});

    const d = Math.round(first.duration || info.duration || 0);
    json(res, 200, {
      platform: NAMES[c.k], title: first.title || info.title || NAMES[c.k],
      thumbnail: first.thumbnail || info.thumbnail || "", duration: d ? `${Math.floor(d / 60)}:${String(d % 60).padStart(2, "0")}` : "",
      formats
    });
  } catch (err) { json(res, 422, {error: "video not found or not public", code: why(err.stderr)}); }
}

// YouTube, Instagram, Facebook and X serve many videos (feed posts especially) as separate video and audio streams.
// They have to be merged into one mp4, and mp4 cannot be streamed to the browser, so merge into a temp file first,
// send it, delete it. Only TikTok (single muxed file) is streamed directly.
let busy = 0;
async function fileDl(res, c, q, pick) {
  if (busy >= MAX_JOBS) return json(res, 503, {error: "busy", code: "busy"});
  busy++;
  const id = "moon-" + crypto.randomUUID(), tmp = os.tmpdir();
  // -S: prefer the shortest side <= q (works for vertical videos too), h264/aac so it plays everywhere
  const sort = (q === "best" ? "res" : `res:${q}`) + ",vcodec:h264,acodec:m4a";
  // headers go out right away so the browser shows the download and the connection stays alive while we merge
  res.writeHead(200, {"Content-Type": "video/mp4", "Content-Disposition": `attachment; filename="moon-${c.k}.mp4"`});
  let gone = false; res.on("close", () => { gone = true; });
  try {
    await run(["-q", "-f", "bv*+ba/b", "-S", sort, "--merge-output-format", "mp4", "--no-playlist", ...pick, "--no-warnings",
      "-N", "4", "--max-filesize", "700M", ...base(c.k), "-o", path.join(tmp, id + ".%(ext)s"), c.href], 4 * 60 * 1000);
    if (gone) return;
    const f = (await readdir(tmp)).find(n => n.startsWith(id) && n.endsWith(".mp4"));
    if (!f) throw new Error("no output file");
    await new Promise(done => {
      const s = createReadStream(path.join(tmp, f));
      s.on("error", done); s.on("close", done);
      res.on("close", () => s.destroy());
      s.pipe(res);
    });
  } catch { res.destroy(); }
  finally {
    busy--;
    const left = await readdir(tmp).catch(() => []);
    await Promise.all(left.filter(n => n.startsWith(id)).map(n => rm(path.join(tmp, n), {force: true})));
  }
}

function download(req, res, params) {
  const c = check(params.get("url") || "");
  const q = params.get("q") || "", i = params.get("i") || "";
  if (!c || !(q === "mp3" || q === "best" || /^\d{3,4}$/.test(q)) || (i && !/^([1-9]|10)$/.test(i))) return json(res, 400, {error: "bad request"});
  const mp3 = q === "mp3", pick = i ? ["--playlist-items", i] : [];
  if (c.k !== "tiktok" && !mp3) return fileDl(res, c, q, pick);
  const fmt = mp3 ? "ba/b" : q === "best" ? "b[ext=mp4]/b" : `b[height<=${q}][ext=mp4]/b[height<=${q}]/b`;   // TikTok video / any MP3
  const yt = spawn(YTDLP, ["-f", fmt, "--no-playlist", ...pick, "--no-warnings", ...base(c.k), "-o", "-", c.href], {stdio: ["ignore", "pipe", "ignore"]});
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

// Diagnostics: shows the REAL reason a link fails. Enabled only when DIAG_KEY env var is set.
// Open: /api/diag?key=YOUR_KEY&url=<video link>
const DIAG_KEY = process.env.DIAG_KEY || "";
const sh = (cmd, args, ms = 8000) => new Promise(r =>
  execFile(cmd, args, {timeout: ms, maxBuffer: 5e6}, (e, o, er) => r(((o || "") + (er || "")) || String(e || ""))));
async function diag(res, params) {
  if (!DIAG_KEY || params.get("key") !== DIAG_KEY) return json(res, 403, {error: "forbidden"});
  const out = {
    ytdlp: (await sh(YTDLP, ["--version"])).trim(), node: (await sh("node", ["--version"])).trim(),
    ffmpeg: (await sh("ffmpeg", ["-version"])).split("\n")[0],
    cookiesLoaded: hasCookies, proxySet: !!PROXY, jsRuntime: JS_RT
  };
  const c = check(params.get("url") || "");
  if (c) {
    let t = await sh(YTDLP, ["-v", "-s", "--no-playlist", ...base(c.k), c.href], 60000);
    if (PROXY) t = t.split(PROXY).join("***");
    out.test = t.slice(-3500);
  }
  json(res, 200, out);
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();
  try {
    if (u.pathname === "/api/diag") return diag(res, u.searchParams);
    if (u.pathname === "/api/health") return json(res, 200, {ok: true});
    if (u.pathname === "/api/extract" && req.method === "POST") return limited(ip) ? json(res, 429, {error: "too many requests"}) : extract(req, res);
    if (u.pathname === "/api/download" && req.method === "GET") return limited(ip) ? json(res, 429, {error: "too many requests"}) : download(req, res, u.searchParams);
    if (u.pathname === "/" || u.pathname === "/index.html") {
      res.writeHead(200, {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache"});
      return res.end(await readFile(path.join(dir, "public/index.html")));
    }
    json(res, 404, {error: "not found"});
  } catch { json(res, 500, {error: "server error"}); }
}).listen(PORT, () => console.log(`MOON running on :${PORT}`));
