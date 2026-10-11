import http from "node:http";
import {spawn, execFile} from "node:child_process";
import {readFile, readdir, rm, writeFile, stat, chmod, rename, mkdir} from "node:fs/promises";
import {copyFileSync, createReadStream, createWriteStream, readFileSync, writeFileSync} from "node:fs";
import {Readable, Transform} from "node:stream";
import {pipeline} from "node:stream/promises";
import os from "node:os";
import crypto from "node:crypto";
import path from "node:path";
import {fileURLToPath} from "node:url";

const dir = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 8080;
let YTDLP = process.env.YTDLP || "yt-dlp";                // replaced by a fresh nightly build at runtime (see updateYtdlp)
const PROXY = process.env.YTDLP_PROXY || "";            // optional: residential proxy for YouTube/Instagram/Facebook
const JS_RT = process.env.YTDLP_JS_RUNTIME ?? "node";   // YouTube needs a JS runtime in recent yt-dlp; this server runs on node. Set to "off" to disable
const MAX_JOBS = Number(process.env.MAX_JOBS || 3);     // parallel downloads/merges
const MAX_QUEUE = Number(process.env.MAX_QUEUE || 30);
const MAX_YT_SEC = Number(process.env.MAX_YT_MIN || 60) * 60;
const AUTO_UPDATE = process.env.YTDLP_AUTO_UPDATE !== "0"; // keep yt-dlp fresh without rebuilding the container
const VYNEX = (process.env.VYNEX_BASE ?? "https://vynex.ai/api/tools/youtube-video").replace(/\/$/, ""); // "off" disables
const YT_MODE = process.env.YT_MODE || "auto";          // auto: yt-dlp first, vynex if YouTube blocks this server | ytdlp | vynex
// Optional extra YouTube source: the owner's own service (youtubetoolkit) through a key-based app API.
// Off until both YTK_BASE and YTK_KEY are set. Paths: YTK_PREFIX + /fetch-video, /generate-download, /progress/:id
const YTK_BASE = (process.env.YTK_BASE || "").replace(/\/$/, "");     // e.g. https://app.youtubetoolkit.com
const YTK_KEY = process.env.YTK_KEY || "";
const YTK_HEADER = process.env.YTK_HEADER || "X-API-Key";
const YTK_PREFIX = process.env.YTK_PREFIX ?? "/api/app";
const YTK_ON = !!(YTK_BASE && YTK_KEY);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";
const TMP = path.join(os.tmpdir(), "moon");
await mkdir(TMP, {recursive: true});

const HOSTS = {
  tiktok: /(^|\.)tiktok\.com$/,
  youtube: /(^|\.)(youtube\.com|youtu\.be|youtube-nocookie\.com)$/,
  instagram: /(^|\.)(instagram\.com|instagr\.am)$/,
  facebook: /(^|\.)(facebook\.com|fb\.watch|fb\.com)$/,
  x: /(^|\.)(x\.com|twitter\.com)$/
};
const NAMES = {tiktok: "TikTok", youtube: "YouTube", instagram: "Instagram", facebook: "Facebook", x: "X"};

// Instagram posts, Facebook posts and age/bot-gated YouTube videos sometimes need a logged-in session.
// Put a Netscape-format cookies.txt in a Render "Secret File" and set YTDLP_COOKIES to its path.
const COOKIES = path.join(TMP, "cookies.txt");
let hasCookies = false;
const cookiesForAll = !!process.env.YTDLP_COOKIES;   // a full cookies.txt is used everywhere; INSTAGRAM_SESSIONID only for Instagram
if (process.env.YTDLP_COOKIES) {
  try { copyFileSync(process.env.YTDLP_COOKIES, COOKIES); hasCookies = true; }  // copy: yt-dlp rewrites the file
  catch (e) { console.error("cookies not loaded:", e.message); }
}
// Simplest way to unlock Instagram reels: paste only the `sessionid` cookie of a (spare) Instagram account into the
// INSTAGRAM_SESSIONID env var. Instagram now hides many reels from logged-out visitors (especially datacenter IPs),
// while the logged-in API returns every public reel/post. A cookies.txt is generated from it automatically.
if (process.env.INSTAGRAM_SESSIONID) {
  const sid = decodeURIComponent(process.env.INSTAGRAM_SESSIONID.trim().replace(/^sessionid=/, ""));
  const uid = (sid.match(/^(\d+)/) || [])[1] || "";
  const exp = Math.floor(Date.now() / 1000) + 365 * 86400;
  const line = (n, v) => `.instagram.com\tTRUE\t/\tTRUE\t${exp}\t${n}\t${v}`;
  let txt = "";
  try { txt = hasCookies ? readFileSync(COOKIES, "utf8").split("\n").filter(l => !/\tsessionid\t|\tds_user_id\t/.test(l)).join("\n") + "\n" : "# Netscape HTTP Cookie File\n"; } catch {}
  txt += [line("sessionid", encodeURIComponent(sid)), uid && line("ds_user_id", uid)].filter(Boolean).join("\n") + "\n";
  try { writeFileSync(COOKIES, txt); hasCookies = true; console.log("Instagram session loaded"); } catch (e) { console.error("session not loaded:", e.message); }
}

// TikTok blocks plain requests from datacenter IPs; impersonating a real browser helps
const base = (k, o = {}) => [
  ...(k === "tiktok" ? ["--impersonate", "chrome"] : []),
  ...(hasCookies && !o.noCookies && (cookiesForAll || k === "instagram") ? ["--cookies", COOKIES] : []),
  ...(PROXY ? ["--proxy", PROXY] : []),
  ...(k === "youtube" && JS_RT && JS_RT !== "off" ? ["--js-runtimes", JS_RT] : [])
];

// ---------------------------------------------------------------- yt-dlp auto-update
// Instagram/YouTube change their internals every few weeks; an old yt-dlp inside the container is the #1 reason
// links "suddenly stop working". Pull the latest nightly zipapp (uses the curl_cffi already installed) on boot and twice a day.
const sh = (cmd, args, ms = 8000) => new Promise(r =>
  execFile(cmd, args, {timeout: ms, maxBuffer: 5e6}, (e, o, er) => r(((o || "") + (er || "")) || String(e || ""))));
let ytVersion = "";
async function updateYtdlp() {
  if (!AUTO_UPDATE) return;
  const dest = path.join(TMP, "yt-dlp"), part = dest + ".part";
  try {
    const r = await fetch("https://github.com/yt-dlp/yt-dlp-nightly-builds/releases/latest/download/yt-dlp", {signal: AbortSignal.timeout(90000)});
    if (!r.ok) throw new Error("HTTP " + r.status);
    const buf = Buffer.from(await r.arrayBuffer());
    if (buf.length < 1e6) throw new Error("download too small");
    await writeFile(part, buf); await chmod(part, 0o755);
    const v = (await sh("python3", [part, "--version"], 30000)).trim();
    if (!/^\d{4}\.\d{2}\.\d{2}/.test(v)) throw new Error("bad binary: " + v.slice(0, 200));
    await rename(part, dest);
    YTDLP = dest; ytVersion = v;
    console.log("yt-dlp updated to", v);
    restartPool();
  } catch (e) { console.error("yt-dlp update failed (keeping current):", e.message); }
}
sh(YTDLP, ["--version"]).then(v => { if (!ytVersion) ytVersion = v.trim(); });
updateYtdlp();
setInterval(updateYtdlp, 12 * 3600e3).unref();

function ytVideo(u) {
  const p = u.pathname;
  if (/(^|\.)youtu\.be$/.test(u.hostname)) return p.length > 1;
  return (p === "/watch" && u.searchParams.has("v")) || /^\/(shorts|live|embed|v)\/[\w-]{6,}/.test(p);
}

// allow only the supported platforms (also blocks SSRF to internal hosts)
function check(raw) {
  try {
    const u = new URL(String(raw).trim());
    if (!/^https?:$/.test(u.protocol)) return null;
    const k = Object.keys(HOSTS).find(k => HOSTS[k].test(u.hostname));
    if (!k) return null;
    if (k === "instagram") {
      // m.instagram.com / instagr.am / tracking params (igsh, utm) confuse yt-dlp. Rewrite every post form
      // (/p/ID, /reel/ID, /reels/ID, /tv/ID, /username/reel/ID) to one canonical URL.
      u.protocol = "https:"; u.hostname = "www.instagram.com"; u.search = ""; u.hash = "";
      const m = u.pathname.match(/\/(p|reels?|tv)\/([\w-]{5,})/);
      if (m && !/^\/share\//.test(u.pathname)) u.pathname = `/${m[1] === "reels" ? "reel" : m[1]}/${m[2]}/`;
    }
    if (k === "facebook" && /(^|\.)fb\.com$/.test(u.hostname)) u.hostname = "www.facebook.com";
    if (k === "youtube" && !ytVideo(u)) return null;   // channels / playlists / home page are not single videos
    return {href: u.href, k};
  } catch { return null; }
}

// "Share" links from the Instagram/Facebook apps (/share/reel/…, /share/p/…, /share/v/…) are redirects to the real post.
// Follow them ourselves, only through allowed hosts. Instagram often answers a datacenter IP with a login redirect
// whose ?next= holds the real /reel/ID path, or with an HTML page whose og:url is the real link — both are handled.
const CRAWLER_UA = "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)";
async function resolve(href, k) {
  if ((k !== "facebook" && k !== "instagram") || !/\/share\//.test(href)) return href;
  const final = h => { const t = check(h); return t && !/\/share\//.test(t.href) ? t.href : null; };
  for (const ua of [UA, CRAWLER_UA]) {
    let cur = href;
    try {
      for (let i = 0; i < 6; i++) {
        const r = await fetch(cur, {redirect: "manual", headers: {"User-Agent": ua, Accept: "text/html", "Accept-Language": "en-US,en;q=0.9"}, signal: AbortSignal.timeout(8000)});
        const loc = r.headers.get("location");
        if (r.status >= 300 && r.status < 400 && loc) {
          const next = new URL(loc, cur);
          if (/^\/(accounts\/)?login/.test(next.pathname)) {
            const n = next.searchParams.get("next");
            const f = n && final(new URL(n, "https://www.instagram.com").href);
            if (f) return f;
            break;
          }
          const ok = check(next.href);
          if (!ok) break;
          if (!/\/share\//.test(ok.href)) return ok.href;
          cur = ok.href;
          continue;
        }
        if (r.ok) {
          const html = (await r.text()).slice(0, 800000).replace(/\\\//g, "/");
          const m = html.match(/<meta[^>]+property="og:url"[^>]+content="([^"]+)"/i) || html.match(/<link[^>]+rel="canonical"[^>]+href="([^"]+)"/i)
            || html.match(/https:\/\/www\.instagram\.com\/(?:reel|p|tv)\/[\w-]+/) || html.match(/https:\/\/www\.facebook\.com\/(?:reel\/\d+|watch\/?\?v=\d+|[^"'\s]+\/videos\/\d+)/);
          const f = m && final((m[1] || m[0]).replace(/&amp;/g, "&"));
          if (f) return f;
        }
        break;
      }
    } catch {}
  }
  return href;
}

const hits = new Map();
function limited(ip) {
  const n = Date.now(), a = (hits.get(ip) || []).filter(t => n - t < 60000);
  a.push(n); hits.set(ip, a);
  return a.length > 20; // 20 requests per minute per IP (extract + start download)
}
setInterval(() => { const n = Date.now(); for (const [ip, a] of hits) if (!a.some(t => n - t < 60000)) hits.delete(ip); }, 120e3).unref();

function run(args, ms = 30000) {
  return new Promise((resolve, reject) => {
    const p = spawn(YTDLP, args, {stdio: ["ignore", "pipe", "pipe"]});
    let out = "", err = "";
    const t = setTimeout(() => p.kill("SIGKILL"), ms);
    p.stdout.on("data", d => { out += d; if (out.length > 40e6) p.kill("SIGKILL"); });
    p.stderr.on("data", d => { err += d; if (err.length > 1e6) err = err.slice(-200000); });
    p.on("close", code => {
      clearTimeout(t);
      if (code === 0) resolve(out);
      else {
        console.error("yt-dlp ERROR:", err.slice(-800));
        const e = new Error("extract failed"); e.stderr = err; reject(e);
      }
    });
    p.on("error", e => { console.error("yt-dlp SPAWN ERROR:", e.message); e.stderr = e.message; reject(e); });
  });
}

// ---------------------------------------------------------------- persistent yt-dlp workers (fast search)
// yt-dlp is imported once per worker process and its sessions/caches are reused, instead of a cold
// `yt-dlp -J` start for every search. Falls back to the CLI automatically if Python workers can't start.
const WORKER_PY = path.join(dir, "ytdl_worker.py");
const POOL_SIZE = Math.max(1, Number(process.env.YTDLP_WORKERS || 1));   // 1 keeps memory low on small instances
const pool = [], waiting = [];
let reqSeq = 0, poolBroken = false, poolCrashes = 0;
function spawnWorker() {
  const zip = YTDLP.startsWith(TMP) ? YTDLP : "";
  const p = spawn("python3", [WORKER_PY, zip], {stdio: ["pipe", "pipe", "pipe"]});
  const w = {p, busy: null, ready: false, buf: "", served: 0, retire: false};
  p.stdout.on("data", d => {
    w.buf += d; let k;
    while ((k = w.buf.indexOf("\n")) >= 0) { const line = w.buf.slice(0, k); w.buf = w.buf.slice(k + 1); onWorkerLine(w, line); }
  });
  p.stderr.on("data", () => {});
  p.stdin.on("error", () => {});
  p.on("error", () => {});
  p.on("close", () => {
    const i = pool.indexOf(w); if (i >= 0) pool.splice(i, 1);
    if (w.busy) { const b = w.busy; w.busy = null; clearTimeout(b.t); b.reject(Object.assign(new Error("worker died"), {stderr: b.err || "ERROR: worker died"})); }
    if (!w.ready && ++poolCrashes >= 3) {
      poolBroken = true;
      console.error("yt-dlp workers can't start — using the CLI");
      waiting.splice(0).forEach(b => b.reject(Object.assign(new Error("pool down"), {poolDown: true, stderr: ""})));
      return;
    }
    setTimeout(fillPool, 300);
  });
  pool.push(w);
}
function fillPool() { if (!poolBroken) while (pool.length < POOL_SIZE) spawnWorker(); }
function onWorkerLine(w, line) {
  let m; try { m = JSON.parse(line); } catch { return; }
  if (m.ready) { w.ready = true; poolCrashes = 0; pumpPool(); return; }
  const b = w.busy; if (!b || m.id !== b.id) return;
  w.busy = null; clearTimeout(b.t); w.served++;
  if (m.ok) b.resolve(m.info);
  else { console.error("yt-dlp ERROR:", String(m.err).slice(-600)); b.reject(Object.assign(new Error("extract failed"), {stderr: String(m.err || "")})); }
  if (w.retire || w.served > 300) w.p.kill();
  pumpPool();
}
function pumpPool() {
  for (const w of pool) {
    if (!w.ready || w.busy || w.retire || !waiting.length) continue;
    const b = waiting.shift();
    w.busy = b;
    b.t = setTimeout(() => { b.err = "ERROR: timeout"; w.p.kill("SIGKILL"); }, b.ms);
    w.p.stdin.write(JSON.stringify({id: b.id, args: b.args, url: b.url}) + "\n");
  }
}
function poolExtract(args, url, ms) {
  if (poolBroken) return Promise.reject(Object.assign(new Error("pool down"), {poolDown: true, stderr: ""}));
  return new Promise((resolve, reject) => { waiting.push({id: ++reqSeq, args, url, ms, resolve, reject}); fillPool(); pumpPool(); });
}
function restartPool() { for (const w of [...pool]) { if (w.busy) w.retire = true; else w.p.kill(); } }
fillPool();

// the line of yt-dlp's error that explains the failure (shown in the app so problems can be diagnosed)
const errLine = (s = "") => {
  const l = String(s).split("\n").map(x => x.trim()).filter(Boolean).reverse().find(x => /ERROR/.test(x)) || "";
  let t = l.replace(/^.*?ERROR:\s*/, "").replace(/^\[[^\]]+\]\s*[\w-]+:\s*/, "").slice(0, 240);
  if (PROXY) t = t.split(PROXY).join("***");
  return t;
};

const json = (res, code, obj) => {
  if (res.headersSent) return res.end();
  res.writeHead(code, {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store"});
  res.end(JSON.stringify(obj));
};

// why did yt-dlp fail? the page/app shows a different message for each
function why(err = "") {
  if (/no video|not a video|does not contain a video|There is no video/i.test(err)) return "novideo";
  if (/login|log in|logged[- ]in|sign in|cookies|rate-limit|not granting access|empty media|private|not a bot|confirm your age|restricted|checkpoint|authenticat/i.test(err)) {
    console.error("HINT: platform wants a login. Set YTDLP_COOKIES (and update yt-dlp).");
    return "login";
  }
  return "notfound";
}
const botBlock = err => /not a bot|sign in to confirm|confirm you.re not|HTTP Error 429|po.?token/i.test(err || "");

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

// ---------------------------------------------------------------- info cache
// The info from /api/extract is reused for the download (yt-dlp --load-info-json): one request to Instagram/YouTube
// instead of two, which avoids most Instagram rate-limit/login walls and makes downloads start faster.
const cache = new Map(); // href -> {t, kind:"ytdlp"|"vynex", info|data, file?}
const CACHE_MS = 25 * 60e3;
async function cachePut(href, entry) {
  entry.t = Date.now();
  if (entry.kind === "ytdlp") {
    entry.file = path.join(TMP, "info-" + crypto.createHash("sha1").update(href).digest("hex") + ".json");
    try { await writeFile(entry.file, JSON.stringify(entry.info)); } catch { entry.file = null; }
  }
  cache.set(href, entry);
}
function cacheGet(href) {
  const e = cache.get(href);
  if (e && Date.now() - e.t < CACHE_MS) return e;
  return null;
}
setInterval(() => {
  const n = Date.now();
  for (const [h, e] of cache) if (n - e.t > CACHE_MS) { cache.delete(h); if (e.file) rm(e.file, {force: true}).catch(() => {}); }
}, 5 * 60e3).unref();

async function ytdlpInfo(c) {
  const T = c.k === "youtube" ? 60000 : 35000;
  const opts = o => ["--no-playlist", "--playlist-end", "10", "--no-warnings", "--socket-timeout", "15", ...base(c.k, o)];
  const one = async o => {
    if (c.k === "youtube") return JSON.parse(await run(["-J", ...opts(o), c.href], T));
    try { return await poolExtract(opts(o), c.href, T); }
    catch (e) { if (e.poolDown) return JSON.parse(await run(["-J", ...opts(o), c.href], T)); throw e; }
  };
  try { return await one(); }
  catch (e) {
    // a stale/flagged session cookie makes Instagram/Facebook answer "checkpoint"/"login" even for public posts:
    // retry once logged out (yt-dlp's logged-out Instagram path works for public reels/posts)
    if (hasCookies && (c.k === "instagram" || c.k === "facebook") && why(e.stderr) !== "novideo") {
      try { return await one({noCookies: true}); } catch {}
    }
    throw e;
  }
}

// ---- YouTube through vynex (fallback when YouTube blocks this server's IP). The video and audio streams it serves
// are separate, so the server merges them with ffmpeg: the user always gets one MP4 WITH sound.
let ytBlockedUntil = 0;
async function vynexInfo(href) {
  const r = await fetch(`${VYNEX}?url=${encodeURIComponent(href)}`, {headers: {Accept: "application/json", "User-Agent": UA, Origin: "https://vynex.ai", Referer: "https://vynex.ai/"}, signal: AbortSignal.timeout(40000)});
  let d = null; try { d = await r.json(); } catch {}
  if (!r.ok || !d || !d.id) { const e = new Error("vynex failed"); e.stderr = (d && (d.error || d.message)) || ("HTTP " + r.status); throw e; }
  return d;
}
// ---- owner's service (key-based app API). Same flow as its website: fetch-video → generate-download → progress → file
async function ytkCall(method, p, body, ms = 40000) {
  const r = await fetch(YTK_BASE + YTK_PREFIX + p, {
    method, headers: {Accept: "application/json", "Content-Type": "application/json", "User-Agent": UA, [YTK_HEADER]: YTK_KEY},
    body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(ms)
  });
  let d = null; try { d = await r.json(); } catch {}
  if (!r.ok || !d) { const e = new Error("ytk failed"); e.stderr = (d && (d.message || d.error)) || ("HTTP " + r.status); e.status = r.status; throw e; }
  return d;
}
const qHeight = q => {
  const t = `${q.label || ""} ${q.value || ""}`;
  if (/\b4k\b|2160/i.test(t)) return 2160;
  if (/\b2k\b|1440/i.test(t)) return 1440;
  const m = t.match(/(\d{3,4})\s*p?/); return m ? +m[1] : 0;
};
const durSec = v => {
  if (typeof v === "number") return v;
  const p = String(v || "").split(":").map(Number);
  return p.length > 1 && p.every(n => !isNaN(n)) ? p.reduce((a, n) => a * 60 + n, 0) : (+v || 0);
};
async function ytkInfo(href) {
  const d = await ytkCall("POST", "/fetch-video", {url: href, mode: "video"});
  const v = d.video || {};
  const qualities = (d.qualities || []).filter(q => !q.type || q.type === "video").map(q => ({...q, height: qHeight(q)})).filter(q => q.height);
  if (!qualities.length) { const e = new Error("ytk no qualities"); e.stderr = "no video"; throw e; }
  return {title: v.title || "YouTube", thumbnail: v.thumbnail || "", channel: v.channel_name || "", duration: +v.duration_seconds || durSec(v.duration), qualities};
}
async function ytkJob(j, d) {
  const want = j.q === "best" ? 1e5 : +j.q;
  const sorted = d.qualities.slice().sort((a, b) => b.height - a.height);
  const q = sorted.find(x => x.height <= want * 1.04) || sorted[sorted.length - 1];
  j.phase = "preparing";
  let r = await ytkCall("POST", "/generate-download", {url: j.c.href, mode: "video", format: q.value, audio_quality: q.audio_quality ?? null,
    title: d.title, thumbnail: d.thumbnail, channel_name: d.channel}, 60000);
  let dl = r.download;
  for (let n = 0; dl && dl.status !== "completed" && n < 40; n++) {
    if (j.state !== "working") return;
    if (dl.status === "failed") break;
    await sleep(n < 3 ? 3000 : 5000);
    r = await ytkCall("GET", "/progress/" + dl.id); dl = r.download;
  }
  if (!dl || dl.status !== "completed" || !dl.download_url) { const e = new Error("ytk not ready"); e.stderr = (dl && dl.error_message) || "ytk not ready"; throw e; }
  const url = new URL(dl.download_url, YTK_BASE);
  const ac = new AbortController(); j.kill = () => ac.abort();
  const res = await fetch(url, {headers: {"User-Agent": UA, ...(url.origin === new URL(YTK_BASE).origin ? {[YTK_HEADER]: YTK_KEY} : {})}, signal: ac.signal});
  if (!res.ok || !res.body) { const e = new Error("ytk file"); e.stderr = "HTTP " + res.status; throw e; }
  const ext = /webm/i.test(res.headers.get("content-type") || "") ? "webm" : "mp4";
  j.total = +res.headers.get("content-length") || 0; j.bytes = 0; j.phase = "downloading";
  await mkdir(j.dir, {recursive: true});
  const count = new Transform({transform(c, _e, cb) { j.bytes += c.length; if (j.total) j.progress = Math.min(99, Math.round(j.bytes / j.total * 100)); cb(null, c); }});
  try { await pipeline(Readable.fromWeb(res.body), count, createWriteStream(path.join(j.dir, "media." + ext))); } finally { j.kill = null; }
}

// Search speed: ask two sources at once and take the first good answer. yt-dlp is preferred (more qualities),
// so when the other source answers first we still give yt-dlp a short grace period.
function preferFirst(a, b, grace) {
  return new Promise((resolve, reject) => {
    let settled = false, aErr = null, bErr = null, bVal = null, t = null;
    const win = (v, who) => { if (settled) return; settled = true; clearTimeout(t); resolve({...v, winner: who}); };
    a.then(v => win(v, "a"), e => {
      aErr = e;
      if (bVal) win(bVal, "b"); else if (bErr && !settled) { settled = true; reject(aErr); }
    });
    b.then(v => { bVal = v; if (aErr) win(v, "b"); else t = setTimeout(() => win(v, "b"), grace); },
      e => { bErr = e; if (aErr && !settled) { settled = true; reject(aErr); } });
  });
}

async function ytInfo(c) {
  if (YTK_ON && YT_MODE !== "ytdlp") {
    try { return {kind: "ytk", data: await ytkInfo(c.href)}; } catch (e) { console.error("ytk:", errLine(e.stderr) || e.message); }
  }
  const ytp = () => ytdlpInfo(c).then(info => ({kind: "ytdlp", info}));
  const vxp = () => vynexInfo(c.href).then(data => ({kind: "vynex", data}));
  if (VYNEX === "off" || YT_MODE === "ytdlp") return ytp();
  if (YT_MODE === "vynex" || Date.now() < ytBlockedUntil) return vxp().catch(() => ytp());
  const a = ytp(), b = vxp();
  a.catch(e => { if (botBlock(e.stderr)) ytBlockedUntil = Date.now() + 30 * 60e3; });
  const r = await preferFirst(a, b, 2500);
  if (r.winner === "b") a.then(e => cachePut(c.href, e)).catch(() => {});   // upgrade the cache when yt-dlp finishes later
  return r;
}

// Instagram fallback that needs no login and answers in ~1 s: the public embed page carries the reel's MP4 (with sound).
const unjson = s => { try { return JSON.parse(`"${s}"`); } catch { return s.replace(/\\\//g, "/").replace(/\\u0026/g, "&"); } };
async function igEmbed(href) {
  const m = href.match(/instagram\.com\/(?:p|reels?|tv)\/([\w-]+)/);
  if (!m) throw Object.assign(new Error("no shortcode"), {stderr: "notfound"});
  const sc = m[1];
  for (const u of [`https://www.instagram.com/p/${sc}/embed/captioned/`, `https://www.instagram.com/reel/${sc}/embed/`]) {
    try {
      const r = await fetch(u, {headers: {"User-Agent": UA, Accept: "text/html", "Accept-Language": "en-US,en;q=0.9"}, signal: AbortSignal.timeout(10000)});
      if (!r.ok) continue;
      const raw = await r.text();
      const html = raw.replace(/\\\\\//g, "/").replace(/\\\\"/g, '"').replace(/\\"/g, '"');
      const vm = html.match(/"video_url":"(https:[^"]+)"/) || raw.match(/<video[^>]+src="(https:[^"]+)"/);
      if (!vm) continue;
      const pick = re => { const x = html.match(re); return x ? unjson(x[1]) : ""; };
      const url = unjson(vm[1]).replace(/&amp;/g, "&");
      const w = +pick(/"dimensions":\{"height":\d+,"width":(\d+)\}/) || 0, h = +pick(/"dimensions":\{"height":(\d+)/) || 0;
      const user = pick(/"owner":\{[^}]*?"username":"([^"]+)"/) || pick(/"username":"([^"]+)"/);
      return {
        title: user ? `Video by ${user}` : "Instagram", uploader: user, duration: +pick(/"video_duration":([\d.]+)/) || 0,
        thumbnail: unjson(pick(/"display_url":"(https:[^"]+)"/)).replace(/&amp;/g, "&"),
        formats: [{format_id: "embed", url, ext: "mp4", vcodec: "h264", acodec: "aac", width: w, height: h}],
      };
    } catch {}
  }
  throw Object.assign(new Error("embed: no video"), {stderr: "notfound"});
}
// Facebook without login: yt-dlp answers "Cannot parse data" when Facebook shows a datacenter IP a login wall.
// The page that Facebook serves to its own link-preview crawler (and the plugin/embed page) still carries the public video.
const fbMeta = (html, p) => {
  const a = html.match(new RegExp(`<meta[^>]+(?:property|name)="${p}"[^>]+content="([^"]*)"`, "i"))
    || html.match(new RegExp(`<meta[^>]+content="([^"]*)"[^>]+(?:property|name)="${p}"`, "i"));
  return a ? a[1].replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&#039;/g, "'") : "";
};
function fbParse(raw) {
  const html = raw.replace(/\\\//g, "/");
  const pick = re => { const m = html.match(re); return m ? unjson(m[1]).replace(/&amp;/g, "&") : ""; };
  const hd = pick(/"(?:browser_native_hd_url|playable_url_quality_hd|hd_src_no_ratelimit|hd_src)":"(https:[^"]+)"/);
  let sd = pick(/"(?:browser_native_sd_url|playable_url|sd_src_no_ratelimit|sd_src)":"(https:[^"]+)"/);
  const og = fbMeta(html, "og:video:secure_url") || fbMeta(html, "og:video:url") || fbMeta(html, "og:video");
  if (!sd && og && !/\/video\/embed|\/plugins\//.test(og) && /fbcdn|\.mp4|video/i.test(og)) sd = og;
  const formats = [];
  if (hd) formats.push({format_id: "hd", url: hd, ext: "mp4", vcodec: "h264", acodec: "aac", height: 720, width: 1280});
  if (sd && sd !== hd) formats.push({format_id: "sd", url: sd, ext: "mp4", vcodec: "h264", acodec: "aac", height: 360, width: 640});
  if (!formats.length) return null;
  const h = +fbMeta(html, "og:video:height") || 0, w = +fbMeta(html, "og:video:width") || 0;
  if (h && !hd) { formats[0].height = h; formats[0].width = w || Math.round(h * 16 / 9); }
  return {
    title: (fbMeta(html, "og:title") || "Facebook video").replace(/\s+/g, " ").slice(0, 120), uploader: "", duration: 0,
    thumbnail: fbMeta(html, "og:image"), formats, referer: "https://www.facebook.com/"
  };
}
const FB_MOBILE = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36";
// A "share" link resolves to .../story.php?story_fbid=ID&id=PAGE (or /reel/ID, /watch/?v=ID, /PAGE/videos/ID): pull the numeric ids out
function fbIds(href) {
  let u; try { u = new URL(href); } catch { return []; }
  const ids = new Set();
  for (const k of ["story_fbid", "v", "video_id", "fbid"]) { const v = u.searchParams.get(k); if (/^\d{8,}$/.test(v || "")) ids.add(v); }
  const m = u.pathname.match(/\/(?:videos|reels?)\/(?:[^/]+\/)?(\d{8,})/); if (m) ids.add(m[1]);
  return [...ids];
}
const fbPageId = href => { try { const v = new URL(href).searchParams.get("id"); return /^\d{6,}$/.test(v || "") ? v : ""; } catch { return ""; } };
// pages that carry the public video without a login: the embed page of the video id (what Facebook's own embed iframe loads),
// the video plugin, and the plain page as seen by the link-preview crawler / a phone
function fbCandidates(href) {
  const plugin = h => `https://www.facebook.com/plugins/video.php?href=${encodeURIComponent(h)}&show_text=false`;
  const page = fbPageId(href), list = [];
  for (const id of fbIds(href)) {
    list.push([`https://www.facebook.com/video/embed?video_id=${id}`, UA]);
    list.push([plugin(`https://www.facebook.com/watch/?v=${id}`), UA]);
    if (page) list.push([plugin(`https://www.facebook.com/${page}/videos/${id}`), UA]);
  }
  list.push([href, CRAWLER_UA], [href.replace("://www.facebook.com", "://m.facebook.com"), FB_MOBILE], [plugin(href), UA]);
  return list;
}
const fbGet = async (u, ua) => {
  const r = await fetch(u, {headers: {"User-Agent": ua, Accept: "text/html", "Accept-Language": "en-US,en;q=0.9"}, redirect: "follow", signal: AbortSignal.timeout(12000)});
  if (!r.ok) throw new Error("HTTP " + r.status);
  return (await r.text()).slice(0, 2e6);
};
async function fbOg(href) {
  const probes = fbCandidates(href).map(([u, ua]) => fbGet(u, ua).then(html => { const info = fbParse(html); if (!info) throw new Error("no video"); return info; }));
  try { return await Promise.any(probes); }
  catch { throw Object.assign(new Error("fb: no video"), {stderr: "notfound"}); }
}
async function fbInfo(c) {
  const id = fbIds(c.href)[0];
  // yt-dlp on the page itself gets a login wall from a datacenter IP; its embed-page path often does not
  const a = ytdlpInfo(c).catch(e => id ? ytdlpInfo({...c, href: `https://www.facebook.com/video/embed?video_id=${id}`}).catch(() => { throw e; }) : Promise.reject(e))
    .then(info => ({kind: "ytdlp", info}));
  const b = fbOg(c.href).then(info => ({kind: "direct", info}));
  const r = await preferFirst(a, b, 2500);
  if (r.winner === "b") a.then(e => cachePut(c.href, e)).catch(() => {});
  return r;
}
async function igInfo(c) {
  const a = ytdlpInfo(c).then(info => ({kind: "ytdlp", info}));
  const b = igEmbed(c.href).then(info => ({kind: "direct", info}));
  const r = await preferFirst(a, b, 2500);
  if (r.winner === "b") a.then(e => cachePut(c.href, e)).catch(() => {});
  return r;
}

async function infoFor(c) {
  const hit = cacheGet(c.href);
  if (hit) return hit;
  const r = c.k === "youtube" ? await ytInfo(c) : c.k === "instagram" ? await igInfo(c) : c.k === "facebook" ? await fbInfo(c) : {kind: "ytdlp", info: await ytdlpInfo(c)};
  const entry = {kind: r.kind, info: r.info, data: r.data};
  await cachePut(c.href, entry);
  return entry;
}

const fmtDur = d => d ? `${Math.floor(d / 60)}:${String(d % 60).padStart(2, "0")}` : "";
// Instagram/Facebook CDNs refuse hot-linked thumbnails: serve them through /api/thumb
const THUMB_HOSTS = /(^|\.)(cdninstagram\.com|fbcdn\.net|ytimg\.com|twimg\.com|tiktokcdn\.com|tiktokcdn-us\.com|ibyteimg\.com|ggpht\.com)$/;
const thumbUrl = t => { try { const u = new URL(t); return THUMB_HOSTS.test(u.hostname) ? "/api/thumb?u=" + encodeURIComponent(u.href) : ""; } catch { return ""; } };

const fsize = f => f.filesize || f.filesize_approx || 0;
const shortSide = f => Math.min(f.width || f.height, f.height || f.width);
function estSize(e, q) {
  const dur = e.duration || 0;
  if (q === "mp3") return dur ? Math.round(dur * 192000 / 8) : 0;
  const fs = e.formats || [];
  const h264 = f => /^(avc|h264)/.test(f.vcodec || "") ? 1 : 0;
  const v = fs.filter(f => f.vcodec && f.vcodec !== "none" && (f.height || f.width) && (q === "best" || shortSide(f) <= +q * 1.04))
    .sort((x, y) => shortSide(y) - shortSide(x) || h264(y) - h264(x))[0];
  if (!v || !fsize(v)) return 0;
  if (v.acodec && v.acodec !== "none") return fsize(v);
  const a = fs.filter(f => f.acodec && f.acodec !== "none" && (!f.vcodec || f.vcodec === "none"))
    .sort((x, y) => (/m4a|mp4/.test(y.ext) ? 1 : 0) - (/m4a|mp4/.test(x.ext) ? 1 : 0) || (y.abr || 0) - (x.abr || 0))[0];
  return fsize(v) + (a ? fsize(a) : 0);
}

// Quality ladder offered to the user: 2K / 4K appear only when the source really has them (YouTube),
// everything else keeps the usual three or four steps.
const LADDER = [2160, 1440, 1080, 720, 480, 360];
const rungs = (max, multi) => LADDER.filter(h => h <= max * 1.04).slice(0, multi ? 1 : (max > 1100 ? 5 : 3));

function buildFormats(c, entry) {
  const enc = encodeURIComponent(c.href), formats = [];
  const add = (q, i, label, note, kind, height, size) => formats.push({
    label, note, q: String(q), i: i || 0, kind, height: height || 0, size: Math.round(size || 0),
    url: `/api/download?url=${enc}&q=${q}${i ? `&i=${i}` : ""}`
  });
  if (entry.kind === "ytk") {
    const d = entry.data, q = [...new Set(d.qualities.map(x => x.height))].sort((a, b) => b - a).slice(0, 6);
    q.forEach((h, n) => add(h, 0, `MP4 ${h}p`, n === 0 && h >= 720 ? "HD" : "", "video", h, 0));
    add("mp3", 0, "MP3", "მხოლოდ ხმა", "audio", 0, d.duration ? d.duration * 24000 : 0);
    return {title: d.title, thumbnail: d.thumbnail, seconds: Math.round(d.duration || 0), uploader: d.channel, formats};
  }
  if (entry.kind === "vynex") {
    const d = entry.data, hs = (d.formats || []).map(f => +f.height || 0).filter(Boolean);
    const max = Math.max(0, ...hs);
    let q = rungs(max, false);
    if (!q.length) q = [max || "best"];
    const au = (d.audio || []).slice().sort((a, b) => (parseInt(b.bitrate) || 0) - (parseInt(a.bitrate) || 0))[0];
    const vsz = h => {
      const v = (d.formats || []).filter(f => +f.height && +f.height <= (h === "best" ? 1e5 : h * 1.04)).sort((a, b) => b.height - a.height)[0];
      return v && +v.size ? +v.size + (v.has_audio ? 0 : +(au && au.size) || 0) : 0;
    };
    q.forEach((h, n) => add(h, 0, h === "best" ? "MP4" : `MP4 ${h}p`, n === 0 && h >= 720 ? "HD" : "", "video", +h || 0, vsz(h)));
    if ((d.audio || []).length || (d.formats || []).some(f => f.has_audio)) add("mp3", 0, "MP3", "მხოლოდ ხმა", "audio", 0, d.duration ? d.duration * 24000 : 0);
    const s = Math.round(+d.duration || 0);
    return {title: d.title || "YouTube", thumbnail: d.thumbnail || "", seconds: s, uploader: d.channel || "", formats};
  }
  const info = entry.info, vids = videosOf(info);
  if (!vids.length) { const e = new Error("no video"); e.code = "novideo"; throw e; }
  const first = vids[0].e, multi = vids.length > 1;
  for (const {e, i} of vids) {
    const max = Math.max(0, ...heightsOf(e));
    let q = rungs(max, multi);
    if (!q.length) q = [max || "best"];
    const at = info.entries ? i : 0;
    q.forEach((h, n) => add(h, at, h === "best" ? "MP4" : `MP4 ${h}p`, multi ? `ვიდეო ${formats.length + 1}` : (n === 0 && h >= 720 ? "HD" : ""), "video", +h || 0, estSize(e, h)));
  }
  if (!multi) add("mp3", info.entries ? vids[0].i : 0, "MP3", "მხოლოდ ხმა", "audio", 0, estSize(vids[0].e, "mp3"));
  const s = Math.round(first.duration || info.duration || 0);
  return {title: first.title || info.title || NAMES[c.k], thumbnail: first.thumbnail || info.thumbnail || "", seconds: s,
    uploader: first.uploader || first.channel || info.uploader || "", formats};
}

async function readBody(req) {
  let body = "";
  for await (const ch of req) { body += ch; if (body.length > 4096) throw Object.assign(new Error("too large"), {status: 413}); }
  return JSON.parse(body || "{}");
}

async function extract(req, res) {
  let url; try { url = (await readBody(req)).url; } catch (e) { return json(res, e.status || 400, {error: "bad request"}); }
  const c = check(url);
  if (!c) return json(res, 400, {error: "unsupported link", code: "unsupported"});
  try {
    c.href = await resolve(c.href, c.k);
    const entry = await infoFor(c);
    if (entry.kind === "ytdlp" && entry.info.is_live) return json(res, 422, {error: "live", code: "live"});
    const out = buildFormats(c, entry);
    if (c.k === "youtube" && out.seconds > MAX_YT_SEC) return json(res, 422, {error: "too long", code: "toolong"});
    const th = out.thumbnail && (c.k === "instagram" || c.k === "facebook" ? thumbUrl(out.thumbnail) || out.thumbnail : out.thumbnail);
    json(res, 200, {
      platform: NAMES[c.k], key: c.k, url: c.href, title: out.title, uploader: out.uploader,
      thumbnail: th || "", duration: fmtDur(out.seconds), seconds: out.seconds, formats: out.formats,
      // what the source really offered (helps to tell "video has no 4K" from "YouTube hid it from this server")
      source: entry.kind,
      maxHeight: entry.kind === "ytk" ? Math.max(0, ...entry.data.qualities.map(x => x.height)) : entry.kind === "vynex" ? Math.max(0, ...(entry.data.formats || []).map(f => +f.height || 0))
        : Math.max(0, ...videosOf(entry.info).flatMap(v => heightsOf(v.e)))
    });
  } catch (err) {
    cache.delete(c.href);
    json(res, 422, {error: "video not found or not public", code: err.code || why(err.stderr), detail: errLine(err.stderr)});
  }
}

// ---------------------------------------------------------------- download jobs
// Every download becomes a job: fetch + merge video and audio into ONE mp4 (or convert to mp3) in a temp folder,
// then send it with a real Content-Length (the Android app shows exact progress and can resume with Range).
const jobs = new Map(), queue = [];
let running = 0;
const safeName = s => String(s || "video").replace(/[\\/:*?"<>|\x00-\x1f]+/g, " ").replace(/\s+/g, " ").trim().slice(0, 80) || "video";

function newJob(c, q, i) {
  const id = crypto.randomUUID();
  const j = {id, c, q, i, state: "queued", phase: "queued", progress: 0, created: Date.now(), dir: path.join(TMP, "job-" + id),
    file: null, size: 0, name: "", mime: "", error: null, kill: null, readers: 0, expires: Date.now() + 30 * 60e3};
  j.done = new Promise(r => { j._done = r; });
  jobs.set(id, j); queue.push(j); pump();
  return j;
}
function pump() {
  while (running < MAX_JOBS && queue.length) {
    const j = queue.shift();
    if (j.state !== "queued") continue;
    running++;
    runJob(j).finally(() => { running--; pump(); });
  }
}
function cancelJob(j) {
  if (j.state === "queued" || j.state === "working") { j.state = "error"; j.error = "cancelled"; j.kill && j.kill(); j._done(); }
}
async function dropJob(j) { jobs.delete(j.id); await rm(j.dir, {recursive: true, force: true}).catch(() => {}); }
setInterval(() => {
  const n = Date.now();
  for (const j of jobs.values()) if (n > j.expires && j.readers === 0) { cancelJob(j); dropJob(j); }
}, 60e3).unref();

// spawn yt-dlp/ffmpeg, report progress lines, kill on timeout or cancel
function spawnTracked(j, cmd, args, ms, onLine) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, {stdio: ["ignore", "pipe", "pipe"]});
    let err = "", buf = "";
    const t = setTimeout(() => p.kill("SIGKILL"), ms);
    j.kill = () => p.kill("SIGKILL");
    p.stdout.on("data", d => {
      buf += d; let k;
      while ((k = buf.indexOf("\n")) >= 0) { const line = buf.slice(0, k).trim(); buf = buf.slice(k + 1); if (line) onLine(line); }
    });
    p.stderr.on("data", d => { err += d; if (err.length > 1e6) err = err.slice(-200000); });
    p.on("close", code => {
      clearTimeout(t); j.kill = null;
      if (code === 0) resolve();
      else { const e = new Error(cmd + " failed"); e.stderr = err; console.error(cmd, "ERROR:", err.slice(-600)); reject(e); }
    });
    p.on("error", e => { clearTimeout(t); e.stderr = e.message; reject(e); });
  });
}

async function ytdlpJob(j, entry) {
  const {c, q, i} = j, mp3 = q === "mp3", pick = i ? ["--playlist-items", String(i)] : [];
  const sort = (q === "best" || mp3 ? "res" : `res:${q}`) + ",vcodec:h264,acodec:m4a";
  // above 1080p YouTube only has VP9/AV1: take VP9 (plays on far more devices than AV1) and merge it into MP4
  const hi = !mp3 && q !== "best" && +q > 1080;
  const fmt = mp3 ? ["-f", "ba/b", "-x", "--audio-format", "mp3", "--audio-quality", "192K"]
    : ["-f", hi ? "bv*[vcodec^=vp09]+ba/bv*+ba/b" : "bv*+ba/b", "-S", sort, "--merge-output-format", "mp4", "--remux-video", "mp4"];
  let stream = 0, last = 0, baseBytes = 0, lastTotal = 0;
  const onLine = line => {
    const m = line.match(/^MOONP\s+([\d.]+)%\s*(\S*)\s*(\S*)/);
    if (m) {
      const p = +m[1], got = +m[2] || 0, tot = Math.round(+m[3] || 0);
      if (p + 5 < last) { stream++; baseBytes += lastTotal; }   // second stream (audio) started
      last = p; if (tot) lastTotal = tot;
      j.bytes = baseBytes + got; j.total = Math.max(j.total || 0, baseBytes + (tot || got));
      j.phase = "downloading";
      j.progress = Math.min(97, Math.round(mp3 ? p * 0.9 : stream === 0 ? p * 0.85 : 85 + p * 0.12));
    } else if (/^\[(Merger|VideoRemuxer|ExtractAudio|FixupM)/.test(line)) { j.phase = mp3 ? "converting" : "merging"; j.progress = Math.max(j.progress, 97); }
  };
  const common = ["--newline", "--progress-template", "download:MOONP %(progress._percent_str)s %(progress.downloaded_bytes)s %(progress.total_bytes,progress.total_bytes_estimate)s", "--no-warnings", "--no-playlist", ...pick,
    ...fmt, "-N", "8", "--http-chunk-size", "10M", "--max-filesize", "1500M", "--no-part", "-o", path.join(j.dir, "media.%(ext)s")];
  const attempts = [];
  if (entry && entry.file) attempts.push([...common, ...base(j.c.k), "--load-info-json", entry.file]);
  attempts.push([...common, ...base(c.k), c.href]);
  if (hasCookies && (c.k === "instagram" || c.k === "facebook")) attempts.push([...common, ...base(c.k, {noCookies: true}), c.href]);
  let lastErr;
  for (const args of attempts) {
    if (j.state !== "working") return;
    await rm(j.dir, {recursive: true, force: true}).catch(() => {}); await mkdir(j.dir, {recursive: true});
    stream = 0; last = 0; baseBytes = 0; lastTotal = 0; j.bytes = 0; j.total = 0;
    try { await spawnTracked(j, YTDLP, args, 8 * 60e3, onLine); lastErr = null; break; }
    catch (e) {
      lastErr = e;
      if (c.k === "youtube" && botBlock(e.stderr)) ytBlockedUntil = Date.now() + 30 * 60e3;
      if (why(e.stderr) === "novideo") break;
    }
  }
  if (lastErr) throw lastErr;
}

// Instagram embed result: one progressive MP4 (picture + sound) → copy it, or extract MP3
async function directJob(j, info) {
  const fs = (info.formats || []).filter(f => f && f.url).sort((a, b) => (b.height || 0) - (a.height || 0));
  if (!fs.length) throw Object.assign(new Error("no url"), {code: "notfound"});
  const want = j.q === "best" || j.q === "mp3" ? 1e5 : +j.q;
  const first = fs.find(f => (f.height || 0) <= want * 1.04) || fs[fs.length - 1];
  const vurls = [first, ...fs.filter(f => f !== first)].map(f => f.url);
  return ffmpegJob(j, {vurls, aurls: [], hasAudio: true, dur: +info.duration || 0, headers: `Referer: ${info.referer || "https://www.instagram.com/"}\r\n`});
}

async function vynexJob(j, d) {
  const mp3 = j.q === "mp3", want = j.q === "best" ? 1e5 : +j.q;
  const vf = (d.formats || []).filter(f => +f.height).sort((a, b) => b.height - a.height);
  const v = vf.find(f => +f.height <= want * 1.04) || vf[vf.length - 1];
  const isAac = a => /m4a|mp4|aac/i.test(`${a.ext || ""} ${a.mime || a.mime_type || ""} ${a.codec || a.acodec || ""}`) || String(a.format_id) === "140";
  const au = (d.audio || []).slice().sort((a, b) => (isAac(b) ? 1 : 0) - (isAac(a) ? 1 : 0) || (parseInt(b.bitrate) || 0) - (parseInt(a.bitrate) || 0))[0];
  const vurls = v ? [`${VYNEX}/download?id=${encodeURIComponent(d.id)}&height=${v.height}`, v.url].filter(Boolean) : [];
  const aurls = au ? [`${VYNEX}/audio?id=${encodeURIComponent(d.id)}&format_id=${encodeURIComponent(au.format_id)}`, au.url].filter(Boolean) : [];
  j.total = v && +v.size ? +v.size + (v.has_audio ? 0 : +(au && au.size) || 0) : 0;
  return ffmpegJob(j, {vurls, aurls, hasAudio: !!(v && (v.has_audio || !aurls.length)), dur: +d.duration || 0, noVideo: !v, aacAudio: !!(au && isAac(au))});
}

async function ffmpegJob(j, {vurls, aurls, hasAudio, dur, headers, noVideo, aacAudio}) {
  const mp3 = j.q === "mp3";
  const onLine = line => {
    const m = line.match(/^out_time_(?:us|ms)=(\d+)/);
    if (m && dur) { j.phase = mp3 ? "converting" : "merging"; j.progress = Math.min(98, Math.round(+m[1] / 1e6 / dur * 100)); }
    const sz = line.match(/^total_size=(\d+)/);
    if (sz) { j.bytes = +sz[1]; if (j.total && j.bytes > j.total) j.total = j.bytes; }
  };
  const inp = u => ["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5", "-user_agent", UA, ...(headers ? ["-headers", headers] : []), "-i", u];
  const head = ["-hide_banner", "-loglevel", "error", "-nostats", "-progress", "pipe:1", "-y"];
  await mkdir(j.dir, {recursive: true});
  const tries = [];
  if (mp3) {
    const src = aurls.length ? aurls : vurls;
    for (const a of src) tries.push([...head, ...inp(a), "-vn", "-c:a", "libmp3lame", "-b:a", "192k", path.join(j.dir, "media.mp3")]);
  } else if (!noVideo && hasAudio) {
    for (const x of vurls) tries.push([...head, ...inp(x), "-c", "copy", "-movflags", "+faststart", path.join(j.dir, "media.mp4")]);
  } else {
    // stream copy first (instant merge); re-encode the audio only if its codec can't go into MP4 as-is
    const codecs = aacAudio ? [["-c:a", "copy"], ["-c:a", "aac", "-b:a", "192k"]] : [["-c:a", "aac", "-b:a", "192k"]];
    for (const acodec of codecs)
      for (const x of vurls) for (const a of aurls)
        tries.push([...head, ...inp(x), ...inp(a), "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", ...acodec,
          "-shortest", "-movflags", "+faststart", path.join(j.dir, "media.mp4")]);
  }
  if (!tries.length) throw Object.assign(new Error("no formats"), {code: "notfound"});
  let lastErr;
  for (const args of tries) {
    if (j.state !== "working") return;
    j.phase = "downloading";
    try { await spawnTracked(j, "ffmpeg", args, 10 * 60e3, onLine); return; } catch (e) { lastErr = e; lastErr.stderr = "notfound"; }
  }
  throw lastErr;
}

async function runJob(j) {
  if (j.state !== "queued") return j._done();
  j.state = "working"; j.phase = "preparing";
  try {
    await mkdir(j.dir, {recursive: true});
    let entry = cacheGet(j.c.href);
    if (j.c.k === "youtube" && !entry) entry = await infoFor(j.c).catch(() => null);
    const title = entry ? (entry.kind === "vynex" || entry.kind === "ytk" ? entry.data.title : (entry.info.entries?.[(j.i || 1) - 1]?.title || entry.info.title)) : "";
    if (entry && entry.kind === "ytk") {
      const others = async () => {   // MP3, or the owner's service failed: the usual sources take over
        try { const d = await vynexInfo(j.c.href); await cachePut(j.c.href, {kind: "vynex", data: d}); await vynexJob(j, d); }
        catch (e) { if (j.state !== "working") throw e; await ytdlpJob(j, null); }
      };
      if (j.q === "mp3") await others();
      else { try { await ytkJob(j, entry.data); } catch (e) { if (j.state !== "working") throw e; console.error("ytk job:", errLine(e.stderr) || e.message); await rm(j.dir, {recursive: true, force: true}).catch(() => {}); await mkdir(j.dir, {recursive: true}); await others(); } }
    }
    else if (entry && entry.kind === "vynex") await vynexJob(j, entry.data);
    else if (entry && entry.kind === "direct") {
      try { await directJob(j, entry.info); }
      catch (e) { if (j.state !== "working") throw e; await ytdlpJob(j, null); }   // CDN link expired → let yt-dlp try
    }
    else {
      try { await ytdlpJob(j, entry); }
      catch (e) {
        // YouTube blocked this server mid-download: fetch the same video via vynex and merge it here
        if (j.c.k !== "youtube" || VYNEX === "off" || YT_MODE === "ytdlp" || j.state !== "working" || why(e.stderr) === "novideo") throw e;
        const d = await vynexInfo(j.c.href);
        await cachePut(j.c.href, {kind: "vynex", data: d});
        await vynexJob(j, d);
      }
    }
    if (j.state !== "working") return;
    const f = (await readdir(j.dir)).find(n => n.startsWith("media.") && !/\.(part|ytdl|temp|json)$/.test(n));
    if (!f) throw new Error("no output file");
    j.file = path.join(j.dir, f);
    j.size = (await stat(j.file)).size; j.bytes = j.size; j.total = j.size;
    const ext = path.extname(f).slice(1).toLowerCase();
    j.mime = ext === "mp3" ? "audio/mpeg" : ext === "m4a" ? "audio/mp4" : ext === "webm" ? "video/webm" : "video/mp4";
    j.name = `${safeName(title || NAMES[j.c.k] + " video")}${j.q === "mp3" ? "" : j.q === "best" ? "" : ` ${j.q}p`}.${ext}`;
    j.state = "ready"; j.phase = "ready"; j.progress = 100;
    j.expires = Date.now() + 90 * 60e3;
  } catch (e) {
    if (j.state === "working") { j.state = "error"; j.error = e.code || why(e.stderr); }
    rm(j.dir, {recursive: true, force: true}).catch(() => {});
  } finally { j._done(); }
}

function parseDl(get) {
  const c = check(get("url") || "");
  const q = String(get("q") || ""), i = String(get("i") || "");
  if (!c || !(q === "mp3" || q === "best" || /^\d{3,4}$/.test(q)) || (i && i !== "0" && !/^([1-9]|10)$/.test(i))) return null;
  return {c, q, i: i && i !== "0" ? +i : 0};
}
const disposition = name => {
  const ascii = name.replace(/[^\x20-\x7e]/g, "_").replace(/"/g, "'");
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(name)}`;
};
const jobView = j => ({id: j.id, state: j.state, phase: j.phase, progress: j.progress, size: j.size, name: j.name, mime: j.mime, error: j.error,
  bytes: Math.round(j.bytes || 0), total: Math.round(j.total || 0),
  position: j.state === "queued" ? queue.indexOf(j) + 1 : 0});

async function sendFile(req, res, j) {
  let size;
  try { size = (await stat(j.file)).size; } catch { return json(res, 410, {error: "gone", code: "expired"}); }
  const h = {"Content-Type": j.mime, "Content-Disposition": disposition(j.name), "Accept-Ranges": "bytes", "Cache-Control": "no-store"};
  let start = 0, end = size - 1, code = 200;
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || "");
  if (m && (m[1] || m[2])) {
    if (m[1]) { start = +m[1]; if (m[2]) end = Math.min(+m[2], size - 1); }
    else start = Math.max(0, size - +m[2]);
    if (start > end || start >= size) { res.writeHead(416, {"Content-Range": `bytes */${size}`}); return res.end(); }
    code = 206; h["Content-Range"] = `bytes ${start}-${end}/${size}`;
  }
  h["Content-Length"] = end - start + 1;
  res.writeHead(code, h);
  if (req.method === "HEAD") return res.end();
  j.readers++; j.expires = Math.max(j.expires, Date.now() + 15 * 60e3);
  const s = createReadStream(j.file, {start, end});
  const fin = () => { j.readers = Math.max(0, j.readers - 1); };
  s.on("close", fin); res.on("close", () => { s.destroy(); j.expires = Math.max(j.expires, Date.now() + 90 * 60e3); });
  s.pipe(res);
}

// API for the app:  POST /api/jobs {url,q,i} -> {id};  GET /api/jobs/:id -> status;  GET /api/jobs/:id/file;  DELETE /api/jobs/:id
async function createJob(req, res) {
  let b; try { b = await readBody(req); } catch (e) { return json(res, e.status || 400, {error: "bad request"}); }
  const p = parseDl(k => b[k] == null ? "" : b[k]);
  if (!p) return json(res, 400, {error: "bad request"});
  if (queue.length >= MAX_QUEUE) return json(res, 503, {error: "busy", code: "busy"});
  p.c.href = await resolve(p.c.href, p.c.k);
  const j = newJob(p.c, p.q, p.i);
  json(res, 202, jobView(j));
}

// Website: one link per quality. Same job pipeline; headers go out right away so the browser shows the download
// and the connection stays alive while the server merges video + audio.
async function download(req, res, params) {
  const p = parseDl(k => params.get(k));
  if (!p) return json(res, 400, {error: "bad request"});
  if (queue.length >= MAX_QUEUE) return json(res, 503, {error: "busy", code: "busy"});
  p.c.href = await resolve(p.c.href, p.c.k);
  const j = newJob(p.c, p.q, p.i), mp3 = p.q === "mp3";
  const entry = cacheGet(p.c.href);
  const t = entry ? (entry.kind === "vynex" || entry.kind === "ytk" ? entry.data.title : entry.info.title) : "";
  res.writeHead(200, {"Content-Type": mp3 ? "audio/mpeg" : "video/mp4", "Cache-Control": "no-store",
    "Content-Disposition": disposition(`${safeName(t || "moon-" + p.c.k)}${mp3 ? ".mp3" : ".mp4"}`)});
  let gone = false; res.on("close", () => { gone = true; if (j.state !== "ready") cancelJob(j); });
  await j.done;
  if (gone || j.state !== "ready") { res.destroy(); return; }
  j.readers++;
  const s = createReadStream(j.file);
  s.on("close", () => { j.readers--; j.expires = Date.now() + 60e3; });
  res.on("close", () => s.destroy());
  s.pipe(res);
}

async function thumb(res, params) {
  let u; try { u = new URL(params.get("u") || ""); } catch { return json(res, 400, {error: "bad request"}); }
  if (u.protocol !== "https:" || !THUMB_HOSTS.test(u.hostname)) return json(res, 400, {error: "bad request"});
  try {
    const r = await fetch(u.href, {headers: {"User-Agent": UA, Referer: "https://www.instagram.com/"}, signal: AbortSignal.timeout(10000)});
    const ct = r.headers.get("content-type") || "";
    if (!r.ok || !/^image\//.test(ct)) return json(res, 404, {error: "not found"});
    const buf = Buffer.from(await r.arrayBuffer());
    if (buf.length > 5e6) return json(res, 413, {error: "too large"});
    res.writeHead(200, {"Content-Type": ct, "Content-Length": buf.length, "Cache-Control": "public, max-age=3600"});
    res.end(buf);
  } catch { json(res, 404, {error: "not found"}); }
}

// Diagnostics: shows the REAL reason a link fails. Enabled only when DIAG_KEY env var is set.
// Open: /api/diag?key=YOUR_KEY&url=<video link>
const DIAG_KEY = process.env.DIAG_KEY || "";
async function diag(res, params) {
  if (!DIAG_KEY || params.get("key") !== DIAG_KEY) return json(res, 403, {error: "forbidden"});
  const full = params.has("full");   // the slow yt-dlp test only on request: /api/diag?key=…&url=…&full=1
  const [ytdlp, node, ffmpeg] = await Promise.all([sh(YTDLP, ["--version"], 30000), sh("node", ["--version"]), sh("ffmpeg", ["-version"])]);
  const out = {
    ytdlp: ytdlp.trim().slice(0, 120), ytdlpPath: YTDLP, node: node.trim(), ffmpeg: ffmpeg.split("\n")[0],
    ytk: YTK_ON ? "on" : "off (set YTK_BASE and YTK_KEY)", cookiesLoaded: hasCookies, proxySet: !!PROXY, jsRuntime: JS_RT, ytMode: YT_MODE, ytBlocked: Date.now() < ytBlockedUntil,
    jobs: {running, queued: queue.length, total: jobs.size}
  };
  if (full) out.impersonate = (await sh(YTDLP, ["--list-impersonate-targets"], 20000)).split("\n").filter(l => /chrome|safari/i.test(l)).slice(0, 4);
  const c = check(params.get("url") || "");
  if (c) {
    out.input = c.href;
    c.href = await Promise.race([resolve(c.href, c.k), new Promise(r => setTimeout(() => r(c.href), 15000))]);
    out.resolved = c.href;
    if (c.k === "facebook") {   // what Facebook really answers this server (status, login wall?, video found?)
      const probe = ([name, u, ua]) => fetch(u, {headers: {"User-Agent": ua, Accept: "text/html", "Accept-Language": "en-US,en;q=0.9"}, redirect: "follow", signal: AbortSignal.timeout(9000)})
        .then(async r => {
          const html = (await r.text()).slice(0, 2e6);
          return {name, status: r.status, finalUrl: r.url.slice(0, 160), bytes: html.length, title: (html.match(/<title[^>]*>([^<]{0,80})/i) || [])[1] || "",
            loginWall: /log in to facebook|login_form|\/login\/\?/i.test(r.url + html.slice(0, 4000)),
            markers: Object.fromEntries(["hd_src", "sd_src", "playable_url", "browser_native", "videoData", ".mp4", "progressive_url", "dash_manifest", "VideoConfig"].map(k => [k, html.split(k).length - 1])),
            text: html.replace(/<(script|style)[\s\S]*?<\/\1>/gi, " ").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, 220),
            hint: [...html.matchAll(/.{0,70}(isn.?t available|not available|unavailable|can.?t be embedded|embedding is disabled|in your (?:country|region)|restricted|permission|content isn.?t|log in to continue).{0,70}/gi)].slice(0, 3).map(m => m[0].replace(/\s+/g, " ")), ogVideo: !!fbMeta(html, "og:video") || !!fbMeta(html, "og:video:secure_url"), parsed: !!fbParse(html)};
        }, e => ({name, error: String(e.message || e).slice(0, 120)}));
      out.fbIds = fbIds(c.href); out.fbPage = fbPageId(c.href);
      out.fbTest = await Promise.all(fbCandidates(c.href).slice(0, 8).map(([u, ua], n) => probe([`#${n} ${u.replace("https://www.facebook.com", "").slice(0, 70)}`, u, ua])));
    }
    if (YTK_ON && c.k === "youtube") out.ytkTest = await ytkInfo(c.href).then(d => ({ok: true, heights: d.qualities.map(x => x.height), labels: d.qualities.map(x => x.label)}), e => ({ok: false, error: errLine(e.stderr) || e.message}));
    if (full || c.k !== "facebook") {
      let t = await sh(YTDLP, ["-v", "-s", "--no-playlist", ...base(c.k), c.href], 60000);
      if (PROXY) t = t.split(PROXY).join("***");
      out.test = t.slice(-3500);
    } else out.test = "skipped (add &full=1 for the yt-dlp test)";
  }
  json(res, 200, out);
}

const SELF = process.env.KEEP_AWAKE === "0" ? "" : (process.env.KEEP_AWAKE_URL || process.env.RENDER_EXTERNAL_URL || "");
if (SELF) setInterval(() => fetch(SELF.replace(/\/$/, "") + "/api/health", {signal: AbortSignal.timeout(15000)}).catch(() => {}), 10 * 60e3).unref();

const CORS = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS", "Access-Control-Allow-Headers": "Content-Type,Range",
  "Access-Control-Expose-Headers": "Content-Length,Content-Disposition,Content-Range"};

http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://x");
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();
  const pth = u.pathname;
  try {
    if (pth.startsWith("/api/")) {
      for (const [k, v] of Object.entries(CORS)) res.setHeader(k, v);
      if (req.method === "OPTIONS") { res.writeHead(204); return res.end(); }
    }
    if (pth === "/api/diag") return diag(res, u.searchParams);
    if (pth === "/api/health") return json(res, 200, {ok: true});
    if (pth === "/api/version") return json(res, 200, {name: "MOON", api: 2, build: "4k-7", ytk: YTK_ON, features: ["jobs", "merge", "mp3", "thumb"], ytdlp: ytVersion,
      platforms: Object.keys(HOSTS)});
    if (pth === "/api/thumb" && req.method === "GET") return thumb(res, u.searchParams);
    if (pth === "/api/extract" && req.method === "POST") return limited(ip) ? json(res, 429, {error: "too many requests", code: "ratelimit"}) : extract(req, res);
    if (pth === "/api/download" && req.method === "GET") return limited(ip) ? json(res, 429, {error: "too many requests", code: "ratelimit"}) : download(req, res, u.searchParams);
    if (pth === "/api/jobs" && req.method === "POST") return limited(ip) ? json(res, 429, {error: "too many requests", code: "ratelimit"}) : createJob(req, res);
    const jm = pth.match(/^\/api\/jobs\/([0-9a-f-]{36})(\/file)?$/);
    if (jm) {
      const j = jobs.get(jm[1]);
      if (!j) return json(res, 404, {error: "not found", code: "expired"});
      if (req.method === "DELETE") { cancelJob(j); await dropJob(j); return json(res, 200, {ok: true}); }
      if (!jm[2]) { if (j.state === "ready") j.expires = Math.max(j.expires, Date.now() + 90 * 60e3); return json(res, 200, jobView(j)); }
      if (j.state !== "ready") return json(res, 409, {error: "not ready", ...jobView(j)});
      return sendFile(req, res, j);
    }
    if (pth === "/" || pth === "/index.html") {
      res.writeHead(200, {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache"});
      return res.end(await readFile(path.join(dir, "public/index.html")));
    }
    json(res, 404, {error: "not found"});
  } catch (e) { console.error("server error:", e); json(res, 500, {error: "server error"}); }
}).listen(PORT, () => console.log(`MOON running on :${PORT}`));
