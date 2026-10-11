"""
Persistent yt-dlp worker for MOON.

Starting `yt-dlp -J` for every search costs a Python start + import of all extractors (several seconds on a
small Render instance) and throws away every cache (YouTube player code, Instagram session token...).
This worker imports yt-dlp ONCE and answers extract requests over stdin/stdout, one JSON object per line:

  in : {"id": 1, "args": ["--no-playlist", ...], "url": "https://..."}
  out: {"id": 1, "ok": true, "info": {...}}   or   {"id": 1, "ok": false, "err": "ERROR: ..."}

`args` are ordinary yt-dlp command-line options (without -J and without the URL); one YoutubeDL instance is kept
per distinct option set, so sessions and caches are reused between searches.
"""
import json
import os
import sys

zipapp = sys.argv[1] if len(sys.argv) > 1 else ""
if zipapp and os.path.exists(zipapp):
    sys.path.insert(0, zipapp)  # the auto-updated nightly yt-dlp (a zip file) is importable directly

import yt_dlp  # noqa: E402

# everything yt-dlp might print goes to stderr; our protocol owns the real stdout
out = os.fdopen(os.dup(sys.stdout.fileno()), "w", buffering=1, encoding="utf-8")
sys.stdout = sys.stderr


class Log:
    def __init__(self):
        self.lines = []

    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        self.lines.append(str(msg))

    def error(self, msg):
        self.lines.append(str(msg))


LOG = Log()
instances = {}


def ydl_for(args):
    key = tuple(args)
    ydl = instances.get(key)
    if ydl is None:
        opts = yt_dlp.parse_options(list(args)).ydl_opts
        opts.update({"quiet": True, "no_warnings": False, "noprogress": True, "logger": LOG, "simulate": True, "ignoreerrors": False})
        ydl = yt_dlp.YoutubeDL(opts)
        if len(instances) > 12:
            instances.clear()
        instances[key] = ydl
    return ydl


out.write(json.dumps({"ready": True, "version": yt_dlp.version.__version__}) + "\n")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
    except Exception:
        continue
    rid = req.get("id")
    LOG.lines = []
    try:
        ydl = ydl_for(req.get("args") or [])
        info = ydl.extract_info(req["url"], download=False)
        if not info:
            raise RuntimeError("ERROR: no info")
        res = {"id": rid, "ok": True, "info": ydl.sanitize_info(info)}
    except BaseException as e:  # noqa: BLE001 - report every failure to node
        if isinstance(e, KeyboardInterrupt):
            raise
        msg = "\n".join(LOG.lines[-6:])
        text = str(e)
        if text not in msg:
            msg = (msg + "\n" + text).strip()
        res = {"id": rid, "ok": False, "err": msg or "ERROR: unknown"}
    try:
        out.write(json.dumps(res, default=str, ensure_ascii=False) + "\n")
    except Exception as e:  # noqa: BLE001
        out.write(json.dumps({"id": rid, "ok": False, "err": "ERROR: encode " + str(e)}) + "\n")
