const net = require("net");
const http = require("http");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const PORT_VIDEO = 3001;
const PORT_AUDIO = 3002;
const HTTP_PORT = process.env.PORT || 3000;

const STREAMS_ROOT = path.join(process.cwd(), "streams");

if (!fs.existsSync(STREAMS_ROOT)) {
  fs.mkdirSync(STREAMS_ROOT, { recursive: true });
}

const HTML_PAGE = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Camera Stream</title>
  <style>
    html, body {
      margin: 0;
      width: 100%;
      height: 100%;
      background: #000;
      overflow: hidden;
    }
    body {
      display: flex;
      align-items: center;
      justify-content: center;
    }
    video {
      width: 100vw;
      height: 100vh;
      background: #000;
      object-fit: contain;
    }
  </style>
</head>
<body>
  <video id="video" controls autoplay playsinline muted></video>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <script>
    (function () {
      var video = document.getElementById('video');
      var params = new URLSearchParams(window.location.search);
      var user = params.get('u') || 'nicolas';
      var sid = params.get('s') || 'live1';
      var src = window.location.origin + '/streams/' + user + '/' + sid + '/stream.m3u8';

      function start() {
        if (window.Hls && Hls.isSupported()) {
          var hls = new Hls({
            liveSyncDurationCount: 1,
            liveMaxLatencyDurationCount: 3,
            maxBufferLength: 4,
            backBufferLength: 0,
            enableWorker: true,
            lowLatencyMode: false
          });

          hls.loadSource(src);
          hls.attachMedia(video);

          hls.on(Hls.Events.MANIFEST_PARSED, function () {
            video.play().catch(function () {});
          });

          hls.on(Hls.Events.ERROR, function (event, data) {
            console.log('HLS error:', data);
            if (data.fatal) {
              setTimeout(function () {
                hls.loadSource(src);
              }, 2000);
            }
          });
        } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
          video.src = src;
          video.addEventListener('loadedmetadata', function () {
            video.play().catch(function () {});
          });
        } else {
          document.body.innerHTML = '<div style="color:#fff;font-family:sans-serif;padding:20px">HLS nao suportado neste navegador.</div>';
        }
      }

      start();

      setInterval(function () {
        if (video.paused) {
          video.play().catch(function () {});
        }
      }, 1500);
    })();
  </script>
</body>
</html>`;

function sanitize(value, maxLen) {
  if (maxLen === undefined) maxLen = 24;
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^-+|-+$/g, "")
    .slice(0, maxLen) || "default";
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function rmDir(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch (e) {}
}

function createLiveKey(username, streamId) {
  return username + "::" + streamId;
}

function getLiveDir(username, streamId) {
  return path.join(STREAMS_ROOT, sanitize(username, 24), sanitize(streamId, 24));
}

function parseAuthLine(line) {
  const parts = String(line).trim().split("|");
  if (parts.length !== 4) return null;
  if (parts[0] !== "AUTH") return null;

  return {
    type: parts[1],
    username: sanitize(parts[2], 24),
    streamId: sanitize(parts[3], 24)
  };
}

function startFfmpeg(live) {
  if (live.ffmpeg) return;

  ensureDir(live.dir);

  const playlist = path.join(live.dir, "stream.m3u8");
  const segmentPattern = path.join(live.dir, "stream%d.ts");

  const args = [
    "-hide_banner",
    "-y",
    "-loglevel", "info",
    "-fflags", "+genpts+discardcorrupt+nobuffer",
    "-flags", "low_delay",
    "-use_wallclock_as_timestamps", "1",
    "-probesize", "5000000",
    "-analyzeduration", "5000000",
    "-f", "h264",
    "-i", "pipe:0",
    "-an",
    "-c:v", "copy",
    "-f", "hls",
    "-hls_time", "1",
    "-hls_list_size", "6",
    "-hls_delete_threshold", "2",
    "-hls_flags", "delete_segments+append_list+omit_endlist+independent_segments+temp_file",
    "-hls_segment_filename", segmentPattern,
    playlist
  ];

  console.log("[FFmpeg] Iniciando para " + live.key);
  console.log("[FFmpeg] Dir: " + live.dir);
  console.log("[FFmpeg] Playlist: " + playlist);

  live.ffmpeg = spawn("ffmpeg", args, { stdio: ["pipe", "pipe", "pipe"] });
  live.videoPipe = live.ffmpeg.stdin;

  live.ffmpeg.stderr.on("data", function (data) {
    const txt = String(data).trim();
    if (txt) {
      console.log("[FFmpeg " + live.key + "] " + txt);
    }
  });

  live.ffmpeg.stdout.on("data", function (data) {
    const txt = String(data).trim();
    if (txt) {
      console.log("[FFmpeg OUT " + live.key + "] " + txt);
    }
  });

  live.ffmpeg.on("close", function (code, signal) {
    console.log("[FFmpeg] Finalizou " + live.key + " code=" + code + " signal=" + signal);
    try { if (live.videoPipe) live.videoPipe.destroy(); } catch (e) {}
    live.ffmpeg = null;
    live.videoPipe = null;
    live.videoSocket = null;
    live.audioSocket = null;
    rmDir(live.dir);
    lives.delete(live.key);
  });

  live.ffmpeg.on("error", function (err) {
    console.log("[FFmpeg ERRO] " + live.key + ": " + err.message);
  });
}

function stopLive(live) {
  if (!live) return;

  console.log("[STOP] Encerrando " + live.key);

  try { if (live.videoPipe) live.videoPipe.end(); } catch (e) {}
  try { if (live.ffmpeg) live.ffmpeg.kill("SIGKILL"); } catch (e) {}
  try { if (live.videoSocket) live.videoSocket.destroy(); } catch (e) {}
  try { if (live.audioSocket) live.audioSocket.destroy(); } catch (e) {}

  rmDir(live.dir);
  lives.delete(live.key);
}

const lives = new Map();

function bindSocketAuth(socket, kind) {
  socket.setKeepAlive(true, 1000);
  socket.setNoDelay(true);

  let buffer = Buffer.alloc(0);
  let authenticated = false;
  let live = null;

  socket.on("data", function (data) {
    if (!authenticated) {
      buffer = Buffer.concat([buffer, data]);
      const idx = buffer.indexOf(0x0A);
      if (idx === -1) return;

      const line = buffer.slice(0, idx).toString("utf8");
      buffer = buffer.slice(idx + 1);

      console.log("[AUTH] Recebido: " + line);

      const auth = parseAuthLine(line);
      if (!auth || auth.type !== kind || !auth.username || !auth.streamId) {
        console.log("[AUTH] Invalido, destruindo socket");
        socket.destroy();
        return;
      }

      const key = createLiveKey(auth.username, auth.streamId);
      const dir = getLiveDir(auth.username, auth.streamId);

      if (!lives.has(key)) {
        live = {
          key: key,
          dir: dir,
          username: auth.username,
          streamId: auth.streamId,
          ffmpeg: null,
          videoPipe: null,
          videoSocket: null,
          audioSocket: null
        };
        lives.set(key, live);
        ensureDir(dir);
        console.log("[LIVE] Novo: " + key);
        console.log("[LIVE] Dir: " + dir);
      } else {
        live = lives.get(key);
        console.log("[LIVE] Reusando: " + key);
      }

      if (kind === "VIDEO") live.videoSocket = socket;
      if (kind === "AUDIO") live.audioSocket = socket;

      if (kind === "VIDEO") {
        startFfmpeg(live);
      }

      authenticated = true;

      if (buffer.length > 0 && kind === "VIDEO" && live.videoPipe && !live.videoPipe.destroyed) {
        live.videoPipe.write(buffer);
        console.log("[VIDEO] Enviou buffer residual: " + buffer.length + " bytes");
      }

      buffer = Buffer.alloc(0);
      return;
    }

    if (!live) return;

    if (kind === "VIDEO") {
      if (live.videoPipe && !live.videoPipe.destroyed) {
        const ok = live.videoPipe.write(data);
        if (!ok) {
          live.videoPipe.once("drain", function () {});
        }
      }
    }
  });

  socket.on("close", function () {
    if (!live) return;

    console.log("[SOCKET] Fechou " + kind + " de " + live.key);

    if (kind === "VIDEO") live.videoSocket = null;
    if (kind === "AUDIO") live.audioSocket = null;

    if (!live.videoSocket && !live.audioSocket) {
      stopLive(live);
    }
  });

  socket.on("error", function (err) {
    console.log("[SOCKET ERRO] " + kind + ": " + err.message);
  });
}

function serveHlsFile(req, res) {
  const parts = req.url.split("/").filter(Boolean);
  if (parts.length < 4) {
    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("Not found");
    return;
  }

  const username = sanitize(parts[1], 24);
  const streamId = sanitize(parts[2], 24);
  const filename = path.basename(parts[3]);

  const filePath = path.join(STREAMS_ROOT, username, streamId, filename);

  console.log("[HTTP] Pedido: " + req.url + " -> " + filePath);

  if (!fs.existsSync(filePath)) {
    console.log("[HTTP] Arquivo nao existe: " + filePath);
    res.writeHead(404, {
      "Content-Type": "text/plain; charset=utf-8",
      "Access-Control-Allow-Origin": "*"
    });
    res.end("Segment not found");
    return;
  }

  const ext = path.extname(filePath).toLowerCase();

  res.writeHead(200, {
    "Content-Type": ext === ".m3u8" ? "application/vnd.apple.mpegurl" : "video/mp2t",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
    "Access-Control-Allow-Origin": "*"
  });

  fs.createReadStream(filePath).pipe(res);
}

const videoServer = net.createServer(function (socket) {
  console.log("[SERVER] VIDEO conectado");
  bindSocketAuth(socket, "VIDEO");
});

videoServer.listen(PORT_VIDEO, "0.0.0.0", function () {
  console.log("Servidor video em " + PORT_VIDEO);
});

const audioServer = net.createServer(function (socket) {
  console.log("[SERVER] AUDIO conectado");
  bindSocketAuth(socket, "AUDIO");
});

audioServer.listen(PORT_AUDIO, "0.0.0.0", function () {
  console.log("Servidor audio em " + PORT_AUDIO);
});

const server = http.createServer(function (req, res) {
  if (req.url === "/" || req.url.startsWith("/?")) {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(HTML_PAGE);
    return;
  }

  if (req.url && req.url.startsWith("/streams/")) {
    serveHlsFile(req, res);
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
});

server.listen(HTTP_PORT, "0.0.0.0", function () {
  console.log("HTTP em http://localhost:" + HTTP_PORT);
});
