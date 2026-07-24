const net = require("net");
const http = require("http");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const crypto = require("crypto");

const HTTP_PORT = Number(process.env.PORT || 10000);
const PORT_VIDEO_BASE = Number(process.env.PORT_VIDEO_BASE || 3001);
const PORT_AUDIO_BASE = Number(process.env.PORT_AUDIO_BASE || 3002);

const ROOT = process.env.STREAM_ROOT || "streams";
const STREAMS_DIR = path.join(ROOT, "streams");
const USERS_FILE = path.join(ROOT, "users.json");
const STREAMS_FILE = path.join(ROOT, "streams.json");

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

if (!fs.existsSync(ROOT)) fs.mkdirSync(ROOT, { recursive: true });
if (!fs.existsSync(STREAMS_DIR)) fs.mkdirSync(STREAMS_DIR, { recursive: true });

function readJson(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, "utf8");
    if (!raw.trim()) return fallback;
    return JSON.parse(raw);
  } catch (e) {
    return fallback;
  }
}

function writeJson(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

let USER_KEYS = readJson(USERS_FILE, {});
let STREAM_META = readJson(STREAMS_FILE, {});
const streams = {};

function saveUsers() {
  writeJson(USERS_FILE, USER_KEYS);
}

function saveStreams() {
  writeJson(STREAMS_FILE, STREAM_META);
}

function getUserPassword(username) {
  return username + "12345";
}

function createStream(username, apiKey) {
  const index = Object.keys(STREAM_META).length;
  const videoPort = PORT_VIDEO_BASE + index * 2;
  const audioPort = PORT_AUDIO_BASE + index * 2;

  const dir = path.join(STREAMS_DIR, username);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const playlist = path.join(dir, "stream.m3u8");
  const segmentPattern = path.join(dir, "stream%d.ts");

  try {
    if (fs.existsSync(playlist)) fs.unlinkSync(playlist);
    fs.readdirSync(dir).forEach(f => {
      if (f.endsWith(".ts") || f.endsWith(".m3u8")) {
        try { fs.unlinkSync(path.join(dir, f)); } catch (e) {}
      }
    });
  } catch (e) {}

  const stream = {
    username,
    apiKey,
    videoPort,
    audioPort,
    dir,
    playlist,
    segmentPattern,
    ffmpeg: null,
    videoPipe: null,
    ffmpegStarted: false,
    videoSocket: null,
    lastLogTime: 0,
    packetCount: 0,
    totalBytes: 0,
    createdAt: Date.now(),
    spsPpsSeen: false,
    buffer: Buffer.alloc(0),
    awaitingSize: true,
    sizeValue: 0,
    sizeBuffer: Buffer.alloc(4),
    sizePos: 0,
    pendingConfig: null,
    sentHeader: false
  };

  streams[username] = stream;
  STREAM_META[username] = {
    username,
    apiKey,
    videoPort,
    audioPort,
    dir,
    playlist,
    segmentPattern,
    createdAt: stream.createdAt
  };
  saveStreams();
  return stream;
}

function deleteStream(username) {
  const stream = streams[username];
  const meta = STREAM_META[username];
  if (!stream && !meta) return;

  if (stream && stream.ffmpeg) {
    try { stream.ffmpeg.kill("SIGTERM"); } catch (e) {}
  }

  if (stream && stream.videoSocket) {
    try { stream.videoSocket.destroy(); } catch (e) {}
  }

  const dir = stream ? stream.dir : (meta ? meta.dir : null);
  try {
    if (dir && fs.existsSync(dir)) {
      fs.readdirSync(dir).forEach(f => {
        try { fs.unlinkSync(path.join(dir, f)); } catch (e) {}
      });
      try { fs.rmdirSync(dir); } catch (e) {}
    }
  } catch (e) {}

  delete streams[username];
  delete STREAM_META[username];
  saveStreams();
}

function startFfmpeg(stream) {
  if (stream.ffmpegStarted) return;
  stream.ffmpegStarted = true;

  const args = [
    "-y",
    "-loglevel", "info",
    "-fflags", "+genpts",
    "-use_wallclock_as_timestamps", "1",
    "-f", "h264",
    "-i", "pipe:0",
    "-c:v", "libx264",
    "-preset", "ultrafast",
    "-tune", "zerolatency",
    "-g", "30",
    "-keyint_min", "30",
    "-pix_fmt", "yuv420p",
    "-f", "hls",
    "-hls_time", "1",
    "-hls_list_size", "3",
    "-hls_delete_threshold", "1",
    "-hls_flags", "delete_segments+append_list+omit_endlist+independent_segments",
    "-hls_segment_filename", stream.segmentPattern,
    stream.playlist
  ];

  console.log("Iniciando FFmpeg para", stream.username, "playlist:", stream.playlist);

  stream.ffmpeg = spawn("ffmpeg", args, { stdio: ["pipe", "pipe", "pipe"] });
  stream.videoPipe = stream.ffmpeg.stdin;

  stream.ffmpeg.stderr.on("data", (data) => {
    const line = data.toString();
    console.log("FFmpeg[" + stream.username + "]", line.trim());
  });

  stream.ffmpeg.on("close", (code) => {
    console.log("FFmpeg fechado para", stream.username, "code", code);
    stream.ffmpeg = null;
    stream.videoPipe = null;
    stream.ffmpegStarted = false;
  });

  stream.ffmpeg.on("error", (err) => {
    console.log("FFmpeg erro para", stream.username, err.message);
    stream.ffmpegStarted = false;
  });
}

function startVideoServer(stream) {
  const videoServer = net.createServer((socket) => {
    stream.videoSocket = socket;
    socket.setKeepAlive(true, 1000);
    socket.setNoDelay(true);

    console.log("Cliente de vídeo conectado para", stream.username);

    socket.on("data", (data) => {
      stream.buffer = Buffer.concat([stream.buffer, data]);

      while (true) {
        if (stream.awaitingSize) {
          const toRead = Math.min(stream.buffer.length, 4 - stream.sizePos);
          if (toRead <= 0) break;

          stream.buffer.copy(stream.sizeBuffer, stream.sizePos, 0, toRead);
          stream.buffer = stream.buffer.slice(toRead);
          stream.sizePos += toRead;

          if (stream.sizePos === 4) {
            stream.sizeValue = stream.sizeBuffer.readUInt32BE(0);
            stream.sizePos = 0;
            stream.awaitingSize = false;

            if (stream.sizeValue <= 0 || stream.sizeValue > 10000000) {
              console.log("Tamanho de frame inválido:", stream.sizeValue, "para", stream.username);
              stream.awaitingSize = true;
              stream.buffer = Buffer.alloc(0);
            }
          }
        } else {
          if (stream.buffer.length < stream.sizeValue) break;

          const frameData = stream.buffer.slice(0, stream.sizeValue);
          stream.buffer = stream.buffer.slice(stream.sizeValue);

          processFrame(stream, frameData);
          stream.awaitingSize = true;
        }
      }
    });

    socket.on("close", () => {
      console.log("Cliente de vídeo desconectado para", stream.username);
      stream.videoSocket = null;
    });

    socket.on("error", (err) => {
      console.log("Erro no socket de vídeo para", stream.username, err.message);
    });
  });

  videoServer.on("error", (err) => {
    console.log("Erro no servidor de vídeo para", stream.username, "porta", stream.videoPort, err.message);
  });

  videoServer.listen(stream.videoPort, "0.0.0.0", () => {
    console.log("Servidor de vídeo ouvindo na porta", stream.videoPort, "para", stream.username);
  });

  return videoServer;
}

function processFrame(stream, frameData) {
  if (!frameData || frameData.length === 0) return;
  if (!stream.ffmpegStarted) startFfmpeg(stream);

  if (!stream.sentHeader && looksLikeAvcConfig(frameData)) {
    stream.sentHeader = true;
    const converted = convertLengthPrefixedToAnnexB(frameData);
    if (stream.videoPipe && stream.videoPipe.writable) stream.videoPipe.write(converted);
    stream.packetCount++;
    stream.totalBytes += frameData.length;
    return;
  }

  const normalized = normalizeH264(frameData);

  if (isIdrFrame(normalized) && stream.pendingConfig) {
    if (stream.videoPipe && stream.videoPipe.writable) stream.videoPipe.write(stream.pendingConfig);
    stream.pendingConfig = null;
  }

  if (stream.videoPipe && stream.videoPipe.writable) {
    stream.videoPipe.write(normalized);
  }

  stream.packetCount++;
  stream.totalBytes += frameData.length;
  stream.lastLogTime = Date.now();
}

function looksLikeAvcConfig(buf) {
  if (!buf || buf.length < 8) return false;
  return !(buf[0] === 0x00 && buf[1] === 0x00 && buf[2] === 0x00 && buf[3] === 0x01);
}

function normalizeH264(frame) {
  if (startsWithAnnexB(frame)) return frame;
  return convertLengthPrefixedToAnnexB(frame);
}

function startsWithAnnexB(frame) {
  return frame.length >= 4 &&
    frame[0] === 0x00 &&
    frame[1] === 0x00 &&
    frame[2] === 0x00 &&
    frame[3] === 0x01;
}

function convertLengthPrefixedToAnnexB(frame) {
  const nalUnits = [];
  let i = 0;

  while (i + 4 <= frame.length) {
    const len = ((frame[i] & 0xFF) << 24)
      | ((frame[i + 1] & 0xFF) << 16)
      | ((frame[i + 2] & 0xFF) << 8)
      | (frame[i + 3] & 0xFF);

    i += 4;
    if (len <= 0 || i + len > frame.length) break;

    const nal = Buffer.allocUnsafe(4 + len);
    START_CODE.copy(nal, 0);
    frame.copy(nal, 4, i, i + len);
    nalUnits.push(nal);
    i += len;
  }

  if (nalUnits.length === 0) return frame;
  return Buffer.concat(nalUnits);
}

function isIdrFrame(frame) {
  if (!frame || frame.length < 5) return false;
  const idx = startsWithAnnexB(frame) ? 4 : 0;
  const nalType = frame[idx] & 0x1f;
  return nalType === 5;
}

const START_CODE = Buffer.from([0x00, 0x00, 0x00, 0x01]);

function startAudioServer(stream) {
  const audioServer = net.createServer((socket) => {
    console.log("Cliente de áudio conectado para", stream.username);
    socket.on("data", () => {});
    socket.on("error", () => {});
    socket.on("close", () => {
      console.log("Cliente de áudio desconectado para", stream.username);
    });
  });

  audioServer.on("error", (err) => {
    console.log("Erro no servidor de áudio para", stream.username, "porta", stream.audioPort, err.message);
  });

  audioServer.listen(stream.audioPort, "0.0.0.0", () => {
    console.log("Servidor de áudio ouvindo na porta", stream.audioPort, "para", stream.username);
  });

  return audioServer;
}

function checkOrigin(req, res) {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.length > 0 && origin && !ALLOWED_ORIGINS.includes(origin)) {
    res.writeHead(403, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Origin not allowed" }));
    return false;
  }
  return true;
}

function setCorsHeaders(res, origin) {
  if (origin) res.setHeader("Access-Control-Allow-Origin", origin);
  else if (ALLOWED_ORIGINS.length > 0) res.setHeader("Access-Control-Allow-Origin", ALLOWED_ORIGINS[0]);
  else res.setHeader("Access-Control-Allow-Origin", "*");

  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Username, X-Api-Key");
}

function findUsernameByApiKey(apiKey) {
  for (const username in USER_KEYS) {
    if (USER_KEYS[username] === apiKey) return username;
  }
  return null;
}

function validateApiKey(req, res, origin) {
  const auth = req.headers.authorization || "";
  if (!auth.startsWith("Bearer ")) {
    setCorsHeaders(res, origin);
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Unauthorized" }));
    return null;
  }

  const apiKey = auth.slice(7);
  const username = findUsernameByApiKey(apiKey);

  if (!username) {
    setCorsHeaders(res, origin);
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Invalid API key" }));
    return null;
  }

  return { username, apiKey };
}

function requireStream(req, res, origin) {
  const authObj = validateApiKey(req, res, origin);
  if (!authObj) return null;

  const stream = streams[authObj.username];
  if (!stream || stream.apiKey !== authObj.apiKey) {
    setCorsHeaders(res, origin);
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Stream not found" }));
    return null;
  }

  return stream;
}

const server = http.createServer((req, res) => {
  const origin = req.headers.origin;

  if (req.method === "OPTIONS") {
    setCorsHeaders(res, origin);
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.url === "/api/login" && req.method === "POST") {
    if (!checkOrigin(req, res)) return;
    let body = "";
    req.on("data", chunk => { body += chunk; });
    req.on("end", () => {
      try {
        const { username, password } = JSON.parse(body);

        if (!username || !password) {
          setCorsHeaders(res, origin);
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Missing username or password" }));
          return;
        }

        if (password !== getUserPassword(username)) {
          setCorsHeaders(res, origin);
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Invalid credentials" }));
          return;
        }

        if (!USER_KEYS[username]) {
          USER_KEYS[username] = crypto.randomBytes(24).toString("hex");
          saveUsers();
        }

        const apiKey = USER_KEYS[username];
        setCorsHeaders(res, origin);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ username, apiKey }, null, 2));
      } catch (e) {
        setCorsHeaders(res, origin);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid JSON" }));
      }
    });
    return;
  }

  if (req.url === "/api/stream" && req.method === "POST") {
    if (!checkOrigin(req, res)) return;
    const authObj = validateApiKey(req, res, origin);
    if (!authObj) return;

    if (streams[authObj.username] || STREAM_META[authObj.username]) {
      deleteStream(authObj.username);
    }

    const newStream = createStream(authObj.username, authObj.apiKey);
    startVideoServer(newStream);
    startAudioServer(newStream);

    const baseUrl = process.env.PUBLIC_BASE_URL || `http://${req.headers.host || "localhost"}`;

    setCorsHeaders(res, origin);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      username: newStream.username,
      videoPort: newStream.videoPort,
      audioPort: newStream.audioPort,
      baseUrl,
      streamUrl: `${baseUrl}/live/${newStream.username}/${newStream.apiKey}/stream.m3u8`
    }, null, 2));
    return;
  }

  if (req.url === "/api/stream" && req.method === "DELETE") {
    if (!checkOrigin(req, res)) return;
    const authObj = validateApiKey(req, res, origin);
    if (!authObj) return;

    deleteStream(authObj.username);

    setCorsHeaders(res, origin);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }, null, 2));
    return;
  }

  if (req.url === "/api/stream" && req.method === "GET") {
    if (!checkOrigin(req, res)) return;
    const stream = requireStream(req, res, origin);
    if (!stream) return;

    setCorsHeaders(res, origin);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      username: stream.username,
      videoPort: stream.videoPort,
      audioPort: stream.audioPort,
      createdAt: stream.createdAt,
      packetCount: stream.packetCount,
      totalBytes: stream.totalBytes
    }, null, 2));
    return;
  }

  if (req.url === "/api/live" && req.method === "GET") {
    if (!checkOrigin(req, res)) return;

    const username = req.headers["x-username"];
    const apiKey = req.headers["x-api-key"];

    if (!username || !apiKey || !streams[username] || streams[username].apiKey !== apiKey) {
      setCorsHeaders(res, origin);
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Forbidden" }));
      return;
    }

    const stream = streams[username];
    const base = process.env.PUBLIC_BASE_URL || `http://${req.headers.host || "localhost"}`;
    const streamUrl = `${base}/live/${stream.username}/${stream.apiKey}/stream.m3u8`;

    setCorsHeaders(res, origin);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      username: stream.username,
      streamUrl
    }, null, 2));
    return;
  }

  if (req.url === "/") {
    setCorsHeaders(res, origin);
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(getHtmlPage());
    return;
  }

  const liveMatch = new RegExp("^\\/live\\/([a-zA-Z0-9_-]+)\\/([a-f0-9]+)\\/stream\\.m3u8$").exec(req.url);
  if (liveMatch) {
    const [, username, apiKey] = liveMatch;
    const stream = streams[username];

    if (!stream || stream.apiKey !== apiKey) {
      setCorsHeaders(res, origin);
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Forbidden" }));
      return;
    }

    if (!fs.existsSync(stream.playlist)) {
      const fallback = `#EXTM3U#EXT-X-VERSION:3#EXT-X-TARGETDURATION:1#EXT-X-MEDIA-SEQUENCE:0#EXT-X-PLAYLIST-TYPE:EVENT`;
      fs.writeFileSync(stream.playlist, fallback, "utf8");
    }

    setCorsHeaders(res, origin);
    res.writeHead(200, {
      "Content-Type": "application/vnd.apple.mpegurl",
      "Cache-Control": "no-cache, no-store, must-revalidate",
      "Pragma": "no-cache",
      "Expires": "0"
    });
    fs.createReadStream(stream.playlist).pipe(res);
    return;
  }

  const segmentMatch = new RegExp("^\\/live\\/([a-zA-Z0-9_-]+)\\/([a-f0-9]+)\\/stream(\\d+)\\.ts$").exec(req.url);
  if (segmentMatch) {
    const [, username, apiKey, index] = segmentMatch;
    const stream = streams[username];

    if (!stream || stream.apiKey !== apiKey) {
      setCorsHeaders(res, origin);
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Forbidden" }));
      return;
    }

    const fileName = `stream${index}.ts`;
    const filePath = path.join(stream.dir, fileName);

    if (fs.existsSync(filePath)) {
      const stat = fs.statSync(filePath);
      setCorsHeaders(res, origin);
      res.writeHead(200, {
        "Content-Type": "video/mp2t",
        "Content-Length": stat.size,
        "Cache-Control": "no-cache, no-store, must-revalidate"
      });
      fs.createReadStream(filePath).pipe(res);
    } else {
      setCorsHeaders(res, origin);
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Segment not found");
    }
    return;
  }

  setCorsHeaders(res, origin);
  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
});

server.on("error", (err) => {
  console.log("Erro no servidor HTTP:", err.message);
});

server.listen(HTTP_PORT, "0.0.0.0", () => {
  console.log(`HTTP em http://localhost:${HTTP_PORT}`);
});

function getHtmlPage() {
  return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Camera Stream</title>
  <style>
    body { margin: 0; background: #000; display: flex; align-items: center; justify-content: center; height: 100vh; overflow: hidden; }
    video { width: 100%; height: 100%; max-width: 100%; max-height: 100%; background: #000; }
  </style>
</head>
<body>
  <video id="video" controls autoplay playsinline></video>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <script>
    (function() {
      var video = document.getElementById('video');
      var params = new URLSearchParams(window.location.search);
      var src = params.get('src');

      if (!src) {
        document.body.innerHTML = '<div style="color:#fff;font-family:sans-serif">Informe a URL da stream em ?src=</div>';
        return;
      }

      if (window.Hls && Hls.isSupported()) {
        var hls = new Hls({
          liveSyncDurationCount: 2,
          maxBufferLength: 5,
          maxMaxBufferLength: 10,
          backBufferLength: 0,
          enableWorker: true,
          lowLatencyMode: false,
          startPosition: 0
        });
        hls.loadSource(src);
        hls.attachMedia(video);
        hls.on(Hls.Events.MANIFEST_PARSED, function() {
          video.play().catch(function(err) { console.log('Play error:', err); });
        });
      } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
        video.src = src;
        video.addEventListener('loadedmetadata', function() {
          video.play().catch(function(err) { console.log('Play error:', err); });
        });
      } else {
        document.body.innerHTML = '<div style="color:#fff;font-family:sans-serif">HLS não suportado.</div>';
      }
    })();
  </script>
</body>
</html>`;
}
