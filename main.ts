// main.ts - Deno 原生 VLESS 节点实现
const rawUUID = Deno.env.get("UUID") || "d342d11e-d424-4583-b36e-524ab1f0afa4";
const targetUUID = rawUUID.toLowerCase().replace(/-/g, "");

Deno.serve(async (req) => {
  const url = new URL(req.url);
  const upgrade = req.headers.get("upgrade") || "";

  // 1. 处理 WebSocket 连接（VLESS 传输）
  if (upgrade.toLowerCase() === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    handleVless(socket);
    return response;
  }

  // 2. 访问 /UUID 或 /sub 时输出节点订阅链接
  const host = req.headers.get("host") || "";
  if (url.pathname.includes(rawUUID) || url.pathname === "/sub") {
    const vlessLink = `vless://${rawUUID}@${host}:443?encryption=none&security=tls&type=ws&host=${host}&path=%2F#Deno-VLESS`;
    return new Response(
      `节点链接 (直接复制导入客户端):\n\n${vlessLink}\n`,
      { headers: { "content-type": "text/plain;charset=utf-8" } }
    );
  }

  // 3. 普通 HTTP 访问显示伪装页面
  return new Response(
    `<!DOCTYPE html><html><head><meta charset="utf-8"><title>Welcome</title></head><body><h1>Service is Online</h1><p>Deno Edge Runtime.</p></body></html>`,
    { headers: { "content-type": "text/html;charset=utf-8" } }
  );
});

async function handleVless(socket: WebSocket) {
  socket.binaryType = "arraybuffer";
  let tcpConn: Deno.TcpConn | null = null;
  let isHandshakeDone = false;

  socket.onmessage = async (e) => {
    if (!(e.data instanceof ArrayBuffer)) return;
    const chunk = new Uint8Array(e.data);

    if (!isHandshakeDone) {
      if (chunk.byteLength < 24) return socket.close();

      const version = chunk[0];
      const clientUUID = Array.from(chunk.subarray(1, 17))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");

      if (clientUUID !== targetUUID) {
        socket.close();
        return;
      }

      const addonLen = chunk[17];
      let offset = 18 + addonLen;
      const cmd = chunk[offset++]; // 1 为 TCP

      if (cmd !== 1) {
        socket.close();
        return;
      }

      const port = (chunk[offset] << 8) | chunk[offset + 1];
      offset += 2;
      const addrType = chunk[offset++];
      let address = "";

      if (addrType === 1) {
        // IPv4
        address = `${chunk[offset]}.${chunk[offset + 1]}.${chunk[offset + 2]}.${chunk[offset + 3]}`;
        offset += 4;
      } else if (addrType === 2) {
        // Domain
        const len = chunk[offset++];
        address = new TextDecoder().decode(chunk.subarray(offset, offset + len));
        offset += len;
      } else if (addrType === 3) {
        // IPv6
        const parts = [];
        for (let i = 0; i < 16; i += 2) {
          parts.push(((chunk[offset + i] << 8) | chunk[offset + i + 1]).toString(16));
        }
        address = parts.join(":");
        offset += 16;
      } else {
        socket.close();
        return;
      }

      try {
        tcpConn = await Deno.connect({ hostname: address, port });
        isHandshakeDone = true;

        // 回复 VLESS 握手响应
        socket.send(new Uint8Array([version, 0]));

        // 转发初始附带的有效负载
        const payload = chunk.subarray(offset);
        if (payload.byteLength > 0) {
          await tcpConn.write(payload);
        }

        // 启动后台管道：将远端 TCP 数据转发回 WebSocket
        (async () => {
          const buf = new Uint8Array(32768);
          try {
            while (true) {
              const n = await tcpConn!.read(buf);
              if (n === null) break;
              if (socket.readyState === WebSocket.OPEN) {
                socket.send(buf.subarray(0, n));
              } else {
                break;
              }
            }
          } catch {
            // 连接中断处理
          } finally {
            try { tcpConn?.close(); } catch {}
            try { socket.close(); } catch {}
          }
        })();
      } catch {
        socket.close();
      }
    } else {
      // 握手完成后的常规数据转发
      if (tcpConn) {
        try {
          await tcpConn.write(chunk);
        } catch {
          socket.close();
        }
      }
    }
  };

  socket.onclose = () => {
    try { tcpConn?.close(); } catch {}
  };
  socket.onerror = () => {
    try { tcpConn?.close(); } catch {}
  };
}
