/**
 * cline-proxy.js
 *
 * Lightweight zero-dependency local proxy server (port 7650).
 * Sits between the Cline extension and a Cloudflare AI Gateway endpoint.
 *
 * Responsibilities:
 *  - Forward all requests (headers, body, auth) verbatim to gateway.ai.cloudflare.com
 *  - For SSE / text/event-stream responses: sanitize malformed GLM tool-call tokens
 *  - For all other responses: pipe bytes straight through (zero transformation)
 *
 * Sanitization rules applied to the ASSEMBLED content stream:
 *  1. <tool_call>toolName>  →  <toolName>
 *  2. <arg_value>           →  (removed)
 *  3. </arg_value>          →  (removed)
 *
 * Why content-stream parsing instead of raw-wire regex:
 *  GLM streams the tool-call token split across multiple SSE chunks, each
 *  carrying only a fragment inside its own JSON envelope:
 *    chunk 1 content: "<tool_call>"
 *    chunk 2 content: "read_file"
 *    chunk 3 content: ">\n"
 *  A raw-wire regex will never see these fragments together. We parse each
 *  data: line, accumulate the content field, sanitize the assembled string,
 *  and re-inject the corrected content before forwarding.
 */

'use strict';

const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');

// ─── Debug Log ────────────────────────────────────────────────────────────────

const LOG_FILE  = path.join(__dirname, 'proxy-debug.log');
const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

function debugLog(label, data) {
  const ts = new Date().toISOString();
  logStream.write(`\n[${ts}] ${label}\n`);
  logStream.write(JSON.stringify(data) + '\n');
  logStream.write('---\n');
}

// ─── Configuration ────────────────────────────────────────────────────────────

const PROXY_PORT       = 7650;
const TARGET_HOST      = 'gateway.ai.cloudflare.com';
const TARGET_BASE_PATH = '/v1/{accountid}/default/compat';
const TARGET_PORT      = 443;

/**
 * Minimum number of accumulated *content* characters to hold back before
 * flushing. Must exceed the longest possible cross-chunk pattern (~62 chars).
 */
const BUFFER_TAIL = 128;

// ─── Stream Sanitizer ─────────────────────────────────────────────────────────

/**
 * Trim any trailing incomplete <tool_call>... sequence from a content slice
 * so Rule 1 isn't applied to a slice that ends with "<tool_call>" or a partial
 * tool name before the closing ">" has arrived in a later chunk.
 *
 * Cases held back (returned as a shorter prefix):
 *   "<tool_ca"           – partial opening tag
 *   "<tool_call>"        – complete opening tag, tool name not yet arrived
 *   "<tool_call>read_fi" – tool name truncated, closing > not arrived
 *
 * False positives (e.g. "<too" being held back when it's actually "<tooltip>")
 * are harmless — they delay flushing by one cycle and are resolved on the next.
 *
 * @param {string} slice
 * @returns {string} safe prefix to flush
 */
function trimIncompletePattern(slice) {
  // Hold back if there is an open <task_progress> block without its closing tag.
  // These blocks can span hundreds of chars across many chunks; we must keep
  // them in the accumulator until </task_progress> arrives so the strip regex
  // in sanitize() can remove the whole block in one pass.
  const tpOpen = slice.lastIndexOf('<task_progress>');
  if (tpOpen !== -1 && slice.indexOf('</task_progress>', tpOpen) === -1) {
    return slice.slice(0, tpOpen);
  }

  // Hold back partial or unresolved <tool_call>... sequences.
  const lastAngle = slice.lastIndexOf('<');
  if (lastAngle === -1) return slice;

  const tail = slice.slice(lastAngle);

  // Partial prefix of the literal string "<tool_call>"
  if ('<tool_call>'.startsWith(tail)) return slice.slice(0, lastAngle);

  // Complete "<tool_call>" tag + optional partial tool name without closing ">"
  if (/^<tool_call>[a-zA-Z0-9_-]*$/.test(tail)) return slice.slice(0, lastAngle);

  return slice;
}

/**
 * Sanitize a fully-assembled content string segment.
 *
 * @param {string} text
 * @returns {string}
 */
function sanitize(text) {
  // Rule 1: <tool_call>toolName>  →  <toolName>
  text = text.replace(/<tool_call>([a-zA-Z0-9_-]+)>/g, '<$1>');

  // Rule 2 & 3: strip arg_value wrapper tags
  text = text.replace(/<\/arg_value>/g, '');
  text = text.replace(/<arg_value>/g, '');

  // Rule 4: ensure tool opening tags start on their own line.
  // Cline's parser requires <toolName> at a line boundary. The GLM model
  // often emits conversational prose immediately before the tool tag with no
  // newline separator (e.g. "...informed plan.<read_file>"). Insert \n before
  // any <word> tag that is directly preceded by a non-newline, non-< character.
  text = text.replace(/([^\n<])(<[a-zA-Z][a-zA-Z0-9_-]*>)/g, '$1\n$2');

  // Rule 5: strip <task_progress>...</task_progress> blocks entirely.
  // The GLM model outputs these as real XML parameter blocks inside tool calls
  // (e.g. inside <write_to_file>), causing Cline to write the checklist content
  // into the target file. Remove the whole block including its content.
  text = text.replace(/<task_progress>[\s\S]*?<\/task_progress>/g, '');

  return text;
}

// ─── SSE Content-Stream Sanitizer ─────────────────────────────────────────────

/**
 * Intercept an upstream SSE response, parse each data: JSON line, accumulate
 * the content deltas, sanitize the assembled content string, and re-inject
 * the corrected content into the SSE envelopes before forwarding.
 *
 * Key invariant:
 *   contentAcc.length === sum of pendingMsgs[i].contentLen
 *
 * @param {http.IncomingMessage} proxyRes
 * @param {http.ServerResponse}  clientRes
 */
function pipeSanitizedStream(proxyRes, clientRes) {
  let wireBuffer  = '';   // raw bytes awaiting a complete \n\n boundary
  let contentAcc  = '';   // accumulated original content deltas (not yet emitted)
  let pendingMsgs = [];   // { dataPayload: string, contentLen: number }

  /**
   * Flush the safe leading portion of contentAcc through the sanitizer.
   *
   * force=false : hold back BUFFER_TAIL chars so cross-chunk patterns complete
   * force=true  : flush everything (end of stream)
   *
   * Strategy: the sanitized content is placed entirely in the FIRST message of
   * the flushed batch; subsequent messages get content='' so all SSE events
   * and metadata still arrive at Cline in order.
   */
  function flushSafe(force) {
    const safeLen = force
      ? contentAcc.length
      : Math.max(0, contentAcc.length - BUFFER_TAIL);

    if (safeLen <= 0 || pendingMsgs.length === 0) return;

    // Walk pendingMsgs until cumulative contentLen >= safeLen.
    let covered = 0;
    let count   = 0;
    for (const msg of pendingMsgs) {
      covered += msg.contentLen;
      count++;
      if (covered >= safeLen) break;
    }

    let originalSlice = contentAcc.slice(0, covered);

    // Guard (non-force only): if the slice ends with a partial or unresolved
    // <tool_call>... pattern, trim back and recalculate the batch so we never
    // emit an incomplete pattern that Rule 1 can no longer match next cycle.
    if (!force) {
      const trimmed = trimIncompletePattern(originalSlice);
      if (trimmed.length < originalSlice.length) {
        const trimmedLen = trimmed.length;
        covered = 0;
        count   = 0;
        for (const msg of pendingMsgs) {
          if (covered + msg.contentLen > trimmedLen) break;
          covered += msg.contentLen;
          count++;
        }
        if (count === 0) return;  // nothing safe to flush right now
        originalSlice = contentAcc.slice(0, covered);
      }
    }

    const sanitizedSlice = sanitize(originalSlice);

    if (originalSlice !== sanitizedSlice) {
      debugLog('SANITIZED', { before: originalSlice, after: sanitizedSlice });
    }

    contentAcc = contentAcc.slice(covered);
    const batch = pendingMsgs.splice(0, count);

    for (let i = 0; i < batch.length; i++) {
      const { dataPayload } = batch[i];
      try {
        const parsed = JSON.parse(dataPayload);
        // First message in batch carries all sanitized content; rest are emptied.
        parsed.choices[0].delta.content = (i === 0) ? sanitizedSlice : '';
        clientRes.write('data: ' + JSON.stringify(parsed) + '\n\n', 'utf8');
      } catch (_) {
        // Fallback: emit raw for first message only.
        if (i === 0) {
          clientRes.write('data: ' + dataPayload + '\n\n', 'utf8');
        }
      }
    }
  }

  /**
   * Drain wireBuffer, processing each complete \n\n-delimited SSE message.
   */
  function processWireBuffer() {
    let idx;
    while ((idx = wireBuffer.indexOf('\n\n')) !== -1) {
      const message = wireBuffer.slice(0, idx);
      wireBuffer    = wireBuffer.slice(idx + 2);

      if (!message.trim()) continue;

      // Non-data lines (SSE comments, event: fields) — pass through directly.
      if (!message.startsWith('data: ')) {
        clientRes.write(message + '\n\n', 'utf8');
        continue;
      }

      const dataPayload = message.slice(6).trim();

      // Terminal SSE marker — force-flush all buffered content first.
      if (dataPayload === '[DONE]') {
        flushSafe(true);
        clientRes.write('data: [DONE]\n\n', 'utf8');
        continue;
      }

      let parsed;
      try {
        parsed = JSON.parse(dataPayload);
      } catch (_) {
        // Unparseable chunk — flush safe zone then emit raw.
        flushSafe(false);
        clientRes.write(message + '\n\n', 'utf8');
        continue;
      }

      const content = parsed?.choices?.[0]?.delta?.content;

      if (typeof content === 'string' && content.length > 0) {
        // Content delta — buffer for sanitization.
        contentAcc += content;
        pendingMsgs.push({ dataPayload, contentLen: content.length });
      } else {
        // Metadata-only chunk (finish_reason, etc.)
        // Flush safe content so it arrives before this metadata event.
        flushSafe(false);
        clientRes.write(message + '\n\n', 'utf8');
      }

      // After each new message, attempt to flush the safe zone.
      flushSafe(false);
    }
  }

  proxyRes.on('data', (chunk) => {
    wireBuffer += chunk.toString('utf8');
    processWireBuffer();
  });

  proxyRes.on('end', () => {
    processWireBuffer();
    flushSafe(true);
    clientRes.end();
  });

  proxyRes.on('error', (err) => {
    console.error('[proxy] Upstream stream error:', err.message);
    clientRes.end();
  });
}

// ─── Request Handler ──────────────────────────────────────────────────────────

/**
 * Build forwarding headers: copy all request headers, override host,
 * strip hop-by-hop headers that must not cross a proxy boundary.
 *
 * @param {http.IncomingMessage} req
 * @returns {object}
 */
function buildUpstreamHeaders(req) {
  const headers = Object.assign({}, req.headers);

  // Clean hostname only — no port suffix — required for TLS SNI resolution.
  headers['host'] = TARGET_HOST;

  // Hop-by-hop headers must not be forwarded.
  delete headers['connection'];
  delete headers['keep-alive'];
  delete headers['proxy-authorization'];
  delete headers['proxy-connection'];
  delete headers['transfer-encoding'];
  delete headers['upgrade'];
  delete headers['te'];

  return headers;
}

function handleRequest(req, res) {
  const upstreamOptions = {
    hostname : TARGET_HOST,
    port     : TARGET_PORT,
    path     : TARGET_BASE_PATH + req.url,
    method   : req.method,
    headers  : buildUpstreamHeaders(req),
  };

  console.log(`[proxy] ${req.method} ${req.url}`);

  const proxyReq = https.request(upstreamOptions, (proxyRes) => {
    const statusCode  = proxyRes.statusCode;
    const contentType = (proxyRes.headers['content-type'] || '').toLowerCase();
    const isSSE       = contentType.includes('text/event-stream');

    const responseHeaders = Object.assign({}, proxyRes.headers);
    if (isSSE) {
      delete responseHeaders['content-length'];
      responseHeaders['transfer-encoding']  = 'chunked';
      // Prevent Cloudflare Tunnel (and any nginx-based reverse proxy) from
      // buffering the SSE stream — without this, tokens batch up and Cline
      // receives no output until the connection closes.
      responseHeaders['x-accel-buffering'] = 'no';
      responseHeaders['cache-control']     = 'no-cache';
    }

    res.writeHead(statusCode, responseHeaders);

    if (isSSE) {
      console.log('[proxy] SSE stream — content-stream sanitization active');
      pipeSanitizedStream(proxyRes, res);
    } else {
      proxyRes.pipe(res);
    }
  });

  proxyReq.on('error', (err) => {
    console.error('[proxy] Upstream request error:', err.message);
    if (!res.headersSent) {
      res.writeHead(502, { 'content-type': 'application/json' });
    }
    res.end(JSON.stringify({ error: 'proxy_upstream_error', message: err.message }));
  });

  req.pipe(proxyReq);
}

// ─── Server Bootstrap ─────────────────────────────────────────────────────────

const server = http.createServer(handleRequest);

server.on('error', (err) => {
  console.error('[proxy] Server error:', err.message);
  if (err.code === 'EADDRINUSE') {
    console.error(`[proxy] Port ${PROXY_PORT} is already in use.`);
    process.exit(1);
  }
});

server.listen(PROXY_PORT, '127.0.0.1', () => {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║          Cline ↔ Cloudflare AI Gateway Proxy         ║');
  console.log('╠══════════════════════════════════════════════════════╣');
  console.log(`║  Listening  : http://127.0.0.1:${PROXY_PORT}               ║`);
  console.log(`║  Forwarding : https://${TARGET_HOST}              ║`);
  console.log(`║  Base path  : ${TARGET_BASE_PATH}  ║`);
  console.log('║  Mode       : SSE content-stream sanitize            ║');
  console.log('╚══════════════════════════════════════════════════════╝');
  console.log('');
});