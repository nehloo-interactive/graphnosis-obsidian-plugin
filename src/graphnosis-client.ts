import { requestUrl, type RequestUrlResponse } from 'obsidian';

/** Thin HTTP client for the Graphnosis local MCP bridge (mcp-http-server.ts). */
export class GraphnosisClient {
  private sessionId: string | undefined;
  private initialized = false;
  private requestId = 0;

  constructor(
    private readonly baseUrl: string,
    private readonly token: string,
  ) {}

  async recall(query: string, maxTokens: number): Promise<string> {
    const result = await this.callTool('recall', { query, maxTokens }) as {
      content?: Array<{ type: string; text?: string }>;
    } | undefined;
    if (result?.content && Array.isArray(result.content)) {
      return result.content.map(c => c.text ?? '').filter(Boolean).join('\n');
    }
    return '(no results)';
  }

  async remember(text: string, label?: string, targetEngram?: string): Promise<void> {
    await this.callTool('remember', {
      text,
      kind: 'ai-conversation',
      ...(label ? { label } : {}),
      ...(targetEngram ? { target_engram: targetEngram } : {}),
    });
  }

  async vitality(): Promise<number | null> {
    try {
      const result = await this.callTool('vitality', {}) as {
        content?: Array<{ type: string; text?: string }>;
      } | undefined;
      const text = result?.content?.find(c => c.type === 'text')?.text ?? '';
      // Response is JSON: { overall: 0-100, byGraph: {...}, computedAt: N }
      const parsed = JSON.parse(text.split('\n---')[0].trim()) as { overall?: number };
      return typeof parsed.overall === 'number' ? parsed.overall : null;
    } catch {
      return null;
    }
  }

  async ingestBatch(
    items: Array<{ text: string; label?: string; target_engram?: string }>,
  ): Promise<void> {
    await this.callTool('ingest_batch', { items });
  }

  async listEngrams(): Promise<string[]> {
    try {
      const result = await this.callTool('list_engrams', {}) as {
        content?: Array<{ type: string; text?: string }>;
      } | undefined;
      const text = result?.content?.find(c => c.type === 'text')?.text ?? '';
      // Response is a JSON array: [{ graphId, displayName, tier, archived, ... }]
      const rows = JSON.parse(text) as Array<{ graphId: string; archived?: boolean }>;
      return rows.filter(r => !r.archived).map(r => r.graphId);
    } catch {
      return [];
    }
  }

  /** Returns true if the bridge is reachable and the MCP handshake completes. */
  async ping(): Promise<boolean> {
    try {
      this.reset();
      await this.ensureSession();
      return true;
    } catch {
      return false;
    }
  }

  private nextId(): number {
    return ++this.requestId;
  }

  private reset(): void {
    this.sessionId = undefined;
    this.initialized = false;
  }

  /**
   * Complete the MCP initialization handshake once per session:
   *   1. POST `initialize` (no session id) → server allocates a session and
   *      returns it in the `Mcp-Session-Id` response header.
   *   2. POST the `notifications/initialized` notification with that session id.
   * StreamableHTTPServerTransport rejects `tools/call` until this is done.
   */
  private async ensureSession(): Promise<void> {
    if (this.initialized && this.sessionId) return;

    const initRes = await this.post({
      jsonrpc: '2.0',
      id: this.nextId(),
      method: 'initialize',
      params: {
        protocolVersion: '2024-11-05',
        capabilities: {},
        clientInfo: { name: 'obsidian-graphnosis', version: '0.1.0' },
      },
    }, 5000);

    if (initRes.status < 200 || initRes.status >= 300) {
      throw new Error(`Graphnosis bridge returned HTTP ${initRes.status}`);
    }

    const sid = initRes.headers['mcp-session-id'] ?? initRes.headers['Mcp-Session-Id'];
    if (!sid) throw new Error('Graphnosis bridge did not return a session id');
    this.sessionId = sid;

    const init = this.extractRpc(initRes);
    if (init.error) {
      throw new Error(`Graphnosis init failed: ${init.error.message ?? JSON.stringify(init.error)}`);
    }

    // Notification (no id, no response body expected) — finishes the handshake.
    await this.post({ jsonrpc: '2.0', method: 'notifications/initialized' }, 5000);
    this.initialized = true;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      'Authorization': `Bearer ${this.token}`,
      'Content-Type': 'application/json',
      // Streamable-HTTP MCP transport (StreamableHTTPServerTransport) rejects with
      // 406 unless the client accepts both JSON and the SSE stream.
      'Accept': 'application/json, text/event-stream',
    };
    if (this.sessionId) h['Mcp-Session-Id'] = this.sessionId;
    return h;
  }

  private async callTool(name: string, args: Record<string, unknown>): Promise<unknown> {
    await this.ensureSession();

    const send = () => this.post({
      jsonrpc: '2.0',
      id: this.nextId(),
      method: 'tools/call',
      params: { name, arguments: args },
    }, 15_000);

    let res = await send();

    // The bridge prunes idle sessions after ~10 min, then answers 404. Re-handshake once.
    if (res.status === 404) {
      this.reset();
      await this.ensureSession();
      res = await send();
    }

    if (res.status < 200 || res.status >= 300) {
      throw new Error(`Graphnosis bridge returned HTTP ${res.status}`);
    }

    const json = this.extractRpc(res);
    if (json.error) {
      throw new Error(`Graphnosis MCP error: ${json.error.message ?? JSON.stringify(json.error)}`);
    }
    return json.result;
  }

  /**
   * Parse a bridge response into a JSON-RPC payload. The StreamableHTTP transport
   * replies with `text/event-stream` (one `event: message` / `data: {…}` block),
   * so we extract the data lines and JSON-parse them; falls back to plain JSON.
   */
  private extractRpc(res: RequestUrlResponse): { result?: unknown; error?: { message?: string; code?: number } } {
    const contentType = (res.headers['content-type'] ?? res.headers['Content-Type'] ?? '').toLowerCase();
    const body = res.text;

    if (contentType.includes('text/event-stream')) {
      for (const block of body.split(/\r?\n\r?\n/)) {
        const data = block
          .split(/\r?\n/)
          .filter(line => line.startsWith('data:'))
          .map(line => line.replace(/^data:\s?/, ''))
          .join('\n');
        if (!data) continue;
        try {
          const msg = JSON.parse(data) as { result?: unknown; error?: { message?: string; code?: number } };
          if (msg.result !== undefined || msg.error !== undefined) return msg;
        } catch {
          // Not a JSON-RPC payload (e.g. a comment/keepalive) — skip.
        }
      }
      throw new Error('Graphnosis bridge returned no JSON-RPC message');
    }

    return JSON.parse(body) as { result?: unknown; error?: { message?: string; code?: number } };
  }

  /**
   * POST a JSON-RPC body via Obsidian's requestUrl (avoids CORS; works cross-platform).
   * requestUrl has no native timeout, so we race it against a timer; `throw: false`
   * lets us inspect non-2xx statuses instead of having requestUrl throw.
   */
  private async post(body: unknown, timeoutMs: number): Promise<RequestUrlResponse> {
    let timer: number | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = window.setTimeout(
        () => reject(new Error('Graphnosis bridge timed out')),
        timeoutMs,
      );
    });
    try {
      return await Promise.race([
        requestUrl({
          url: this.baseUrl,
          method: 'POST',
          headers: this.headers(),
          body: JSON.stringify(body),
          throw: false,
        }),
        timeout,
      ]);
    } finally {
      window.clearTimeout(timer);
    }
  }
}
