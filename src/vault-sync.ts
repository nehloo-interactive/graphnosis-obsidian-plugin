import type { TFile, Vault } from 'obsidian';
import type { GraphnosisClient } from './graphnosis-client';
import type { PluginSettings } from './settings';

export class VaultSync {
  private queue: Map<string, TFile> = new Map();
  private timer: number | undefined;

  constructor(
    private readonly vault: Vault,
    private readonly client: GraphnosisClient,
    private readonly settings: PluginSettings,
    private readonly onSyncAt: (ts: number) => Promise<void>,
  ) {}

  enqueue(file: TFile): void {
    if (file.path.startsWith(`${this.vault.configDir}/`) || !file.path.endsWith('.md')) return;
    this.queue.set(file.path, file);
    window.clearTimeout(this.timer);
    this.timer = window.setTimeout(() => void this.flush(), 5_000);
  }

  /**
   * Queue all .md files modified after `since` (Unix ms). Runs in the background.
   *
   * NOTE ON VAULT ENUMERATION (getMarkdownFiles): this is used only by the optional
   * Vault sync feature, which is off by default. When the user explicitly enables it,
   * the plugin lists markdown files to find ones modified since the last sync (catch-up).
   * No file contents are read or transmitted unless sync is enabled, and the config dir
   * is excluded. There's no way to offer catch-up sync without enumerating notes.
   */
  async catchUp(since: number): Promise<void> {
    const files = this.vault.getMarkdownFiles();
    for (const file of files) {
      if (file.path.startsWith(`${this.vault.configDir}/`)) continue;
      if (file.stat.mtime > since) {
        this.queue.set(file.path, file);
      }
    }
    if (this.queue.size > 0) {
      window.clearTimeout(this.timer);
      this.timer = window.setTimeout(() => void this.flush(), 5_000);
    }
  }

  private async flush(): Promise<void> {
    if (!this.queue.size) return;

    const files = [...this.queue.values()];
    this.queue.clear();

    for (let i = 0; i < files.length; i += 20) {
      const chunk = files.slice(i, i + 20);
      const items: Array<{ text: string; label: string; target_engram: string }> = [];

      for (const file of chunk) {
        try {
          const content = await this.vault.cachedRead(file);
          if (!content.trim()) continue;
          items.push({
            text: `# ${file.basename}\n\n${content}`,
            label: file.basename,
            target_engram: this.settings.targetEngram,
          });
        } catch {
          // Skip unreadable files silently
        }
      }

      if (items.length === 0) continue;

      try {
        await this.client.ingestBatch(items);
      } catch {
        // Re-enqueue on failure so we retry next save cycle
        for (const file of chunk) this.queue.set(file.path, file);
        return;
      }
    }

    await this.onSyncAt(Date.now());
  }

  destroy(): void {
    window.clearTimeout(this.timer);
    this.queue.clear();
  }
}
