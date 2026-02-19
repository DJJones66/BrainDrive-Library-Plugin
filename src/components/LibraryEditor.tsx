import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { LibraryEditorService } from "../services/libraryEditorService";
import {
  LibraryEditorProps,
  LibraryTheme,
  LibraryTreeItem,
  ThemeService,
} from "../types";
import "../styles/LibraryEditor.css";

const MARKDOWN_EXTENSIONS = new Set([".md", ".markdown"]);
const SUPPORTED_FILE_EXTENSIONS = [
  ".md",
  ".markdown",
  ".txt",
  ".json",
  ".yaml",
  ".yml",
];

function normalizeTheme(value: string | undefined): LibraryTheme {
  return value === "dark" ? "dark" : "light";
}

function bytesToLabel(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const kb = bytes / 1024;
  if (kb < 1024) {
    return `${kb.toFixed(1)} KB`;
  }
  const mb = kb / 1024;
  return `${mb.toFixed(2)} MB`;
}

function relativeParent(path: string): string {
  const cleaned = (path || "").replace(/\\+/g, "/").replace(/^\/+|\/+$/g, "");
  if (!cleaned) {
    return "";
  }
  const parts = cleaned.split("/");
  parts.pop();
  return parts.join("/");
}

function displayTimestamp(value?: string): string {
  if (!value) {
    return "Unknown";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString();
}

interface LibraryEditorState {
  theme: LibraryTheme;
  currentPath: string;
  items: LibraryTreeItem[];
  treeLoading: boolean;
  treeError: string | null;
  search: string;

  selectedFilePath: string | null;
  selectedFileName: string;
  selectedExtension: string;
  selectedSize: number;
  selectedUpdatedAt: string;

  fileLoading: boolean;
  fileSaving: boolean;
  fileError: string | null;
  fileContent: string;
  originalContent: string;
  isEditing: boolean;
}

class LibraryEditor extends React.Component<LibraryEditorProps, LibraryEditorState> {
  private editorService: LibraryEditorService;
  private themeListener: ((theme: string) => void) | null;
  private subscribedThemeService: ThemeService | undefined;

  constructor(props: LibraryEditorProps) {
    super(props);
    this.editorService = new LibraryEditorService(props.services?.api);
    this.themeListener = null;
    this.subscribedThemeService = undefined;

    this.state = {
      theme: "light",
      currentPath: "",
      items: [],
      treeLoading: false,
      treeError: null,
      search: "",

      selectedFilePath: null,
      selectedFileName: "",
      selectedExtension: "",
      selectedSize: 0,
      selectedUpdatedAt: "",

      fileLoading: false,
      fileSaving: false,
      fileError: null,
      fileContent: "",
      originalContent: "",
      isEditing: false,
    };
  }

  componentDidMount(): void {
    this.attachThemeListener(this.props.services?.theme);
    this.loadTree("");
  }

  componentDidUpdate(prevProps: LibraryEditorProps): void {
    if (prevProps.services?.theme !== this.props.services?.theme) {
      this.detachThemeListener();
      this.attachThemeListener(this.props.services?.theme);
    }

    if (prevProps.services?.api !== this.props.services?.api) {
      this.editorService = new LibraryEditorService(this.props.services?.api);
      this.loadTree("");
    }
  }

  componentWillUnmount(): void {
    this.detachThemeListener();
  }

  private attachThemeListener(themeService: ThemeService | undefined): void {
    if (!themeService) {
      this.setState({ theme: "light" });
      return;
    }

    this.subscribedThemeService = themeService;

    try {
      this.setState({ theme: normalizeTheme(themeService.getCurrentTheme?.()) });
    } catch (_error) {
      this.setState({ theme: "light" });
    }

    this.themeListener = (nextTheme: string) => {
      this.setState({ theme: normalizeTheme(nextTheme) });
    };

    themeService.addThemeChangeListener?.(this.themeListener);
  }

  private detachThemeListener(): void {
    if (this.subscribedThemeService && this.themeListener) {
      this.subscribedThemeService.removeThemeChangeListener?.(this.themeListener);
    }
    this.themeListener = null;
    this.subscribedThemeService = undefined;
  }

  private getHasUnsavedChanges(): boolean {
    const { isEditing, fileContent, originalContent } = this.state;
    return isEditing && fileContent !== originalContent;
  }

  private formatUnsupportedFileMessage(pathOrName: string): string {
    const source = String(pathOrName || "").trim();
    const filename = source.split("/").pop() || source || "This file";
    const extensionIndex = filename.lastIndexOf(".");
    const extension =
      extensionIndex >= 0 ? filename.slice(extensionIndex).toLowerCase() : "";

    if (extension) {
      return `${filename} (${extension}) is not supported in Library Editor. Supported file types: ${SUPPORTED_FILE_EXTENSIONS.join(
        ", "
      )}.`;
    }

    return `${filename} is not supported in Library Editor. Supported file types: ${SUPPORTED_FILE_EXTENSIONS.join(
      ", "
    )}.`;
  }

  private async loadTree(path: string): Promise<void> {
    this.setState({ treeLoading: true, treeError: null });

    try {
      const response = await this.editorService.listTree(path);
      this.setState({
        currentPath: response.path || "",
        items: Array.isArray(response.items) ? response.items : [],
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to load folder.";
      this.setState({ treeError: message, items: [] });
    } finally {
      this.setState({ treeLoading: false });
    }
  }

  private async openFile(path: string): Promise<void> {
    if (this.getHasUnsavedChanges()) {
      const discard = window.confirm(
        "You have unsaved changes. Discard them and open another file?"
      );
      if (!discard) {
        return;
      }
    }

    this.setState({ fileLoading: true, fileError: null });

    try {
      const response = await this.editorService.readFile(path);
      this.setState({
        selectedFilePath: response.path,
        selectedFileName: response.name || response.path.split("/").pop() || "",
        selectedExtension: response.extension || "",
        selectedSize: Number(response.size || 0),
        selectedUpdatedAt: response.modified_at || "",
        fileContent: response.content || "",
        originalContent: response.content || "",
        isEditing: false,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to open file.";
      this.setState({ fileError: message });
    } finally {
      this.setState({ fileLoading: false });
    }
  }

  private async saveFile(): Promise<void> {
    const { selectedFilePath, fileContent, currentPath } = this.state;
    if (!selectedFilePath) {
      return;
    }

    this.setState({ fileSaving: true, fileError: null });

    try {
      const response = await this.editorService.saveFile(selectedFilePath, fileContent);
      this.setState({
        originalContent: fileContent,
        selectedSize: response.bytes || new TextEncoder().encode(fileContent).length,
        selectedUpdatedAt: response.updated_at || new Date().toISOString(),
        isEditing: false,
      });
      await this.loadTree(currentPath);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to save file.";
      this.setState({ fileError: message });
    } finally {
      this.setState({ fileSaving: false });
    }
  }

  private async refreshSelectedFile(): Promise<void> {
    const { selectedFilePath } = this.state;
    if (!selectedFilePath) {
      return;
    }
    await this.openFile(selectedFilePath);
  }

  private async handleTreeItemClick(item: LibraryTreeItem): Promise<void> {
    if (item.type === "directory") {
      await this.loadTree(item.path);
      return;
    }
    if (item.supported === false) {
      this.setState({
        fileError: this.formatUnsupportedFileMessage(item.path || item.name),
      });
      return;
    }
    await this.openFile(item.path);
  }

  private async goToParent(): Promise<void> {
    const { currentPath } = this.state;
    await this.loadTree(relativeParent(currentPath));
  }

  render(): React.ReactNode {
    const {
      theme,
      currentPath,
      items,
      treeLoading,
      treeError,
      search,
      selectedFilePath,
      selectedFileName,
      selectedExtension,
      selectedSize,
      selectedUpdatedAt,
      fileLoading,
      fileSaving,
      fileError,
      fileContent,
      originalContent,
      isEditing,
    } = this.state;

    const hasUnsavedChanges = isEditing && fileContent !== originalContent;
    const token = search.trim().toLowerCase();
    const visibleItems = token
      ? items.filter((item) => item.name.toLowerCase().includes(token))
      : items;
    const isMarkdown = MARKDOWN_EXTENSIONS.has((selectedExtension || "").toLowerCase());

    return (
      <div
        className={`library-editor-root ${theme === "dark" ? "dark-theme" : ""}`}
        data-theme={theme}
      >
        <section className="library-editor-shell">
          <aside className="library-tree-panel open">
            <div className="library-tree-toolbar">
              <button
                type="button"
                className="library-btn secondary"
                onClick={() => this.goToParent()}
                disabled={!currentPath || treeLoading}
              >
                Up
              </button>
              <button
                type="button"
                className="library-btn secondary"
                onClick={() => this.loadTree(currentPath)}
                disabled={treeLoading}
              >
                Refresh
              </button>
              <input
                className="library-search"
                value={search}
                onChange={(event: React.ChangeEvent<HTMLInputElement>) =>
                  this.setState({ search: event.target.value })
                }
                placeholder="Filter files"
                aria-label="Filter files"
              />
            </div>

            <div className="library-tree-content">
              {treeLoading ? (
                <p className="library-state-text">Loading folder...</p>
              ) : treeError ? (
                <p className="library-state-error">{treeError}</p>
              ) : visibleItems.length === 0 ? (
                <p className="library-state-text">No files or folders in this location.</p>
              ) : (
                <ul className="library-tree-list">
                  {visibleItems.map((item) => {
                    const isActive = selectedFilePath === item.path;
                    return (
                      <li key={item.path}>
                        <button
                          type="button"
                          className={`library-tree-item ${isActive ? "active" : ""}`}
                          onClick={() => this.handleTreeItemClick(item)}
                          title={item.path}
                        >
                          <span className="library-tree-icon" aria-hidden="true">
                            {item.type === "directory" ? "📁" : "📄"}
                          </span>
                          <span className="library-tree-label">{item.name}</span>
                          {item.type === "file" ? (
                            <span className="library-tree-size">{bytesToLabel(item.size)}</span>
                          ) : (
                            <span className="library-tree-size">Folder</span>
                          )}
                        </button>
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          </aside>

          <main className="library-main-panel">
            <div className="library-main-toolbar">
              <div>
                <h2 className="library-main-title">{selectedFileName || "Select a file"}</h2>
                <p className="library-main-meta">
                  {selectedFilePath
                    ? `${selectedFilePath} • ${bytesToLabel(selectedSize)} • Updated ${displayTimestamp(
                        selectedUpdatedAt
                      )}`
                    : "Markdown, JSON, YAML, and TXT files are supported for editing."}
                </p>
              </div>

              <div className="library-main-actions">
                {selectedFilePath ? (
                  <>
                    <button
                      type="button"
                      className="library-btn secondary"
                      onClick={() => this.refreshSelectedFile()}
                      disabled={fileLoading || fileSaving}
                    >
                      Reload
                    </button>

                    {!isEditing ? (
                      <button
                        type="button"
                        className="library-btn primary"
                        onClick={() => this.setState({ isEditing: true })}
                        disabled={fileLoading || fileSaving}
                      >
                        Edit
                      </button>
                    ) : (
                      <>
                        <button
                          type="button"
                          className="library-btn secondary"
                          onClick={() => {
                            this.setState({
                              isEditing: false,
                              fileContent: originalContent,
                            });
                          }}
                          disabled={fileSaving}
                        >
                          Cancel
                        </button>
                        <button
                          type="button"
                          className="library-btn primary"
                          onClick={() => this.saveFile()}
                          disabled={fileSaving || !hasUnsavedChanges}
                        >
                          {fileSaving ? "Saving..." : "Save"}
                        </button>
                      </>
                    )}
                  </>
                ) : null}
              </div>
            </div>

            <div className="library-main-content">
              {fileError ? <p className="library-state-error">{fileError}</p> : null}

              {!selectedFilePath ? (
                <div className="library-empty-state">
                  <p>Select a file in the navigator to preview and edit its content.</p>
                </div>
              ) : fileLoading ? (
                <div className="library-empty-state">
                  <p>Loading file...</p>
                </div>
              ) : isEditing ? (
                <textarea
                  className="library-editor-textarea"
                  value={fileContent}
                  onChange={(event: React.ChangeEvent<HTMLTextAreaElement>) =>
                    this.setState({ fileContent: event.target.value })
                  }
                  spellCheck={false}
                  aria-label="File content editor"
                />
              ) : isMarkdown ? (
                <article className="library-markdown-preview">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{fileContent}</ReactMarkdown>
                </article>
              ) : (
                <pre className="library-code-preview">{fileContent}</pre>
              )}
            </div>
          </main>
        </section>
      </div>
    );
  }
}

export default LibraryEditor;
