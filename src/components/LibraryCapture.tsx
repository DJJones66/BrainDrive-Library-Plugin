import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { LibraryCaptureService } from "../services/libraryCaptureService";
import { LibraryPageDefaultsService } from "../services/libraryPageDefaultsService";
import {
  CaptureMessage,
  LibraryCaptureProps,
  LibraryTheme,
  MCPApprovalDecision,
  MCPApprovalRequest,
  MCPMetadataEvent,
  MCPProjectSource,
  MCPRequestParams,
  LibraryPageContext,
  ModelInfo,
  ScopeOption,
  ThemeService,
} from "../types";
import SearchableDropdown, { DropdownOption } from "./SearchableDropdown";
import "../styles/LibraryCapture.css";

function generateId(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeTheme(value: string | undefined): LibraryTheme {
  return value === "dark" ? "dark" : "light";
}

function normalizePath(value: string): string {
  return String(value || "")
    .trim()
    .replace(/\\+/g, "/")
    .replace(/^\/+/, "")
    .replace(/\/+$/, "");
}

function sortScopes(values: ScopeOption[]): ScopeOption[] {
  return [...values].sort((left, right) => {
    if (left.scope_root !== right.scope_root) {
      return left.scope_root === "life" ? -1 : 1;
    }
    return left.name.localeCompare(right.name);
  });
}

interface LibraryCaptureState {
  theme: LibraryTheme;
  modelsLoading: boolean;
  scopesLoading: boolean;
  models: ModelInfo[];
  selectedModel: ModelInfo | null;
  savedDefaultModelKey: string | null;
  isSavingDefaultModel: boolean;
  defaultModelStatus: string | null;
  defaultModelStatusTone: "success" | "error" | null;

  scopeOptions: ScopeOption[];
  scopeEnabled: boolean;
  selectedScopePath: string | null;

  conversationId: string | null;
  messages: CaptureMessage[];
  inputText: string;
  isSubmitting: boolean;
  isProcessingTranscript: boolean;
  error: string | null;
  transcriptError: string | null;
  pendingApproval: MCPApprovalRequest | null;
  activityStatus: string | null;
}

class LibraryCapture extends React.Component<LibraryCaptureProps, LibraryCaptureState> {
  private captureService: LibraryCaptureService;
  private pageDefaultsService: LibraryPageDefaultsService;
  private themeListener: ((theme: string) => void) | null;
  private subscribedThemeService: ThemeService | undefined;
  private activeAbortController: AbortController | null;
  private transcriptInputRef: React.RefObject<HTMLInputElement>;
  private messagesContainerRef: React.RefObject<HTMLDivElement>;
  private defaultModelStatusTimeout: ReturnType<typeof setTimeout> | null;

  constructor(props: LibraryCaptureProps) {
    super(props);

    this.captureService = new LibraryCaptureService(props.services?.api);
    this.pageDefaultsService = new LibraryPageDefaultsService(props.services?.api);
    this.themeListener = null;
    this.subscribedThemeService = undefined;
    this.activeAbortController = null;
    this.transcriptInputRef = React.createRef<HTMLInputElement>();
    this.messagesContainerRef = React.createRef<HTMLDivElement>();
    this.defaultModelStatusTimeout = null;

    this.state = {
      theme: "light",
      modelsLoading: true,
      scopesLoading: true,
      models: [],
      selectedModel: null,
      savedDefaultModelKey: this.getConfiguredDefaultModelKey(),
      isSavingDefaultModel: false,
      defaultModelStatus: null,
      defaultModelStatusTone: null,
      scopeOptions: [],
      scopeEnabled: false,
      selectedScopePath: null,
      conversationId: null,
      messages: [
        {
          id: generateId("greeting"),
          sender: "ai",
          content:
            this.getConfiguredStringValue(
              this.props.initialGreeting,
              this.props.initial_greeting
            ) ||
            "Capture is ready. Add a note, decision, task, completion update, or upload a transcript.",
          timestamp: new Date().toISOString(),
        },
      ],
      inputText: "",
      isSubmitting: false,
      isProcessingTranscript: false,
      error: null,
      transcriptError: null,
      pendingApproval: null,
      activityStatus: null,
    };
  }

  componentDidMount(): void {
    this.attachThemeListener(this.props.services?.theme);
    void this.initialize();
    this.scrollMessagesToBottom();
  }

  componentDidUpdate(prevProps: LibraryCaptureProps, prevState: LibraryCaptureState): void {
    if (prevProps.services?.theme !== this.props.services?.theme) {
      this.detachThemeListener();
      this.attachThemeListener(this.props.services?.theme);
    }

    if (prevProps.services?.api !== this.props.services?.api) {
      this.captureService = new LibraryCaptureService(this.props.services?.api);
      this.pageDefaultsService = new LibraryPageDefaultsService(this.props.services?.api);
      void this.initialize();
    }

    if (
      prevProps.defaultModelKey !== this.props.defaultModelKey ||
      prevProps.default_model_key !== this.props.default_model_key ||
      prevProps.defaultModelProvider !== this.props.defaultModelProvider ||
      prevProps.default_model_provider !== this.props.default_model_provider ||
      prevProps.defaultModelServerId !== this.props.defaultModelServerId ||
      prevProps.default_model_server_id !== this.props.default_model_server_id ||
      prevProps.defaultModelName !== this.props.defaultModelName ||
      prevProps.default_model_name !== this.props.default_model_name
    ) {
      const configuredKey = this.getConfiguredDefaultModelKey();
      if (configuredKey !== this.state.savedDefaultModelKey) {
        this.setState({ savedDefaultModelKey: configuredKey }, () => {
          if (this.state.models.length > 0) {
            this.applyConfiguredModelDefault();
          }
        });
      } else if (this.state.models.length > 0) {
        this.applyConfiguredModelDefault();
      }
    }

    if (prevState.defaultModelStatus !== this.state.defaultModelStatus) {
      this.resetDefaultModelStatusTimer();
    }

    if (prevState.messages !== this.state.messages) {
      this.scrollMessagesToBottom();
    }
  }

  componentWillUnmount(): void {
    this.detachThemeListener();
    if (this.defaultModelStatusTimeout) {
      clearTimeout(this.defaultModelStatusTimeout);
      this.defaultModelStatusTimeout = null;
    }
    if (this.activeAbortController) {
      this.activeAbortController.abort();
      this.activeAbortController = null;
    }
  }

  private scrollMessagesToBottom(): void {
    const container = this.messagesContainerRef.current;
    if (!container) {
      return;
    }

    requestAnimationFrame(() => {
      const nextContainer = this.messagesContainerRef.current;
      if (!nextContainer) {
        return;
      }
      nextContainer.scrollTop = nextContainer.scrollHeight;
    });
  }

  private resetDefaultModelStatusTimer(): void {
    if (this.defaultModelStatusTimeout) {
      clearTimeout(this.defaultModelStatusTimeout);
      this.defaultModelStatusTimeout = null;
    }

    if (!this.state.defaultModelStatus) {
      return;
    }

    this.defaultModelStatusTimeout = setTimeout(() => {
      this.defaultModelStatusTimeout = null;
      this.setState((previousState) => {
        if (!previousState.defaultModelStatus) {
          return null;
        }

        return {
          defaultModelStatus: null,
          defaultModelStatusTone: null,
        };
      });
    }, 5000);
  }

  private async initialize(): Promise<void> {
    this.setState({
      modelsLoading: true,
      scopesLoading: true,
      error: null,
      transcriptError: null,
    });

    try {
      const [models, projectScopes, lifeScopes] = await Promise.all([
        this.captureService.loadModels(),
        this.captureService.loadProjectScopes(),
        this.captureService.loadLifeScopes(),
      ]);

      const mergedScopes = sortScopes([...lifeScopes, ...projectScopes]);

      this.setState(
        {
          models,
          scopeOptions: mergedScopes,
          modelsLoading: false,
          scopesLoading: false,
        },
        () => {
          this.applyConfiguredModelDefault();
          this.applyConfiguredScopeDefault();
        }
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load Capture dependencies.";
      this.setState({
        modelsLoading: false,
        scopesLoading: false,
        error: message,
      });
    }
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

  private getConfiguredStringValue(
    primaryValue: string | null | undefined,
    fallbackValue: string | null | undefined
  ): string {
    const normalizedPrimary = typeof primaryValue === "string" ? primaryValue.trim() : "";
    if (normalizedPrimary) {
      return normalizedPrimary;
    }

    const normalizedFallback = typeof fallbackValue === "string" ? fallbackValue.trim() : "";
    return normalizedFallback;
  }

  private getConfiguredBooleanValue(
    primaryValue: boolean | null | undefined,
    fallbackValue: boolean | null | undefined
  ): boolean | undefined {
    if (typeof primaryValue === "boolean") {
      return primaryValue;
    }

    if (typeof fallbackValue === "boolean") {
      return fallbackValue;
    }

    return undefined;
  }

  private getEffectiveConversationType(): string {
    return (
      this.getConfiguredStringValue(
        this.props.conversationType,
        this.props.conversation_type
      ) || "capture"
    );
  }

  private shouldShowModelSelection(): boolean {
    return this.getConfiguredBooleanValue(
      this.props.showModelSelection,
      this.props.show_model_selection
    ) !== false;
  }

  private isModelSelectionLocked(): boolean {
    return this.getConfiguredBooleanValue(
      this.props.lockModelSelection,
      this.props.lock_model_selection
    ) === true;
  }

  private isScopeSelectionLocked(): boolean {
    return this.getConfiguredBooleanValue(
      this.props.lockProjectScope,
      this.props.lock_project_scope
    ) === true;
  }

  private shouldShowTranscriptUpload(): boolean {
    return this.getConfiguredBooleanValue(
      this.props.showTranscriptUpload,
      this.props.show_transcript_upload
    ) !== false;
  }

  private getDefaultTranscriptSource(): string {
    return (
      this.getConfiguredStringValue(
        this.props.defaultTranscriptSource,
        this.props.default_transcript_source
      ) || "capture-upload"
    );
  }

  private getCurrentPageContext(): LibraryPageContext | null {
    try {
      const context = this.props.services?.pageContext?.getCurrentPageContext?.();
      if (context && typeof context === "object") {
        return context as LibraryPageContext;
      }
    } catch (_error) {
      // best effort lookup only
    }
    return null;
  }

  private parseConfiguredDefaultModelKey(): {
    provider: string;
    serverId: string | null;
    modelName: string;
  } | null {
    const rawModelKey = this.getConfiguredStringValue(
      this.props.defaultModelKey,
      this.props.default_model_key
    );

    if (rawModelKey) {
      const firstDelimiter = rawModelKey.indexOf("::");
      const secondDelimiter = rawModelKey.indexOf("::", firstDelimiter + 2);
      if (firstDelimiter > 0 && secondDelimiter > firstDelimiter + 2) {
        const provider = rawModelKey.slice(0, firstDelimiter).trim();
        const serverId = rawModelKey.slice(firstDelimiter + 2, secondDelimiter).trim();
        const modelName = rawModelKey.slice(secondDelimiter + 2).trim();

        if (provider && serverId && modelName) {
          return { provider, serverId, modelName };
        }
      }
    }

    const provider = this.getConfiguredStringValue(
      this.props.defaultModelProvider,
      this.props.default_model_provider
    );
    const modelName = this.getConfiguredStringValue(
      this.props.defaultModelName,
      this.props.default_model_name
    );
    const serverId = this.getConfiguredStringValue(
      this.props.defaultModelServerId,
      this.props.default_model_server_id
    );

    if (!provider || !modelName) {
      return null;
    }

    return {
      provider,
      serverId: serverId || null,
      modelName,
    };
  }

  private getConfiguredDefaultModelKey(): string | null {
    const rawKey = this.getConfiguredStringValue(
      this.props.defaultModelKey,
      this.props.default_model_key
    );
    if (rawKey) {
      return rawKey;
    }

    const parsed = this.parseConfiguredDefaultModelKey();
    if (!parsed || !parsed.serverId) {
      return null;
    }

    return `${parsed.provider}::${parsed.serverId}::${parsed.modelName}`;
  }

  private getModelOptionValue(model: ModelInfo): string {
    return `${model.provider}::${model.serverId}::${model.name}`;
  }

  private isModelConfiguredAsPageDefault(model: ModelInfo | null): boolean {
    if (!model) {
      return false;
    }

    const modelKey = this.getModelOptionValue(model).trim().toLowerCase();
    if (!modelKey) {
      return false;
    }

    const savedKey = String(this.state.savedDefaultModelKey || "").trim().toLowerCase();
    if (savedKey && savedKey === modelKey) {
      return true;
    }

    const configured = this.parseConfiguredDefaultModelKey();
    if (!configured) {
      return false;
    }

    const normalize = (value: string) => value.trim().toLowerCase();
    if (
      normalize(model.provider || "") !== normalize(configured.provider) ||
      normalize(model.name || "") !== normalize(configured.modelName)
    ) {
      return false;
    }

    if (configured.serverId) {
      return normalize(model.serverId || "") === normalize(configured.serverId);
    }

    return true;
  }

  private canSaveDefaultModelForPage(): boolean {
    const pageContext = this.getCurrentPageContext();
    return Boolean(pageContext?.pageId && this.state.selectedModel);
  }

  private saveSelectedModelAsPageDefault = async (): Promise<void> => {
    if (this.state.isSavingDefaultModel) {
      return;
    }

    const selectedModel = this.state.selectedModel;
    if (!selectedModel) {
      this.setState({
        defaultModelStatus: "Select a model first.",
        defaultModelStatusTone: "error",
      });
      return;
    }

    const pageContext = this.getCurrentPageContext();
    const pageId = typeof pageContext?.pageId === "string" ? pageContext.pageId : "";
    if (!pageId) {
      this.setState({
        defaultModelStatus: "Page context is unavailable; cannot save page default model.",
        defaultModelStatusTone: "error",
      });
      return;
    }

    this.setState({
      isSavingDefaultModel: true,
      defaultModelStatus: null,
      defaultModelStatusTone: null,
    });

    try {
      const result = await this.pageDefaultsService.saveCaptureDefaultModelForPage({
        pageId,
        moduleId: this.props.moduleId || null,
        selectedModel,
      });

      this.setState({
        isSavingDefaultModel: false,
        savedDefaultModelKey: result.default_model_key,
        defaultModelStatus: `Saved ${selectedModel.name} as the default model for this page.`,
        defaultModelStatusTone: "success",
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to save default model for page.";
      this.setState({
        isSavingDefaultModel: false,
        defaultModelStatus: message,
        defaultModelStatusTone: "error",
      });
    }
  };

  private applyConfiguredModelDefault(): void {
    const { models } = this.state;
    if (!Array.isArray(models) || models.length === 0) {
      this.setState({ selectedModel: null });
      return;
    }

    const parsedModelKey = this.parseConfiguredDefaultModelKey();
    if (!parsedModelKey) {
      this.setState((prevState) => ({
        selectedModel: prevState.selectedModel || models[0],
      }));
      return;
    }

    const normalize = (value: string) => value.trim().toLowerCase();
    const matched = models.find((model) => {
      if (
        normalize(model.provider || "") !== normalize(parsedModelKey.provider) ||
        normalize(model.name || "") !== normalize(parsedModelKey.modelName)
      ) {
        return false;
      }

      if (parsedModelKey.serverId) {
        return normalize(model.serverId || "") === normalize(parsedModelKey.serverId);
      }

      return true;
    });

    this.setState({ selectedModel: matched || models[0] });
  }

  private getNormalizedDefaultScopePath(): string | null {
    const configuredPath = this.getConfiguredStringValue(
      this.props.defaultScopePath || null,
      this.props.default_scope_path || null
    );

    const normalized = normalizePath(configuredPath || "");
    if (!normalized) {
      return null;
    }

    if (normalized === "capture") {
      return "capture";
    }

    if (normalized.startsWith("life/") || normalized.startsWith("projects/")) {
      return normalized;
    }

    return `projects/active/${normalized}`;
  }

  private getNormalizedDefaultScopeRoot(): "life" | "projects" | null {
    const raw = this.getConfiguredStringValue(
      this.props.defaultScopeRoot || null,
      this.props.default_scope_root || null
    ).toLowerCase();

    if (raw === "life") {
      return "life";
    }

    if (raw === "projects") {
      return "projects";
    }

    return null;
  }

  private getNormalizedDefaultProjectSlug(): string | null {
    const raw = this.getConfiguredStringValue(
      this.props.defaultProjectSlug,
      this.props.default_project_slug
    );

    const normalized = normalizePath(raw);
    if (!normalized) {
      return null;
    }

    const parts = normalized.split("/").filter(Boolean);
    return parts[parts.length - 1] || null;
  }

  private applyConfiguredScopeDefault(): void {
    const shouldEnable =
      this.getConfiguredBooleanValue(
        this.props.defaultLibraryScopeEnabled,
        this.props.default_library_scope_enabled
      ) === true;

    if (!shouldEnable) {
      this.setState({
        scopeEnabled: false,
        selectedScopePath: null,
      });
      return;
    }

    const options = this.state.scopeOptions;
    if (options.length === 0) {
      this.setState({
        scopeEnabled: false,
        selectedScopePath: null,
      });
      return;
    }

    const configuredPath = this.getNormalizedDefaultScopePath();
    const configuredRoot = this.getNormalizedDefaultScopeRoot();
    const configuredSlug = this.getNormalizedDefaultProjectSlug();

    let matched: ScopeOption | null = null;

    if (configuredPath) {
      matched =
        options.find((entry) => normalizePath(entry.path).toLowerCase() === configuredPath.toLowerCase()) ||
        null;
    }

    if (!matched && configuredSlug) {
      matched =
        options.find((entry) => {
          if (configuredRoot && entry.scope_root !== configuredRoot) {
            return false;
          }
          return String(entry.slug || "").trim().toLowerCase() === configuredSlug.toLowerCase();
        }) || null;
    }

    this.setState({
      scopeEnabled: Boolean(matched),
      selectedScopePath: matched ? normalizePath(matched.path) : null,
    });
  }

  private getSelectedScopeOption(): ScopeOption | null {
    if (!this.state.selectedScopePath) {
      return null;
    }

    const needle = this.state.selectedScopePath.toLowerCase();
    return (
      this.state.scopeOptions.find(
        (entry) => normalizePath(entry.path).toLowerCase() === needle
      ) || null
    );
  }

  private buildMcpParams(approvalDecision?: MCPApprovalDecision): MCPRequestParams {
    const selectedScope = this.state.scopeEnabled ? this.getSelectedScopeOption() : null;
    const selectedScopePath = selectedScope ? normalizePath(selectedScope.path) : null;
    const mcpProjectSource: MCPProjectSource = "ui";

    const params: MCPRequestParams = {
      mcp_tools_enabled: true,
      mcp_scope_mode: selectedScopePath ? "project" : "none",
      mcp_project_slug: selectedScopePath || undefined,
      mcp_project_name: selectedScope?.name,
      mcp_project_lifecycle: selectedScope?.lifecycle || "active",
      mcp_project_source: mcpProjectSource,
      mcp_plugin_slug: "BrainDriveLibraryPlugin",
    };

    if (approvalDecision) {
      params.mcp_approval = approvalDecision;
    }

    return params;
  }

  private appendMessage(message: CaptureMessage): void {
    this.setState((prevState) => ({
      messages: [...prevState.messages, message],
    }));
  }

  private updateMessage(messageId: string, updater: (message: CaptureMessage) => CaptureMessage): void {
    this.setState((prevState) => ({
      messages: prevState.messages.map((message) =>
        message.id === messageId ? updater(message) : message
      ),
    }));
  }

  private handleMetadataEvent = (event: MCPMetadataEvent): void => {
    if (!event || typeof event !== "object" || typeof event.type !== "string") {
      return;
    }

    if (event.type === "tooling_state") {
      const stopReason = String(event.tool_loop_stop_reason || "").trim().toLowerCase();
      if (stopReason === "approval_required") {
        this.setState({ activityStatus: "Awaiting approval for a write action..." });
        return;
      }
      if (stopReason === "provider_timeout") {
        this.setState({ activityStatus: "Model response timed out. Retry when ready." });
        return;
      }
      if (stopReason === "missing_orchestration_context") {
        this.setState({ activityStatus: "Missing required scope context for this write." });
        return;
      }
      if (event.tool_loop_enabled === true) {
        this.setState({ activityStatus: "Planning tool actions..." });
      }
      return;
    }

    if (event.type === "tool_call") {
      const toolName = String(event.name || "tool").trim() || "tool";
      this.setState({ activityStatus: `Running tool: ${toolName}...` });
      return;
    }

    if (event.type === "tool_result") {
      const toolName = String(event.name || "tool").trim() || "tool";
      const success = event.ok !== false;
      this.setState({
        activityStatus: success
          ? `Tool completed: ${toolName}.`
          : `Tool failed: ${toolName}.`,
      });
      return;
    }

    if (event.type === "auto_continue") {
      const passLabel = String(event.pass || "next");
      this.setState({ activityStatus: `Continuing response (pass ${passLabel})...` });
      return;
    }

    if (event.type === "orchestration_context_error") {
      this.setState({ activityStatus: "Write is blocked until scope context is ready." });
      return;
    }

    if (event.type === "approval_request" || event.type === "approval_required") {
      const rawApproval =
        event.type === "approval_required"
          ? (event.approval_request as Record<string, any>)
          : (event as Record<string, any>);

      const normalizedApproval: MCPApprovalRequest = {
        type: "approval_request",
        request_id: typeof rawApproval.request_id === "string" ? rawApproval.request_id : undefined,
        tool: String(rawApproval.tool || "").trim(),
        summary:
          typeof rawApproval.summary === "string" && rawApproval.summary.trim()
            ? rawApproval.summary.trim()
            : "Approval required before executing mutating tool call.",
        arguments:
          rawApproval.arguments && typeof rawApproval.arguments === "object"
            ? (rawApproval.arguments as Record<string, any>)
            : undefined,
        status: "pending",
      };

      if (!normalizedApproval.tool) {
        normalizedApproval.tool = "mutating_tool";
      }

      this.setState({
        pendingApproval: normalizedApproval,
        activityStatus: `Awaiting approval for ${normalizedApproval.tool}...`,
      });
      this.appendMessage({
        id: generateId("approval-required"),
        sender: "system",
        content: `Approval required for tool: ${normalizedApproval.tool}`,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (event.type === "approval_resolution") {
      const status = String(event.status || "").toLowerCase();
      const tool = String(event.tool || "").trim() || "mutating_tool";
      const content =
        status === "approved"
          ? `Approved: ${tool}`
          : status === "rejected"
          ? `Denied: ${tool}`
          : `Approval resolved for: ${tool}`;

      this.appendMessage({
        id: generateId("approval-resolution"),
        sender: "system",
        content,
        timestamp: new Date().toISOString(),
      });

      this.setState((prevState) => {
        if (!prevState.pendingApproval) {
          return {
            pendingApproval: null,
            activityStatus:
              status === "approved"
                ? `Approved ${tool}. Continuing...`
                : status === "rejected"
                ? `Denied ${tool}.`
                : `Approval resolved for ${tool}.`,
          };
        }

        const pendingId = prevState.pendingApproval.request_id || "";
        const resolvedId = typeof event.request_id === "string" ? event.request_id : "";

        if (!pendingId || !resolvedId || pendingId === resolvedId) {
          return {
            pendingApproval: null,
            activityStatus:
              status === "approved"
                ? `Approved ${tool}. Continuing...`
                : status === "rejected"
                ? `Denied ${tool}.`
                : `Approval resolved for ${tool}.`,
          };
        }

        return null;
      });
      return;
    }

    if (event.type === "project_scope_selected") {
      const selectedPath =
        typeof event.scope_path === "string" ? normalizePath(event.scope_path) : "";
      if (!selectedPath) {
        return;
      }

      const matched = this.state.scopeOptions.find(
        (entry) => normalizePath(entry.path).toLowerCase() === selectedPath.toLowerCase()
      );

      if (matched) {
        this.setState({
          scopeEnabled: true,
          selectedScopePath: selectedPath,
          activityStatus: `Using scope: ${matched.name}.`,
        });
      }
    }
  };

  private async sendPrompt(
    prompt: string,
    options?: {
      approvalDecision?: MCPApprovalDecision;
      userLabel?: string;
    }
  ): Promise<void> {
    const selectedModel = this.state.selectedModel;
    if (!selectedModel) {
      this.setState({ error: "Select a model before sending capture input." });
      return;
    }

    const promptToSend = String(prompt || "").trim();
    if (!promptToSend) {
      return;
    }

    const userContent =
      options?.userLabel && options.userLabel.trim()
        ? options.userLabel.trim()
        : promptToSend;

    this.appendMessage({
      id: generateId("user"),
      sender: "user",
      content: userContent,
      timestamp: new Date().toISOString(),
    });

    const assistantMessageId = generateId("assistant");
    this.appendMessage({
      id: assistantMessageId,
      sender: "ai",
      content: "",
      timestamp: new Date().toISOString(),
      isStreaming: true,
    });

    if (this.activeAbortController) {
      this.activeAbortController.abort();
    }

    this.activeAbortController = new AbortController();

    this.setState({ isSubmitting: true, error: null, activityStatus: "Thinking..." });

    try {
      let hasReceivedChunk = false;
      await this.captureService.sendPrompt({
        prompt: promptToSend,
        selectedModel,
        conversationId: this.state.conversationId,
        conversationType: this.getEffectiveConversationType(),
        pageContext: this.getCurrentPageContext(),
        useStreaming: this.getConfiguredBooleanValue(
          this.props.enableStreaming,
          this.props.enable_streaming
        ) !== false,
        mcpParams: this.buildMcpParams(options?.approvalDecision),
        approvalDecision: options?.approvalDecision,
        onChunk: (chunk: string) => {
          if (!hasReceivedChunk && String(chunk || "").trim()) {
            hasReceivedChunk = true;
            this.setState((prevState) => ({
              activityStatus: prevState.pendingApproval
                ? prevState.activityStatus
                : "Streaming response...",
            }));
          }
          this.updateMessage(assistantMessageId, (message) => ({
            ...message,
            content: `${message.content}${chunk}`,
          }));
        },
        onConversationId: (conversationId: string) => {
          this.setState({ conversationId });
        },
        onMetadataEvent: this.handleMetadataEvent,
        abortController: this.activeAbortController,
      });

      this.updateMessage(assistantMessageId, (message) => {
        const trimmed = message.content.trim();
        return {
          ...message,
          content: trimmed || "(No textual response)",
          isStreaming: false,
        };
      });
      this.setState((prevState) => ({
        activityStatus: prevState.pendingApproval
          ? `Awaiting approval for ${prevState.pendingApproval.tool}...`
          : "Response complete.",
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "Capture request failed.";
      this.updateMessage(assistantMessageId, (existing) => ({
        ...existing,
        content: `Error: ${message}`,
        isStreaming: false,
        isError: true,
      }));
      this.setState({ error: message, activityStatus: `Error: ${message}` });
    } finally {
      this.setState({ isSubmitting: false, inputText: "" });
      this.activeAbortController = null;
    }
  }

  private handleSend = async (): Promise<void> => {
    if (this.state.isSubmitting || this.state.isProcessingTranscript) {
      return;
    }

    const prompt = this.state.inputText.trim();
    if (!prompt) {
      return;
    }

    await this.sendPrompt(prompt);
  };

  private handleInputKeyDown = async (
    event: React.KeyboardEvent<HTMLTextAreaElement>
  ): Promise<void> => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      await this.handleSend();
    }
  };

  private handleApprovalDecision = async (action: "approve" | "reject"): Promise<void> => {
    if (this.state.isSubmitting) {
      return;
    }

    const pendingApproval = this.state.pendingApproval;
    if (!pendingApproval) {
      return;
    }

    const approvalDecision: MCPApprovalDecision = {
      action,
      request_id: pendingApproval.request_id,
      tool: pendingApproval.tool,
      arguments: pendingApproval.arguments,
    };

    await this.sendPrompt(action === "approve" ? "Approve." : "Deny.", {
      approvalDecision,
      userLabel: action === "approve" ? "Approve" : "Deny",
    });
  };

  private openTranscriptUpload = (): void => {
    if (this.state.isSubmitting || this.state.isProcessingTranscript) {
      return;
    }

    this.transcriptInputRef.current?.click();
  };

  private handleTranscriptSelected = async (
    event: React.ChangeEvent<HTMLInputElement>
  ): Promise<void> => {
    const file = event.target.files && event.target.files[0];
    if (!file) {
      return;
    }

    this.setState({ isProcessingTranscript: true, transcriptError: null });

    try {
      const processed = await this.captureService.processDocument(file);
      const scopePath = this.state.scopeEnabled ? this.state.selectedScopePath : null;
      const dateIso = new Date().toISOString().slice(0, 10);
      const source = this.getDefaultTranscriptSource();

      const text = String(processed.extracted_text || "").trim();
      if (!text) {
        throw new Error("No transcript text was extracted from the uploaded file.");
      }

      const truncatedText = text.length > 12000 ? `${text.slice(0, 12000)}\n\n[TRUNCATED]` : text;

      const prompt = [
        "Capture transcript ingestion request.",
        "Use ingest_transcript to save this transcript in transcripts/YYYY-MM and update transcripts/index.md.",
        "Require explicit approval before executing the write.",
        `filename: ${file.name}`,
        `date: ${dateIso}`,
        `source: ${source}`,
        scopePath ? `project: ${scopePath}` : "project: capture",
        "",
        "Transcript content:",
        truncatedText,
      ].join("\n");

      await this.sendPrompt(prompt, {
        userLabel: `Uploaded transcript: ${file.name}`,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Transcript processing failed.";
      this.setState({ transcriptError: message });
      this.appendMessage({
        id: generateId("transcript-error"),
        sender: "system",
        content: `Transcript upload failed: ${message}`,
        timestamp: new Date().toISOString(),
        isError: true,
      });
    } finally {
      this.setState({ isProcessingTranscript: false });
      if (this.transcriptInputRef.current) {
        this.transcriptInputRef.current.value = "";
      }
    }
  };

  private renderScopeLabel(scope: ScopeOption): string {
    if (scope.scope_root === "life") {
      return `Life / ${scope.name}`;
    }
    return `Projects / ${scope.name}`;
  }


  private buildModelOptions(models: ModelInfo[]): DropdownOption[] {
    return models.map((model) => ({
      value: this.getModelOptionValue(model),
      label: `${model.name} (${model.provider}:${model.serverName})`,
      keywords: [
        model.name,
        model.provider,
        model.serverName,
        model.providerId,
        model.serverId,
      ],
    }));
  }

  private handleModelSelection = (value: string): void => {
    const matched = this.state.models.find(
      (model) => this.getModelOptionValue(model) === value
    );
    this.setState({
      selectedModel: matched || null,
      defaultModelStatus: null,
      defaultModelStatusTone: null,
    });
  };

  render(): React.ReactNode {
    const {
      theme,
      modelsLoading,
      scopesLoading,
      models,
      selectedModel,
      isSavingDefaultModel,
      defaultModelStatus,
      defaultModelStatusTone,
      scopeOptions,
      scopeEnabled,
      selectedScopePath,
      messages,
      inputText,
      isSubmitting,
      isProcessingTranscript,
      error,
      transcriptError,
      pendingApproval,
      activityStatus,
    } = this.state;

    const placeholder =
      this.getConfiguredStringValue(
        this.props.inputPlaceholder,
        this.props.input_placeholder
      ) ||
      "Capture a note, decision, task, completion, or upload a transcript...";

    const submitLabel =
      this.getConfiguredStringValue(
        this.props.submitLabel,
        this.props.submit_label
      ) || "Capture";

    const modelOptions = this.buildModelOptions(models);
    const selectedModelValue = selectedModel
      ? this.getModelOptionValue(selectedModel)
      : "";
    const canSaveDefaultModelForPage = this.canSaveDefaultModelForPage();
    const selectedModelIsDefault = this.isModelConfiguredAsPageDefault(selectedModel);

    return (
      <div className={`library-capture-root ${theme === "dark" ? "dark-theme" : ""}`}>
        {defaultModelStatus && (
          <div
            className={
              "library-capture-toast " +
              (defaultModelStatusTone === "error" ? "is-error" : "is-success")
            }
            role={defaultModelStatusTone === "error" ? "alert" : "status"}
            aria-live="polite"
          >
            {defaultModelStatus}
          </div>
        )}
        <div className="library-capture-header">
          <div className="library-capture-title-group">
            <h2 className="library-capture-title">Library Capture</h2>
            <p className="library-capture-subtitle">
              Fast text capture with approval-gated writes.
            </p>
            {activityStatus && (
              <p
                className={`library-capture-activity ${
                  isSubmitting ? "is-active" : ""
                }`}
                role="status"
                aria-live="polite"
              >
                {activityStatus}
              </p>
            )}
          </div>

          <div className="library-capture-controls">
            {this.shouldShowModelSelection() && (
              <label className="library-capture-control">
                <span className="library-capture-control-label">Model</span>
                <div className="library-capture-model-row">
                  <button
                    type="button"
                    className="library-capture-secondary library-capture-default-model"
                    title="Set your default model"
                    onClick={() => {
                      void this.saveSelectedModelAsPageDefault();
                    }}
                    disabled={
                      modelsLoading ||
                      isSubmitting ||
                      isSavingDefaultModel ||
                      this.isModelSelectionLocked() ||
                      !canSaveDefaultModelForPage ||
                      !selectedModel ||
                      selectedModelIsDefault
                    }
                  >
                    {isSavingDefaultModel ? "Saving..." : "Set Default"}
                  </button>
                  <div className="library-capture-model-dropdown">
                    <SearchableDropdown
                      id="library-capture-model-selection"
                      value={selectedModelValue}
                      options={modelOptions}
                      onSelect={this.handleModelSelection}
                      placeholder={models.length === 0 ? "No models available" : "Select model"}
                      searchPlaceholder="Search models"
                      noResultsText="No models found"
                      disabled={
                        modelsLoading ||
                        isSubmitting ||
                        this.isModelSelectionLocked() ||
                        models.length === 0
                      }
                      loading={modelsLoading}
                      triggerClassName="library-capture-select-trigger"
                      menuClassName="library-capture-select-menu"
                      inputClassName="library-capture-select-input"
                    />
                  </div>
                </div>

              </label>
            )}

            <label className="library-capture-control">
              <span className="library-capture-control-label">Scope</span>
              <select
                value={scopeEnabled ? selectedScopePath || "__all__" : "__all__"}
                onChange={(event) => {
                  const value = event.target.value;
                  if (value === "__all__") {
                    this.setState({ scopeEnabled: false, selectedScopePath: null });
                  } else {
                    this.setState({
                      scopeEnabled: true,
                      selectedScopePath: value,
                    });
                  }
                }}
                disabled={scopesLoading || isSubmitting || this.isScopeSelectionLocked()}
              >
                <option value="__all__">All Library</option>
                {scopeOptions.map((scope) => (
                  <option key={scope.path} value={scope.path}>
                    {this.renderScopeLabel(scope)}
                  </option>
                ))}
              </select>
            </label>

            {this.shouldShowTranscriptUpload() && (
              <button
                type="button"
                className="library-capture-secondary"
                onClick={this.openTranscriptUpload}
                disabled={isSubmitting || isProcessingTranscript}
              >
                {isProcessingTranscript ? "Processing..." : "Upload Transcript"}
              </button>
            )}

            <input
              ref={this.transcriptInputRef}
              className="library-capture-hidden-input"
              type="file"
              onChange={this.handleTranscriptSelected}
            />
          </div>
        </div>

        <div ref={this.messagesContainerRef} className="library-capture-messages">
          {messages.map((message) => (
            <div
              key={message.id}
              className={`library-capture-message library-capture-message-${message.sender} ${
                message.isError ? "is-error" : ""
              }`}
            >
              <div className="library-capture-message-content">
                <div className="library-capture-markdown">
                  <ReactMarkdown
                    remarkPlugins={[remarkGfm]}
                    components={{
                      a: ({ node: _node, ...props }) => (
                        <a {...props} target="_blank" rel="noreferrer noopener" />
                      ),
                    }}
                  >
                    {message.content}
                  </ReactMarkdown>
                </div>
              </div>
              <div className="library-capture-message-time">
                {new Date(message.timestamp).toLocaleTimeString([], {
                  hour: "2-digit",
                  minute: "2-digit",
                })}
                {message.isStreaming ? " • streaming" : ""}
              </div>
            </div>
          ))}
        </div>

        {pendingApproval && (
          <div className="library-capture-approval-card">
            <h3>Approval Required</h3>
            <p>
              <strong>Tool:</strong> {pendingApproval.tool}
            </p>
            <p>
              <strong>Summary:</strong> {pendingApproval.summary || "No summary provided."}
            </p>
            {pendingApproval.arguments && (
              <pre className="library-capture-approval-args">
                {JSON.stringify(pendingApproval.arguments, null, 2)}
              </pre>
            )}
            <div className="library-capture-approval-actions">
              <button
                type="button"
                className="library-capture-approve"
                onClick={() => {
                  void this.handleApprovalDecision("approve");
                }}
                disabled={isSubmitting}
              >
                Approve
              </button>
              <button
                type="button"
                className="library-capture-deny"
                onClick={() => {
                  void this.handleApprovalDecision("reject");
                }}
                disabled={isSubmitting}
              >
                Deny
              </button>
            </div>
          </div>
        )}

        {(error || transcriptError) && (
          <div className="library-capture-error-banner">
            {error || transcriptError}
          </div>
        )}

        <div className="library-capture-composer">
          <textarea
            value={inputText}
            onChange={(event) => this.setState({ inputText: event.target.value })}
            onKeyDown={(event) => {
              void this.handleInputKeyDown(event);
            }}
            placeholder={placeholder}
            disabled={isSubmitting || isProcessingTranscript}
          />
          <button
            type="button"
            onClick={() => {
              void this.handleSend();
            }}
            disabled={isSubmitting || isProcessingTranscript || !inputText.trim()}
          >
            {isSubmitting ? "Sending..." : submitLabel}
          </button>
        </div>
      </div>
    );
  }
}

export default LibraryCapture;
