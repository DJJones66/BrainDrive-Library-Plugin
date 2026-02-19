import {
  ApiResponse,
  ApiService,
  CaptureDocumentProcessResult,
  MCPApprovalDecision,
  MCPMetadataEvent,
  MCPRequestParams,
  LibraryPageContext,
  ModelInfo,
  ScopeOption,
} from "../types";

const PROVIDER_SETTINGS_ID_MAP: Record<string, string> = {
  ollama: "ollama_servers_settings",
  openai: "openai_api_keys_settings",
  openrouter: "openrouter_api_keys_settings",
  claude: "claude_api_keys_settings",
  anthropic: "claude_api_keys_settings",
  groq: "groq_api_keys_settings",
};

const LIBRARY_PROJECT_ENDPOINT =
  "/api/v1/plugin-api/braindrive-library/library/projects?lifecycle=active";

const LIBRARY_LIFE_ENDPOINT_CANDIDATES = [
  "/api/v1/plugin-api/braindrive-library/library/life",
  "/api/v1/plugin-api/braindrive-library/library/projects?path=life",
  "/api/v1/plugin-api/braindrive-library/library/projects?scope=life",
] as const;


function extractTextFromData(data: any): string {
  if (!data) {
    return "";
  }

  if (typeof data === "string") {
    return data;
  }

  if (typeof data === "object") {
    if (typeof data.text === "string") {
      return data.text;
    }

    if (Array.isArray(data.choices) && data.choices.length > 0) {
      const choice = data.choices[0];
      if (choice?.delta?.content) {
        return String(choice.delta.content);
      }
      if (choice?.message?.content) {
        return String(choice.message.content);
      }
      if (choice?.text) {
        return String(choice.text);
      }
    }

    const fields = ["content", "message", "response", "output", "result", "answer"];
    for (const field of fields) {
      if (typeof data[field] === "string") {
        return data[field] as string;
      }
      if (typeof data[field]?.content === "string") {
        return data[field].content as string;
      }
    }

    if (typeof data.delta?.content === "string") {
      return data.delta.content;
    }
  }

  return "";
}

function normalizeApiPayload(response: ApiResponse | null | undefined): any {
  if (!response || typeof response !== "object") {
    return response;
  }
  if (response.data !== undefined) {
    return response.data;
  }
  return response;
}

function parseListPayload(payload: any): any[] {
  if (!payload || typeof payload !== "object") {
    return [];
  }

  const candidates = [
    payload.projects,
    payload.items,
    payload.life,
    payload.topics,
    payload.scopes,
    payload.data,
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }

  return [];
}

function slugify(value: string): string {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
}

function normalizeScopePath(value: string): string {
  return String(value || "")
    .trim()
    .replace(/\\+/g, "/")
    .replace(/^\/+/, "")
    .replace(/\/+$/, "");
}

function normalizeScopeOption(raw: any, root: "projects" | "life"): ScopeOption | null {
  if (typeof raw === "string") {
    const optionSlug = slugify(raw);
    if (!optionSlug) {
      return null;
    }
    const basePath = root === "life" ? `life/${optionSlug}` : `projects/active/${optionSlug}`;
    return {
      name: String(raw).trim() || optionSlug,
      slug: optionSlug,
      lifecycle: "active",
      path: basePath,
      scope_root: root,
      has_agent_md: false,
      has_spec: false,
      has_build_plan: false,
      has_decisions: false,
    };
  }

  if (!raw || typeof raw !== "object") {
    return null;
  }

  const candidatePath = normalizeScopePath(raw.path || raw.scope_path || "");
  const candidateSlug = slugify(
    raw.slug || raw.topic || raw.name || (candidatePath ? candidatePath.split("/").pop() : "")
  );

  if (!candidateSlug) {
    return null;
  }

  let normalizedPath = candidatePath;
  if (!normalizedPath) {
    normalizedPath = root === "life" ? `life/${candidateSlug}` : `projects/active/${candidateSlug}`;
  }

  if (root === "life" && !normalizedPath.startsWith("life/")) {
    normalizedPath = `life/${candidateSlug}`;
  }

  if (root === "projects" && !normalizedPath.startsWith("projects/")) {
    normalizedPath = `projects/active/${candidateSlug}`;
  }

  return {
    name: String(raw.name || raw.topic || candidateSlug).trim() || candidateSlug,
    slug: candidateSlug,
    lifecycle: String(raw.lifecycle || "active"),
    path: normalizedPath,
    scope_root: root,
    has_agent_md: Boolean(raw.has_agent_md),
    has_spec: Boolean(raw.has_spec),
    has_build_plan: Boolean(raw.has_build_plan),
    has_decisions: Boolean(raw.has_decisions),
  };
}

export interface SendCapturePromptOptions {
  prompt: string;
  selectedModel: ModelInfo;
  conversationId: string | null;
  conversationType: string;
  pageContext?: LibraryPageContext | null;
  useStreaming: boolean;
  mcpParams: MCPRequestParams;
  onChunk: (chunk: string) => void;
  onConversationId: (conversationId: string) => void;
  onMetadataEvent: (event: MCPMetadataEvent) => void;
  abortController?: AbortController;
  approvalDecision?: MCPApprovalDecision;
}

export class LibraryCaptureService {
  private api?: ApiService;
  private currentUserId: string | null;

  constructor(api?: ApiService) {
    this.api = api;
    this.currentUserId = null;
  }

  private ensureApi(): ApiService {
    if (!this.api) {
      throw new Error("API service is not available.");
    }
    return this.api;
  }

  private async initializeUserId(): Promise<void> {
    if (this.currentUserId) {
      return;
    }

    const api = this.ensureApi();
    try {
      const response = await api.get("/api/v1/auth/me");
      const payload = normalizeApiPayload(response);
      const userId = payload?.id || payload?.user_id || null;
      this.currentUserId = typeof userId === "string" ? userId : null;
    } catch (_error) {
      this.currentUserId = null;
    }
  }

  async loadModels(): Promise<ModelInfo[]> {
    const api = this.ensureApi();
    const response = await api.get("/api/v1/ai/providers/all-models");
    const payload = normalizeApiPayload(response);
    const rawModels =
      (payload && Array.isArray(payload.models) ? payload.models : null) ||
      (Array.isArray(payload) ? payload : []);

    const mapped = rawModels
      .map((entry: any) => {
        const provider = String(entry?.provider || "ollama").trim() || "ollama";
        const name = String(entry?.name || entry?.id || "").trim();
        if (!name) {
          return null;
        }

        const modelNameLower = name.toLowerCase();
        if (modelNameLower.includes("embed") || modelNameLower.includes("embedding")) {
          return null;
        }

        const providerId =
          PROVIDER_SETTINGS_ID_MAP[provider.toLowerCase()] ||
          String(entry?.provider_id || entry?.providerId || provider).trim() ||
          provider;
        const serverId = String(entry?.server_id || entry?.serverId || "unknown").trim() || "unknown";
        const serverName = String(entry?.server_name || entry?.serverName || "Unknown Server").trim() || "Unknown Server";

        const mappedModel: ModelInfo = {
          name,
          provider,
          providerId,
          serverId,
          serverName,
        };
        return mappedModel;
      })
      .filter((entry: ModelInfo | null): entry is ModelInfo => Boolean(entry));

    return mapped;
  }

  async loadProjectScopes(): Promise<ScopeOption[]> {
    const api = this.ensureApi();
    const response = await api.get(LIBRARY_PROJECT_ENDPOINT);
    const payload = normalizeApiPayload(response);
    const values = parseListPayload(payload);
    return values
      .map((entry) => normalizeScopeOption(entry, "projects"))
      .filter((entry): entry is ScopeOption => Boolean(entry));
  }

  async loadLifeScopes(): Promise<ScopeOption[]> {
    const api = this.ensureApi();

    for (const endpoint of LIBRARY_LIFE_ENDPOINT_CANDIDATES) {
      try {
        const response = await api.get(endpoint);
        const payload = normalizeApiPayload(response);
        const values = parseListPayload(payload);
        if (values.length > 0) {
          return values
            .map((entry) => normalizeScopeOption(entry, "life"))
            .filter((entry): entry is ScopeOption => Boolean(entry));
        }
      } catch (_error) {
        // Try next candidate endpoint.
      }
    }

    return [];
  }

  async processDocument(file: File): Promise<CaptureDocumentProcessResult> {
    const api = this.ensureApi();
    if (typeof api.post !== "function") {
      throw new Error("API service does not support document uploads.");
    }

    const formData = new FormData();
    formData.append("file", file);

    const response = await api.post(
      "/api/v1/documents/process",
      formData,
      {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      }
    );

    const payload = normalizeApiPayload(response);
    if (!payload || payload.processing_success !== true || typeof payload.extracted_text !== "string") {
      throw new Error(payload?.error || "Failed to process transcript file.");
    }

    return payload as CaptureDocumentProcessResult;
  }

  async sendPrompt(options: SendCapturePromptOptions): Promise<boolean> {
    const api = this.ensureApi();
    await this.initializeUserId();

    if (typeof api.post !== "function") {
      throw new Error("API service does not support POST requests.");
    }

    const prompt = String(options.prompt || "").trim() || "Continue.";

    const params: Record<string, any> = {
      temperature: 0.3,
      max_tokens: 2048,
      ...options.mcpParams,
    };

    if (options.approvalDecision) {
      params.mcp_approval = options.approvalDecision;
    }

    const requestPayload: Record<string, any> = {
      provider: options.selectedModel.provider,
      settings_id: options.selectedModel.providerId,
      server_id: options.selectedModel.serverId,
      model: options.selectedModel.name,
      messages: [{ role: "user", content: prompt }],
      params,
      stream: options.useStreaming,
      user_id: this.currentUserId || "current",
      conversation_id: options.conversationId,
      conversation_type: options.conversationType,
    };
    if (options.pageContext && typeof options.pageContext.pageId === "string") {
      requestPayload.page_id = options.pageContext.pageId;
      requestPayload.page_context = JSON.stringify({
        pageName: options.pageContext.pageName,
        pageRoute: options.pageContext.pageRoute,
        isStudioPage: options.pageContext.isStudioPage,
      });
    }

    if (options.useStreaming && typeof api.postStreaming === "function") {
      await api.postStreaming(
        "/api/v1/ai/providers/chat",
        requestPayload,
        (chunk: string) => {
          const content = String(chunk || "");
          const lines = content.split("\n").filter((line) => line.trim().length > 0);
          for (const line of lines) {
            const payloadLine = line.startsWith("data: ") ? line.slice(6) : line;
            if (!payloadLine || payloadLine === "[DONE]") {
              continue;
            }

            try {
              const parsed = JSON.parse(payloadLine);

              if (parsed?.conversation_id && !options.conversationId) {
                options.onConversationId(String(parsed.conversation_id));
              }

              if (
                parsed &&
                typeof parsed === "object" &&
                typeof parsed.type === "string" &&
                (
                  parsed.type === "project_scope_suggested" ||
                  parsed.type === "project_scope_selected" ||
                  parsed.type === "tooling_state" ||
                  parsed.type === "tool_call" ||
                  parsed.type === "tool_result" ||
                  parsed.type === "auto_continue" ||
                  parsed.type === "approval_request" ||
                  parsed.type === "approval_required" ||
                  parsed.type === "approval_resolution" ||
                  parsed.type === "orchestration_context_error"
                )
              ) {
                options.onMetadataEvent(parsed as MCPMetadataEvent);
                continue;
              }

              const text = extractTextFromData(parsed);
              if (text) {
                options.onChunk(text);
              }
            } catch (_error) {
              // Ignore non-json chunk segments.
            }
          }
        },
        {
          timeout: 120000,
          signal: options.abortController?.signal,
        }
      );
      return true;
    }

    const response = await api.post("/api/v1/ai/providers/chat", requestPayload, {
      timeout: 120000,
    });
    const payload = normalizeApiPayload(response);

    if (payload?.conversation_id && !options.conversationId) {
      options.onConversationId(String(payload.conversation_id));
    }

    if (payload?.tooling_state) {
      options.onMetadataEvent({
        type: "tooling_state",
        ...(payload.tooling_state as Record<string, any>),
      });
    }

    if (payload?.approval_required && payload?.approval_request) {
      options.onMetadataEvent({
        type: "approval_required",
        approval_request: payload.approval_request,
      });
    }

    if (payload?.approval_resolution) {
      options.onMetadataEvent({
        type: "approval_resolution",
        ...(payload.approval_resolution as Record<string, any>),
      });
    }

    const text = extractTextFromData(payload);
    if (text) {
      options.onChunk(text);
    }

    if (!text && !payload?.approval_required && !payload?.approval_resolution) {
      throw new Error("No response text received.");
    }

    return true;
  }
}
