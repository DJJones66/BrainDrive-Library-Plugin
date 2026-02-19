export type LibraryTheme = "light" | "dark";

export interface ApiResponse {
  data?: any;
  status?: number;
  [key: string]: any;
}

export interface ApiService {
  get: (url: string, options?: any) => Promise<ApiResponse>;
  post?: (url: string, body?: any, options?: any) => Promise<ApiResponse>;
  put?: (url: string, body?: any, options?: any) => Promise<ApiResponse>;
  delete?: (url: string, options?: any) => Promise<ApiResponse>;
  postStreaming?: (
    url: string,
    data: any,
    onChunk: (chunk: string) => void,
    options?: any
  ) => Promise<ApiResponse>;
}

export interface ThemeService {
  getCurrentTheme?: () => string;
  addThemeChangeListener?: (listener: (theme: string) => void) => void;
  removeThemeChangeListener?: (listener: (theme: string) => void) => void;
}

export interface PageContextService {
  getCurrentPageContext?: () => any;
  onPageContextChange?: (listener: (context: any) => void) => void;
}

export interface LibraryPageContext {
  pageId?: string;
  pageName?: string;
  pageRoute?: string;
  isStudioPage?: boolean;
  [key: string]: any;
}

export interface SettingsService {
  get?: (key: string) => any;
  set?: (key: string, value: any) => Promise<void>;
  getSetting?: (key: string, options?: any) => Promise<any>;
  setSetting?: (key: string, value: any, options?: any) => Promise<any>;
  getSettingDefinitions?: () => Promise<any>;
}

export interface LibraryPluginServices {
  api?: ApiService;
  theme?: ThemeService;
  pageContext?: PageContextService;
  settings?: SettingsService;
}

export interface LibraryEditorProps {
  services?: LibraryPluginServices;
  title?: string;
  subtitle?: string;
}

export interface LibraryCaptureProps {
  services?: LibraryPluginServices;
  moduleId?: string;

  // Common configurable copy
  initialGreeting?: string;
  initial_greeting?: string;
  promptQuestion?: string;
  prompt_question?: string;
  inputPlaceholder?: string;
  input_placeholder?: string;
  submitLabel?: string;
  submit_label?: string;

  // Conversation behavior
  conversationType?: string;
  conversation_type?: string;
  enableStreaming?: boolean;
  enable_streaming?: boolean;

  // Model defaults/controls
  showModelSelection?: boolean;
  show_model_selection?: boolean;
  defaultModelKey?: string;
  default_model_key?: string;
  defaultModelProvider?: string | null;
  default_model_provider?: string | null;
  defaultModelServerId?: string | null;
  default_model_server_id?: string | null;
  defaultModelName?: string | null;
  default_model_name?: string | null;
  defaultModelProviderId?: string | null;
  default_model_provider_id?: string | null;
  defaultModelServerName?: string | null;
  default_model_server_name?: string | null;
  lockModelSelection?: boolean;
  lock_model_selection?: boolean;

  // Scope defaults/controls
  defaultLibraryScopeEnabled?: boolean;
  default_library_scope_enabled?: boolean;
  defaultProjectSlug?: string | null;
  default_project_slug?: string | null;
  defaultProjectLifecycle?: string;
  default_project_lifecycle?: string;
  defaultScopeRoot?: "projects" | "life" | null;
  default_scope_root?: "projects" | "life" | null;
  defaultScopePath?: string | null;
  default_scope_path?: string | null;
  lockProjectScope?: boolean;
  lock_project_scope?: boolean;

  // Transcript controls
  showTranscriptUpload?: boolean;
  show_transcript_upload?: boolean;
  defaultTranscriptSource?: string;
  default_transcript_source?: string;
}

export interface LibraryTreeItem {
  name: string;
  path: string;
  type: "file" | "directory";
  extension: string;
  size: number;
  supported: boolean;
  modified_at?: string;
}

export interface LibraryTreeResponse {
  success: boolean;
  path: string;
  parent_path: string | null;
  count: number;
  items: LibraryTreeItem[];
}

export interface LibraryFileResponse {
  success: boolean;
  path: string;
  name: string;
  extension: string;
  content: string;
  size: number;
  encoding: string;
  modified_at?: string;
  is_markdown: boolean;
  supported: boolean;
}

export interface LibrarySaveResponse {
  success: boolean;
  path: string;
  bytes: number;
  created: boolean;
  updated_at?: string;
}

export interface ModelInfo {
  name: string;
  provider: string;
  providerId: string;
  serverName: string;
  serverId: string;
  isTemporary?: boolean;
}

export interface ScopeOption {
  name: string;
  slug: string;
  lifecycle: string;
  path: string;
  scope_root?: "projects" | "life";
  has_agent_md: boolean;
  has_spec: boolean;
  has_build_plan: boolean;
  has_decisions: boolean;
}

export type MCPProjectSource =
  | "ui"
  | "prompt_auto"
  | "backend_suggested"
  | "config_default";

export type MCPApprovalStatus = "pending" | "approved" | "rejected";

export interface MCPApprovalRequest {
  type?: "approval_request";
  request_id?: string;
  tool: string;
  safety_class?: string;
  summary?: string;
  arguments?: Record<string, any>;
  diff_preview?: string;
  status?: MCPApprovalStatus;
  created_at?: string;
  resolved_at?: string;
  [key: string]: any;
}

export interface MCPApprovalDecision {
  action: "approve" | "reject";
  request_id?: string;
  tool?: string;
  arguments?: Record<string, any>;
}

export interface MCPRequestScope {
  mcp_tools_enabled: boolean;
  mcp_scope_mode: "none" | "project";
  mcp_project_slug?: string;
  mcp_project_name?: string;
  mcp_project_lifecycle?: string;
  mcp_project_source: MCPProjectSource;
  mcp_plugin_slug?: string;
}

export interface MCPRequestParams extends MCPRequestScope {
  mcp_approval?: MCPApprovalDecision;
}

export interface MCPMetadataEvent {
  type:
    | "project_scope_suggested"
    | "project_scope_selected"
    | "tooling_state"
    | "tool_call"
    | "tool_result"
    | "auto_continue"
    | "approval_request"
    | "approval_required"
    | "approval_resolution"
    | "orchestration_context_error";
  [key: string]: any;
}

export interface CaptureMessage {
  id: string;
  sender: "user" | "ai" | "system";
  content: string;
  timestamp: string;
  isStreaming?: boolean;
  isError?: boolean;
}

export interface CaptureDocumentProcessResult {
  filename: string | null;
  file_type: string;
  content_type: string;
  file_size: number | null;
  extracted_text: string;
  text_length: number;
  processing_success: boolean;
  metadata?: Record<string, any>;
  warnings?: string[];
  error?: string;
}
