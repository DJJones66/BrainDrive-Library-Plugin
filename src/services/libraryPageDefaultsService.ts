import { ApiResponse, ApiService, ModelInfo } from "../types";

const LIBRARY_PLUGIN_SLUG = "BrainDriveLibraryPlugin";
const LIBRARY_CAPTURE_MODULE_NAME = "LibraryCapture";

type PageRecord = Record<string, any>;
type PageContent = Record<string, any>;

export interface SaveCaptureDefaultModelOptions {
  pageId: string;
  moduleId?: string | null;
  selectedModel: ModelInfo;
}

export interface SaveCaptureDefaultModelResult {
  pageId: string;
  default_model_key: string;
  default_model_provider: string;
  default_model_server_id: string;
  default_model_name: string;
  updated_targets: number;
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

function normalizeToken(value: unknown): string {
  return String(value || "").trim().toLowerCase();
}

function compactToken(value: unknown): string {
  return normalizeToken(value).replace(/[^a-z0-9]+/g, "");
}

function looksLikeCaptureToken(value: unknown): boolean {
  return compactToken(value).includes("librarycapture");
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

export class LibraryPageDefaultsService {
  private api?: ApiService;

  constructor(api?: ApiService) {
    this.api = api;
  }

  private ensureApi(): ApiService {
    if (!this.api) {
      throw new Error("API service is not available.");
    }
    return this.api;
  }

  private normalizePageId(pageId: string): string {
    return String(pageId || "").trim().replace(/-/g, "");
  }

  private extractPageRecord(response: ApiResponse): PageRecord {
    const payload = normalizeApiPayload(response);
    const candidate =
      payload?.page ||
      payload?.data?.page ||
      payload?.data ||
      payload;

    if (!candidate || typeof candidate !== "object") {
      throw new Error("Unexpected page API response format.");
    }

    return candidate as PageRecord;
  }

  private parsePageContent(rawContent: any): PageContent {
    const parsed = typeof rawContent === "string" ? JSON.parse(rawContent) : rawContent;
    if (!parsed || typeof parsed !== "object") {
      throw new Error("Page content is not a valid JSON object.");
    }
    return parsed as PageContent;
  }

  private buildDefaultModelConfig(model: ModelInfo): Record<string, string> {
    const provider = String(model.provider || "").trim();
    const serverId = String(model.serverId || "").trim();
    const modelName = String(model.name || "").trim();

    if (!provider || !serverId || !modelName) {
      throw new Error("Selected model is missing provider, server, or model name.");
    }

    return {
      default_model_key: `${provider}::${serverId}::${modelName}`,
      default_model_provider: provider,
      default_model_server_id: serverId,
      default_model_name: modelName,
      default_model_provider_id: String(model.providerId || "").trim(),
      default_model_server_name: String(model.serverName || "").trim(),
    };
  }

  private requestedModuleMatches(requestedModuleId: string, candidates: unknown[]): boolean {
    const requestedToken = normalizeToken(requestedModuleId);
    if (!requestedToken) {
      return false;
    }

    const requestedCompact = compactToken(requestedToken);
    for (const candidate of candidates) {
      const token = normalizeToken(candidate);
      if (!token) {
        continue;
      }

      if (token === requestedToken) {
        return true;
      }

      const compact = compactToken(token);
      if (!compact || !requestedCompact) {
        continue;
      }

      if (
        compact === requestedCompact ||
        compact.includes(requestedCompact) ||
        requestedCompact.includes(compact)
      ) {
        return true;
      }
    }

    return false;
  }

  private isLibraryPluginToken(value: unknown): boolean {
    return normalizeToken(value) === normalizeToken(LIBRARY_PLUGIN_SLUG);
  }

  private moduleDefinitionMatchesCaptureTarget(
    moduleKey: string,
    moduleDefinition: Record<string, any>,
    requestedModuleId: string
  ): boolean {
    const config =
      moduleDefinition.config && typeof moduleDefinition.config === "object"
        ? (moduleDefinition.config as Record<string, any>)
        : {};

    const moduleIdToken = normalizeToken(moduleDefinition.moduleId || config.moduleId);
    const moduleNameToken = normalizeToken(
      moduleDefinition.moduleName || config.moduleName || config.displayName
    );
    const pluginToken = normalizeToken(
      moduleDefinition.pluginId ||
        moduleDefinition.pluginSlug ||
        moduleDefinition.plugin_id ||
        config.pluginId ||
        config.pluginSlug ||
        config.plugin_id
    );

    const looksLikeCaptureModule =
      looksLikeCaptureToken(moduleIdToken) ||
      looksLikeCaptureToken(moduleNameToken) ||
      looksLikeCaptureToken(moduleKey);

    if (
      requestedModuleId &&
      this.requestedModuleMatches(requestedModuleId, [
        moduleKey,
        moduleDefinition.moduleId,
        moduleDefinition.moduleName,
        config.moduleId,
        config.moduleName,
      ])
    ) {
      return true;
    }

    if (
      requestedModuleId &&
      !looksLikeCaptureToken(requestedModuleId) &&
      normalizeToken(requestedModuleId) !== normalizeToken(LIBRARY_CAPTURE_MODULE_NAME)
    ) {
      return false;
    }

    if (!this.isLibraryPluginToken(pluginToken) && !looksLikeCaptureModule) {
      return false;
    }

    return (
      moduleIdToken === normalizeToken(LIBRARY_CAPTURE_MODULE_NAME) ||
      moduleNameToken === normalizeToken(LIBRARY_CAPTURE_MODULE_NAME) ||
      looksLikeCaptureModule
    );
  }

  private layoutItemMatchesCaptureTarget(
    layoutItem: Record<string, any>,
    requestedModuleId: string
  ): boolean {
    const args =
      layoutItem.args && typeof layoutItem.args === "object"
        ? (layoutItem.args as Record<string, any>)
        : {};

    const argsModuleId = normalizeToken(args.moduleId);
    const argsModuleName = normalizeToken(args.moduleName);
    const argsDisplayName = normalizeToken(args.displayName);
    const layoutItemId = normalizeToken(layoutItem.i || layoutItem.moduleUniqueId);
    const pluginToken = normalizeToken(
      layoutItem.pluginId || args.pluginId || args.pluginSlug || args.plugin_id
    );

    const looksLikeCaptureLayout =
      argsDisplayName === "library capture" ||
      looksLikeCaptureToken(argsModuleId) ||
      looksLikeCaptureToken(argsModuleName) ||
      looksLikeCaptureToken(layoutItemId);

    if (
      requestedModuleId &&
      this.requestedModuleMatches(requestedModuleId, [
        args.moduleId,
        args.moduleName,
        layoutItem.i,
        layoutItem.moduleUniqueId,
      ])
    ) {
      return true;
    }

    if (
      requestedModuleId &&
      !looksLikeCaptureToken(requestedModuleId) &&
      normalizeToken(requestedModuleId) !== normalizeToken(LIBRARY_CAPTURE_MODULE_NAME)
    ) {
      return false;
    }

    if (!this.isLibraryPluginToken(pluginToken) && !looksLikeCaptureLayout) {
      return false;
    }

    return (
      argsModuleId === normalizeToken(LIBRARY_CAPTURE_MODULE_NAME) ||
      argsModuleName === normalizeToken(LIBRARY_CAPTURE_MODULE_NAME) ||
      looksLikeCaptureLayout
    );
  }

  private applyDefaultModelToModules(
    content: PageContent,
    modelConfig: Record<string, string>,
    requestedModuleId: string
  ): number {
    const modules = content.modules;
    if (!modules || typeof modules !== "object") {
      return 0;
    }

    let updatedTargets = 0;
    for (const [moduleKey, rawModuleDef] of Object.entries(modules)) {
      if (!rawModuleDef || typeof rawModuleDef !== "object") {
        continue;
      }

      const moduleDefinition = rawModuleDef as Record<string, any>;
      if (!this.moduleDefinitionMatchesCaptureTarget(moduleKey, moduleDefinition, requestedModuleId)) {
        continue;
      }

      const existingConfig =
        moduleDefinition.config && typeof moduleDefinition.config === "object"
          ? (moduleDefinition.config as Record<string, any>)
          : {};

      moduleDefinition.config = {
        ...existingConfig,
        ...modelConfig,
      };
      updatedTargets += 1;
    }

    return updatedTargets;
  }

  private applyDefaultModelToLayouts(
    content: PageContent,
    modelConfig: Record<string, string>,
    requestedModuleId: string
  ): number {
    const layouts = content.layouts;
    if (!layouts || typeof layouts !== "object") {
      return 0;
    }

    let updatedTargets = 0;
    for (const rawLayout of Object.values(layouts)) {
      if (!Array.isArray(rawLayout)) {
        continue;
      }

      for (const rawItem of rawLayout) {
        if (!rawItem || typeof rawItem !== "object") {
          continue;
        }

        const layoutItem = rawItem as Record<string, any>;
        if (!this.layoutItemMatchesCaptureTarget(layoutItem, requestedModuleId)) {
          continue;
        }

        const existingArgs =
          layoutItem.args && typeof layoutItem.args === "object"
            ? (layoutItem.args as Record<string, any>)
            : {};

        layoutItem.args = {
          ...existingArgs,
          ...modelConfig,
        };
        updatedTargets += 1;
      }
    }

    return updatedTargets;
  }

  async saveCaptureDefaultModelForPage(
    options: SaveCaptureDefaultModelOptions
  ): Promise<SaveCaptureDefaultModelResult> {
    const api = this.ensureApi();
    if (typeof api.get !== "function") {
      throw new Error("API service does not support page reads.");
    }
    if (typeof api.put !== "function") {
      throw new Error("API service does not support page updates.");
    }

    const normalizedPageId = this.normalizePageId(options.pageId);
    if (!normalizedPageId) {
      throw new Error("Current page ID is unavailable.");
    }

    const requestedModuleId = normalizeToken(options.moduleId || "");
    const modelConfig = this.buildDefaultModelConfig(options.selectedModel);

    const pageResponse = await api.get(`/api/v1/pages/${normalizedPageId}`);
    const pageRecord = this.extractPageRecord(pageResponse);
    const parsedContent = this.parsePageContent(pageRecord.content);
    const updatedContent = deepClone(parsedContent);

    let updatedTargets = 0;
    updatedTargets += this.applyDefaultModelToModules(updatedContent, modelConfig, requestedModuleId);
    updatedTargets += this.applyDefaultModelToLayouts(updatedContent, modelConfig, requestedModuleId);

    if (updatedTargets < 1) {
      throw new Error("Could not locate Library Capture module config for this page.");
    }

    await api.put(`/api/v1/pages/${normalizedPageId}`, {
      content: updatedContent,
    });

    return {
      pageId: normalizedPageId,
      default_model_key: modelConfig.default_model_key,
      default_model_provider: modelConfig.default_model_provider,
      default_model_server_id: modelConfig.default_model_server_id,
      default_model_name: modelConfig.default_model_name,
      updated_targets: updatedTargets,
    };
  }
}
