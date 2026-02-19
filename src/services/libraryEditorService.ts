import {
  ApiResponse,
  ApiService,
  LibraryFileResponse,
  LibrarySaveResponse,
  LibraryTreeResponse,
} from "../types";

const API_BASE_CANDIDATES = [
  "/api/v1/plugin-api/braindrive-library/library/editor",
  "/api/v1/plugin-api/BrainDriveLibraryPlugin/library/editor",
  "/api/v1/plugin-api/BrainDriveLibraryService/library/editor",
] as const;

export class LibraryEditorService {
  private api?: ApiService;

  constructor(api?: ApiService) {
    this.api = api;
  }

  async listTree(path: string = ""): Promise<LibraryTreeResponse> {
    const encodedPath = encodeURIComponent(path || "");
    const endpoint = `/tree?path=${encodedPath}`;
    const payload = await this.requestWithFallback("get", endpoint);

    if (!payload?.success || !Array.isArray(payload.items)) {
      throw new Error(this.extractErrorMessage(payload) || "Unable to load library tree.");
    }

    return payload as LibraryTreeResponse;
  }

  async readFile(path: string): Promise<LibraryFileResponse> {
    const encodedPath = encodeURIComponent(path || "");
    const endpoint = `/file?path=${encodedPath}`;
    const payload = await this.requestWithFallback("get", endpoint);

    if (!payload?.success || typeof payload.content !== "string") {
      throw new Error(this.extractErrorMessage(payload) || "Unable to read file.");
    }

    return payload as LibraryFileResponse;
  }

  async saveFile(path: string, content: string): Promise<LibrarySaveResponse> {
    const payload = await this.requestWithFallback("put", "/file", {
      path,
      content,
    });

    if (!payload?.success) {
      throw new Error(this.extractErrorMessage(payload) || "Unable to save file.");
    }

    return payload as LibrarySaveResponse;
  }

  private ensureApi(): ApiService {
    if (!this.api) {
      throw new Error("API service is not available.");
    }
    return this.api;
  }

  private normalizeResponse(response: ApiResponse | null | undefined): any {
    if (!response || typeof response !== "object") {
      return response;
    }

    const nested = (response as any).data;
    if (nested && typeof nested === "object") {
      return nested;
    }

    return response;
  }

  private extractErrorMessage(payload: any): string | null {
    if (!payload || typeof payload !== "object") {
      return null;
    }

    const direct = payload.message;
    if (typeof direct === "string" && direct.trim()) {
      return direct.trim();
    }

    const detail = payload.detail;
    if (typeof detail === "string" && detail.trim()) {
      return detail.trim();
    }

    if (detail && typeof detail === "object") {
      const nested = (detail as any).message;
      if (typeof nested === "string" && nested.trim()) {
        return nested.trim();
      }
    }

    const error = payload.error;
    if (typeof error === "string" && error.trim()) {
      return error.trim();
    }
    if (error && typeof error === "object") {
      const nested = (error as any).message;
      if (typeof nested === "string" && nested.trim()) {
        return nested.trim();
      }
    }

    return null;
  }

  private extractErrorMessageFromException(error: any): string | null {
    if (!error || typeof error !== "object") {
      return null;
    }

    const responsePayload = this.normalizeResponse((error as any).response?.data);
    const responseMessage = this.extractErrorMessage(responsePayload);
    if (responseMessage) {
      return responseMessage;
    }

    const rawResponsePayload = (error as any).response?.data;
    const rawResponseMessage = this.extractErrorMessage(rawResponsePayload);
    if (rawResponseMessage) {
      return rawResponseMessage;
    }

    const message = (error as any).message;
    if (typeof message === "string" && message.trim()) {
      const normalized = message.trim();
      if (!/^Request failed with status code \\d+$/i.test(normalized)) {
        return normalized;
      }
    }

    return null;
  }

  private async requestWithFallback(
    method: "get" | "put",
    endpoint: string,
    body?: any
  ): Promise<any> {
    const api = this.ensureApi();
    let lastError: any = null;
    let extractedErrorMessage: string | null = null;

    for (const base of API_BASE_CANDIDATES) {
      const url = `${base}${endpoint}`;
      try {
        let response: ApiResponse;

        if (method === "get") {
          response = await api.get(url);
        } else if (typeof api.put === "function") {
          response = await api.put(url, body);
        } else if (typeof api.post === "function") {
          response = await api.post(url, body);
        } else {
          throw new Error("API service does not support PUT or POST operations.");
        }

        return this.normalizeResponse(response);
      } catch (error) {
        lastError = error;
        extractedErrorMessage = this.extractErrorMessageFromException(error) || extractedErrorMessage;
      }
    }

    if (extractedErrorMessage) {
      throw new Error(extractedErrorMessage);
    }

    if (lastError && typeof lastError === "object") {
      const maybeMessage = (lastError as any).message || (lastError as any).detail;
      if (typeof maybeMessage === "string" && maybeMessage.trim()) {
        throw new Error(maybeMessage.trim());
      }
    }

    throw new Error("Library API request failed.");
  }
}
