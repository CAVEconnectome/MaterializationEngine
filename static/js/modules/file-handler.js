import { sessionManager } from "./session-manager.js";

class FileHandler {
  constructor() {
    this.file = null;
    this.chunkSize = 1024 * 1024; // 1MB chunks
    this.maxRetries = 3;
    this.retryDelay = 1000; // 1 second

    const savedFileData = sessionManager.getStepData("fileSelection");
    if (savedFileData) {
      this.restoreFromSession(savedFileData);
    }
    this.uploadState = {
      uploadId: null,
      fileName: null,
      fileSize: null,
      uploadedChunks: new Set(),
      totalChunks: 0,
      startTime: null,
      status: "idle", // idle, uploading, paused, completed, error
      resumeUrl: null,
      lastChunkUploaded: -1,
    };

    this.initializeEventListeners();
  }

  initializeEventListeners() {
    const fileInput = document.getElementById("csvFile");
    const uploadButton = document.getElementById("uploadButton");
    const pauseButton = document.getElementById("pauseUpload");
    const resumeButton = document.getElementById("resumeUpload");

    fileInput?.addEventListener("change", (event) =>
      this.handleFileSelect(event)
    );
    uploadButton?.addEventListener("click", () => this.startUpload());
    pauseButton?.addEventListener("click", () => this.pauseUpload());
    resumeButton?.addEventListener("click", () => this.resumeUpload());

    window.addEventListener("beforeunload", () => this.saveUploadState());
  }

  restoreFromSession(savedData) {
    if (savedData.uploadState) {
      this.uploadState = {
        ...savedData.uploadState,
        uploadedChunks: new Set(savedData.uploadState.uploadedChunks),
      };
    }
    //TODO - Add code to restore file data
  }
  validateAndPreviewFile() {
    if (!this.file) {
      this.handleError("No file selected", new Error("Please select a file"));
      return false;
    }

    if (!this.file.name.toLowerCase().endsWith(".csv")) {
      this.handleError(
        "Invalid file type",
        new Error("Please select a CSV file")
      );
      return false;
    }

    this.previewFile();
    return true;
  }

  async previewFile() {
    try {
      const preview = await this.readFilePreview();
      sessionManager.saveStepData("fileSelection", {
        ...sessionManager.getStepData("fileSelection"),
        preview: preview,
      });

      this.dispatchEvent("filePreview", { preview });
    } catch (error) {
      this.handleError("Failed to preview file", error);
    }
  }

  async startUpload() {
    if (!this.validateAndPreviewFile()) {
      return;
    }

    try {
      await this.initializeUpload();
      await this.processUpload();
    } catch (error) {
      this.handleError("Upload failed", error);
      sessionManager.saveStepData("fileSelection", {
        ...sessionManager.getStepData("fileSelection"),
        uploadState: {
          ...this.uploadState,
          status: "error",
          error: error.message,
        },
      });
    }
  }
  
  handleFileSelect(event) {
    this.file = event.target.files[0];

    if (this.file) {
      sessionManager.saveStepData("fileSelection", {
        file: {
          name: this.file.name,
          size: this.file.size,
          type: this.file.type,
          lastModified: this.file.lastModified,
        },
        uploadState: {
          ...this.uploadState,
          fileName: this.file.name,
          fileSize: this.file.size,
          uploadedChunks: Array.from(this.uploadState.uploadedChunks),
        },
      });

      this.validateAndPreviewFile();
    }
  }

  async initializeUpload() {
    try {
      this.uploadState.uploadId =
        this.uploadState.uploadId || this.generateUploadId();

      const existingState = await this.checkExistingUpload();
      if (existingState) {
        this.uploadState = { ...this.uploadState, ...existingState };
        this.showResumePrompt();
        return;
      }

      const response = await fetch(
        "/materialize/views/generate-presigned-url",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            filename: this.file.name,
            contentType: "text/csv",
            uploadId: this.uploadState.uploadId,
          }),
        }
      );

      if (!response.ok) {
        throw new Error("Failed to initialize upload");
      }

      const { resumableUrl } = await response.json();
      this.uploadState.resumeUrl = resumableUrl;
      this.uploadState.totalChunks = Math.ceil(this.file.size / this.chunkSize);
      this.uploadState.status = "uploading";
      this.uploadState.startTime = Date.now();

      this.saveUploadState();
    } catch (error) {
      throw new Error(`Upload initialization failed: ${error.message}`);
    }
  }

  async readFilePreview() {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        const lines = text.split("\n");
        const headers = lines[0].split(",").map((h) => h.trim());
        const firstRow =
          lines.length > 1 ? lines[1].split(",").map((c) => c.trim()) : [];

        resolve({
          headers,
          firstRow,
          totalRows: lines.length,
        });
      };
      reader.onerror = () => reject(new Error("Failed to read file"));

      // Read only first 2 lines for preview
      const blob = this.file.slice(0, 1024); // Read first 1KB TODO maybe read more?
      reader.readAsText(blob);
    });
  }

  async processUpload() {
    if (!this.file || !this.uploadState.resumeUrl) {
      throw new Error("Upload not properly initialized");
    }

    const startChunk = Math.max(...this.uploadState.uploadedChunks, -1) + 1;

    for (
      let chunkIndex = startChunk;
      chunkIndex < this.uploadState.totalChunks;
      chunkIndex++
    ) {
      if (this.uploadState.status !== "uploading") {
        break;
      }

      try {
        await this.uploadChunk(chunkIndex);
        this.uploadState.uploadedChunks.add(chunkIndex);
        this.uploadState.lastChunkUploaded = chunkIndex;
        this.saveUploadState();
        this.updateProgress();
      } catch (error) {
        if (await this.handleChunkError(chunkIndex, error)) {
          continue;
        }
        throw error;
      }
    }

    if (this.uploadState.status === "uploading") {
      await this.completeUpload();
    }
  }

  async uploadChunk(chunkIndex) {
    const start = chunkIndex * this.chunkSize;
    const end = Math.min(start + this.chunkSize, this.file.size);
    const chunk = this.file.slice(start, end);

    const response = await fetch(this.uploadState.resumeUrl, {
      method: "PUT",
      headers: {
        "Content-Range": `bytes ${start}-${end - 1}/${this.file.size}`,
        "Content-Type": "application/octet-stream",
      },
      body: chunk,
    });

    if (!response.ok && response.status !== 308) {
      throw new Error(`Chunk upload failed: ${response.status}`);
    }
  }

  async handleChunkError(chunkIndex, error) {
    for (let retry = 0; retry < this.maxRetries; retry++) {
      try {
        await new Promise((resolve) =>
          setTimeout(resolve, this.retryDelay * Math.pow(2, retry))
        );
        await this.uploadChunk(chunkIndex);
        return true;
      } catch (retryError) {
        console.warn(
          `Retry ${retry + 1} failed for chunk ${chunkIndex}:`,
          retryError
        );
      }
    }
    return false;
  }

  async checkExistingUpload() {
    const savedState = sessionStorage.getItem(
      `uploadState_${this.uploadState.uploadId}`
    );
    if (!savedState) return null;

    const state = JSON.parse(savedState);
    if (
      state.fileName !== this.file.name ||
      state.fileSize !== this.file.size
    ) {
      sessionStorage.removeItem(`uploadState_${this.uploadState.uploadId}`);
      return null;
    }

    const verifiedChunks = await this.verifyUploadedChunks(
      state.uploadedChunks
    );
    return {
      ...state,
      uploadedChunks: new Set(verifiedChunks),
    };
  }

  async verifyUploadedChunks(chunks) {
    const response = await fetch("/materialize/views/verify-chunks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        uploadId: this.uploadState.uploadId,
        chunks: Array.from(chunks),
      }),
    });

    if (!response.ok) {
      return [];
    }

    const { verifiedChunks } = await response.json();
    return verifiedChunks;
  }

  pauseUpload() {
    if (this.uploadState.status === "uploading") {
      this.uploadState.status = "paused";
      this.saveUploadState();
      this.updateUI();
    }
  }

  async resumeUpload() {
    if (this.uploadState.status === "paused") {
      this.uploadState.status = "uploading";
      this.updateUI();
      await this.processUpload();
    }
  }

  uploadHandler() {
    return this.startUpload();
  }
  async completeUpload() {
    try {
      const response = await fetch("/materialize/views/complete-upload", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          uploadId: this.uploadState.uploadId,
          fileName: this.file.name,
        }),
      });

      if (!response.ok) {
        throw new Error("Failed to complete upload");
      }

      this.uploadState.status = "completed";
      this.clearUploadState();
      this.dispatchEvent("uploadComplete", {
        fileName: this.file.name,
        uploadId: this.uploadState.uploadId,
      });
    } catch (error) {
      this.handleError("Failed to complete upload", error);
    }
  }

  saveUploadState() {
    const currentFileData = sessionManager.getStepData("fileSelection");
    sessionManager.saveStepData("fileSelection", {
      ...currentFileData,
      uploadState: {
        ...this.uploadState,
        uploadedChunks: Array.from(this.uploadState.uploadedChunks),
      },
    });
  }

  clearUploadState() {
    sessionStorage.removeItem(`uploadState_${this.uploadState.uploadId}`);
  }

  updateProgress() {
    const progress =
      (this.uploadState.uploadedChunks.size / this.uploadState.totalChunks) *
      100;
    const timeElapsed = Date.now() - this.uploadState.startTime;
    const uploadSpeed = this.calculateUploadSpeed(timeElapsed);
    const timeRemaining = this.estimateTimeRemaining(progress, uploadSpeed);

    const progressData = {
      progress,
      uploadSpeed,
      timeRemaining,
      uploadedChunks: this.uploadState.uploadedChunks.size,
      totalChunks: this.uploadState.totalChunks,
    };

    this.saveUploadState();
    this.dispatchEvent("uploadProgress", progressData);
    this.updateUI();
  }

  updateUI() {
    const progressBar = document.getElementById("progressBarFill");
    const progressText = document.getElementById("progressText");
    const uploadButton = document.getElementById("uploadButton");
    const pauseButton = document.getElementById("pauseUpload");
    const resumeButton = document.getElementById("resumeUpload");

    if (progressBar && progressText) {
      const progress =
        (this.uploadState.uploadedChunks.size / this.uploadState.totalChunks) *
        100;
      progressBar.style.width = `${progress}%`;
      progressText.textContent = this.getStatusText();
    }

    if (uploadButton) {
      uploadButton.disabled =
        this.uploadState.status === "uploading" ||
        this.uploadState.status === "completed";
    }
    if (pauseButton) {
      pauseButton.disabled = this.uploadState.status !== "uploading";
    }
    if (resumeButton) {
      resumeButton.disabled = this.uploadState.status !== "paused";
    }
  }

  getStatusText() {
    switch (this.uploadState.status) {
      case "uploading":
        return `Uploading: ${Math.round(
          (this.uploadState.uploadedChunks.size /
            this.uploadState.totalChunks) *
            100
        )}%`;
      case "paused":
        return "Upload paused";
      case "completed":
        return "Upload completed";
      case "error":
        return "Upload failed";
      default:
        return "Ready to upload";
    }
  }

  generateUploadId() {
    return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  calculateUploadSpeed(timeElapsed) {
    const uploadedBytes = this.uploadState.uploadedChunks.size * this.chunkSize;
    return uploadedBytes / (timeElapsed / 1000); // bytes per second
  }

  estimateTimeRemaining(progress, uploadSpeed) {
    if (progress === 0 || uploadSpeed === 0) return 0;
    const remainingBytes = this.file.size * (1 - progress / 100);
    return remainingBytes / uploadSpeed;
  }

  formatTime(seconds) {
    if (seconds < 60) {
      return `${Math.round(seconds)}s`;
    }
    const minutes = Math.floor(seconds / 60);
    return `${minutes}m ${Math.round(seconds % 60)}s`;
  }

  handleError(message, error) {
    console.error(message, error);
    this.dispatchEvent("uploadError", {
      message,
      error: error.message,
    });
  }

  dispatchEvent(eventName, detail) {
    const event = new CustomEvent(eventName, { detail });
    document.dispatchEvent(event);
  }
}

const fileHandler = new FileHandler();
export { fileHandler };
export const uploadHandler = () => fileHandler.uploadHandler();
