document.addEventListener("alpine:init", () => {
  Alpine.store("upload", {
    file: null,
    filename: "",
    status: "idle",
    progress: {
      percentage: 0,
      uploaded: 0,
      total: 0,
      currentChunk: 0,
    },
    error: null,
    uploadUrl: null,
    aborted: false,
    paused: false,
    savedState: null,
    previewRows: [],

    init() {
      const savedState = localStorage.getItem("uploadStore");
      if (savedState) {
        const state = JSON.parse(savedState);
        this.filename = state.filename;
        this.status = state.status;
        this.previewRows = state.previewRows;
      }
    },

    reset() {
      const fileInput = document.querySelector('input[type="file"]');
      if (fileInput) {
        fileInput.value = "";
      }
      this.file = null;
      this.filename = "";
      this.status = "idle";
      this.progress = {
        percentage: 0,
        uploaded: 0,
        total: 0,
        currentChunk: 0,
      };
      this.error = null;
      this.uploadUrl = null;
      this.aborted = false;
      this.paused = false;
      this.savedState = null;
      this.previewRows = [];
      this.clearAllStates();
    },

    saveState() {
      const stateToSave = {
        filename: this.filename,
        status: this.status,
        previewRows: this.previewRows,
      };
      localStorage.setItem("uploadStore", JSON.stringify(stateToSave));
    },

    saveProgress() {
      this.savedState = {
        uploaded: this.progress.uploaded,
        currentChunk: this.progress.currentChunk,
        percentage: this.progress.percentage,
        total: this.progress.total,
      };
    },

    clearAllStates() {
      localStorage.removeItem("uploadStore");
      localStorage.removeItem("schemaStore");

      const schemaStore = Alpine.store("schema");
      if (schemaStore) {
        schemaStore.reset();
      }
    },

    handleFileSelect(event) {
      const file = event.target.files[0];
      if (!file) return;

      console.log("File selected:", file);

      this.file = file;
      this.filename = file.name;
      this.status = "ready";
      this.progress.total = file.size;

      this.error = null;
      this.previewCSV(file);
      this.prepareUpload();
    },

    previewCSV(file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        this.previewRows = text
          .split("\n")
          .filter((line) => line.trim())
          .slice(0, 2)
          .map((line) => line.split(",").map((cell) => cell.trim()));
        this.saveState();

        const schemaStore = Alpine.store("schema");
        if (schemaStore) {
          schemaStore.loadCsvColumns();
        }
      };
      reader.readAsText(file.slice(0, 1024));
    },

    parseCSVLine(line) {
      const result = [];
      let inQuotes = false;
      let currentField = "";

      for (let i = 0; i < line.length; i++) {
        const char = line[i];

        if (char === '"') {
          inQuotes = !inQuotes;
        } else if (char === "," && !inQuotes) {
          result.push(currentField.trim());
          currentField = "";
        } else {
          currentField += char;
        }
      }
      result.push(currentField.trim());

      return result;
    },

    async prepareUpload() {
      try {
        const response = await fetch("/materialize/upload/generate-presigned-url", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            filename: this.filename,
            contentType: this.file.type,
            fileSize: this.file.size,
          }),
        });

        if (!response.ok) throw new Error("Failed to get upload URL");

        const data = await response.json();
        this.uploadUrl = data.resumableUrl;
        console.log("Presigned URL received:", this.uploadUrl);
      } catch (error) {
        console.error("Prepare error:", error);
        this.error = error.message;
        this.status = "error";
      }
    },

    async uploadChunk(chunk, start, end) {
      const maxRetries = 3;
      let attempt = 0;

      while (attempt < maxRetries) {
        try {
          const response = await fetch(this.uploadUrl, {
            method: "PUT",
            headers: {
              "Content-Range": `bytes ${start}-${end - 1}/${this.file.size}`,
              "Content-Type": "application/octet-stream",
            },
            body: chunk,
          });

          if (response.ok || response.status === 308) {
            return;
          }

          throw new Error(`Chunk upload failed: ${response.status}`);
        } catch (error) {
          console.warn(
            `Retrying chunk upload (${attempt + 1}/${maxRetries})...`
          );
          attempt++;
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }

      throw new Error("Max retries reached for chunk upload");
    },

    async startUpload() {
      if (!this.file || !this.uploadUrl) return;

      const CHUNK_SIZE = 5 * 1024 * 1024;
      const totalChunks = Math.ceil(this.file.size / CHUNK_SIZE);

      if (this.status === "paused" && this.savedState) {
        Object.assign(this.progress, this.savedState);
      } else {
        this.progress = {
          uploaded: 0,
          currentChunk: 0,
          percentage: 0,
          total: this.file.size,
        };
      }

      console.log("Upload starting from:", {
        chunk: this.progress.currentChunk,
        bytes: this.progress.uploaded,
        percentage: this.progress.percentage,
      });

      let startChunk = this.progress.currentChunk;
      let uploadedBytes = this.progress.uploaded;

      console.log("Upload starting from:", {
        chunk: startChunk,
        bytes: uploadedBytes,
        percentage: this.progress.percentage,
      });

      this.aborted = false;
      this.paused = false;
      this.status = "uploading";

      try {
        for (let i = this.progress.currentChunk; i < totalChunks; i++) {
          if (this.paused || this.aborted) {
            return;
          }

          const start = i * CHUNK_SIZE;
          const end = Math.min(start + CHUNK_SIZE, this.file.size);
          const chunk = this.file.slice(start, end);

          await this.uploadChunk(chunk, start, end);

          this.progress.uploaded += end - start;
          this.progress.currentChunk = i;
          this.progress.percentage = Math.round(
            (this.progress.uploaded / this.file.size) * 100
          );
        }

        if (!this.aborted && !this.paused) {
          this.status = "completed";
          this.savedState = null;
        }
      } catch (error) {
        console.error("Upload error:", error);
        this.error = error.message;
        this.status = "error";
      }
    },

    pause() {
      if (this.status === "uploading") {
        this.paused = true;
        this.status = "paused";
        this.saveProgress();
        console.log("Upload paused at:", this.savedState);
      }
    },

    async resume() {
      if (this.status !== "paused" || !this.savedState) {
        console.error("Invalid resume state");
        return;
      }

      console.log("Resuming from:", this.savedState);
      await this.startUpload();
    },

    cancel() {
      this.aborted = true;
      this.status = "cancelling";
      this.savedState = null;
      this.reset();
      console.log("Upload cancelled");
    },

    isValid() {
      const valid = this.status === "completed";
      if (valid) {
        this.saveState();
      }
      return valid;
    },

    canStartUpload() {
      return (
        this.file && this.status === "ready" && this.uploadUrl && !this.error
      );
    },
  });
});
