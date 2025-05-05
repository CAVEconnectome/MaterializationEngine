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
    speedStats: {
      startTime: null,
      lastUpdateTime: null,
      lastUploadedBytes: 0,
      uploadSpeed: 0,
    },

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
      this.speedStats = {
        startTime: null,
        lastUpdateTime: null,
        lastUploadedBytes: 0,
        uploadSpeed: 0,
      };
      const datastackSelect = document.getElementById('datastackSelect');
      if (datastackSelect) {
          datastackSelect.value = "";
      }
      this.clearAllStates();

      return this.isValid();
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
      localStorage.removeItem("metadataStore");
      localStorage.removeItem("processorStore");
      localStorage.removeItem("wizardState");
      localStorage.removeItem("currentStep");

      const schemaStore = Alpine.store("schema");
      if (schemaStore) {
        schemaStore.reset();
      }

      const metadataStore = Alpine.store("metadata");
      if (metadataStore) {
        metadataStore.reset();
      }

      const processorStore = Alpine.store("processor");
      if (processorStore) {
        processorStore.reset();
      }

      const wizardStore = Alpine.store("wizard");
      if (wizardStore) {
        wizardStore.resetState();
      }
    },

    handleFileSelect(event) {
      const file = event.target.files[0];
      if (!file) return;

      console.log("File selected:", file);

      this.file = file;
      this.filename = file.name;
      this.status = "preparing";
      this.progress = { percentage: 0, uploaded: 0, total: file.size, currentChunk: 0 };
      this.error = null;
      this.uploadUrl = null;
      this.aborted = false;
      this.paused = false;
      this.savedState = null;
      this.previewRows = []; 

      this.previewCSV(file); 

      this.prepareUpload()
      .then(() => {
          if (this.uploadUrl) {
              this.status = "ready";
              console.log("Ready to upload.");
          } else {
              console.log("Preparation failed, cannot proceed to upload.");
          }
      })
      .catch(err => {
          console.error("Error during upload preparation:", err);
      });
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
      this.status = "preparing";
      this.error = null;
      this.uploadUrl = null;

      
      const datastackSelect = document.getElementById('datastackSelect');
      const datastackName = datastackSelect ? datastackSelect.value : null;

      if (!datastackName) {
           const errMsg = "Datastack must be selected before preparing upload.";
           console.error(errMsg);
           this.error = errMsg;
           this.status = "error"; 
           return; 
      }
      
      if (!this.file) {
          const errMsg = "No file selected for upload preparation.";
          console.error(errMsg);
          this.error = errMsg;
          this.status = "error";
          return;
      }

      const apiUrl = `/materialize/upload/generate-presigned-url/${encodeURIComponent(datastackName)}`;
      console.log("Requesting presigned URL from:", apiUrl);

      try {
        const response = await fetch(apiUrl, { 
          method: "POST",
          headers: {
              "Content-Type": "application/json",
              "X-Requested-With": "XMLHttpRequest"
          },
          body: JSON.stringify({
            filename: this.filename, 
            contentType: this.file.type || 'application/octet-stream',
            fileSize: this.file.size, 
          }),
          credentials: 'same-origin' 
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error(`Failed response (${response.status}):`, errorText);
            throw new Error(`Failed to get upload URL (${response.status})`);
        }

        const data = await response.json();
        if (!data.resumableUrl) {
            throw new Error("Server response missing 'resumableUrl'.");
        }
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
      const datastackSelect = document.getElementById('datastackSelect');
      if (!datastackSelect || !datastackSelect.value) {
          this.error = "Please select a datastack before uploading.";
          this.status = "error";
          return;
      }

      if (!this.file) {
          this.error = "No file selected.";
          this.status = "error";
          return;
      }
      if (!this.uploadUrl) {
          this.error = "Upload URL not ready. Please re-select the file.";
          this.status = "error";
          return;
      }
      if (this.status === 'uploading') {
          console.warn("Upload already in progress.");
          return;
      }

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
        this.speedStats.startTime = Date.now();
        this.speedStats.lastUpdateTime = Date.now();
        this.speedStats.lastUploadedBytes = 0;
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
      this.error = null; 

      if (this.status !== "paused" || !this.savedState) {
        this.speedStats.startTime = Date.now();
        this.speedStats.lastUpdateTime = Date.now();
        this.speedStats.lastUploadedBytes = 0;
        this.progress = {
            uploaded: 0,
            currentChunk: 0,
            percentage: 0,
            total: this.file.size,
        };
    } else {
        Object.assign(this.progress, this.savedState);
        console.log("Resuming upload state:", this.progress);
    }

    try {
      for (let i = this.progress.currentChunk; i < Math.ceil(this.file.size / (5 * 1024 * 1024)); i++) { // Use CHUNK_SIZE constant if defined elsewhere
        if (this.paused || this.aborted) {
          console.log(`Upload loop interrupted. Paused: ${this.paused}, Aborted: ${this.aborted}`);
          if (this.paused) {
              this.saveProgress();
              console.log("Upload paused at:", this.savedState);
          }
          return;
        }

        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, this.file.size);
        const chunk = this.file.slice(start, end);

        await this.uploadChunk(chunk, start, end);

        // Ensure progress update happens correctly
        const uploadedInChunk = end - start;
        this.progress.uploaded += uploadedInChunk;
        this.progress.currentChunk = i + 1; // Next chunk to upload is i+1
        this.progress.percentage = Math.min(100, Math.round( // Cap at 100
          (this.progress.uploaded / this.file.size) * 100
        ));

        this.updateUploadSpeed();
      }

      // If loop completes without interruption
      if (!this.aborted && !this.paused) {
        this.status = "completed";
        this.savedState = null; 
        console.log("Upload completed successfully.");
        this.saveState();
      }
    } catch (error) {
      console.error("Upload error:", error);
      this.error = error.message;
      this.status = "error";
    }
  },

    updateUploadSpeed() {
      const now = Date.now();
      const timeElapsed = (now - this.speedStats.lastUpdateTime) / 1000;

      if (timeElapsed > 0.5) {
        const bytesUploaded =
          this.progress.uploaded - this.speedStats.lastUploadedBytes;
        this.speedStats.uploadSpeed = bytesUploaded / timeElapsed;

        this.speedStats.lastUpdateTime = now;
        this.speedStats.lastUploadedBytes = this.progress.uploaded;
      }
    },

    formatSpeed() {
      const speed = this.speedStats.uploadSpeed;

      if (speed < 1024) {
        return `${speed.toFixed(1)} B/s`;
      } else if (speed < 1024 * 1024) {
        return `${(speed / 1024).toFixed(1)} KB/s`;
      } else {
        return `${(speed / (1024 * 1024)).toFixed(1)} MB/s`;
      }
    },

    getEstimatedTimeRemaining() {
      if (this.speedStats.uploadSpeed <= 0) return "Calculating...";

      const bytesRemaining = this.progress.total - this.progress.uploaded;
      const secondsRemaining = bytesRemaining / this.speedStats.uploadSpeed;

      if (secondsRemaining < 60) {
        return `${Math.round(secondsRemaining)} seconds`;
      } else if (secondsRemaining < 3600) {
        return `${Math.floor(secondsRemaining / 60)}m ${Math.round(
          secondsRemaining % 60
        )}s`;
      } else {
        const hours = Math.floor(secondsRemaining / 3600);
        const minutes = Math.floor((secondsRemaining % 3600) / 60);
        return `${hours}h ${minutes}m`;
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
      if (this.status !== "paused") {
        console.error("Cannot resume: Upload not paused.");
        return;
      }
       if (!this.savedState) {
        console.error("Cannot resume: No saved state found.");
        this.status = 'error';
        this.error = 'Cannot resume upload, state lost.';
        return;
      }

      console.log("Resuming from:", this.savedState);
      this.paused = false; 
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
      const valid =
        this.file !== null && this.status === "completed" && !this.error;
      if (valid) {
        this.saveState();
      }
      return valid;
    },

    async handleNext() {
      return this.isValid();
    },

    canStartUpload() {
      const datastackSelect = document.getElementById('datastackSelect');
      const datastackSelected = datastackSelect && datastackSelect.value;

      return (
        this.file &&
        datastackSelected &&
        this.status === "ready" && 
        this.uploadUrl &&
        !this.error
      );
    },
  });
});
