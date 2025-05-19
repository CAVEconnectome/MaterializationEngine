document.addEventListener("alpine:init", () => {
  Alpine.store("processor", {
    state: {
      status: "idle",
      phase: "Waiting to start...", 
      taskId: null,
      progress: 0,
      processedRows: 0,
      totalRows: 0,
      currentChunkNum: 0,
      totalChunks: 0,
      error: null,
      inputFile: "", 
      outputFile: "", 
      processingTime: "",
      startTime: null,
      pollTimer: null,
      lastMessage: "",
    },

    init() {
      this.loadState();
      if (
        this.state.taskId &&
        (this.state.status === "processing" ||
          this.state.status === "preparing")
      ) {
        this.state.status = "processing";
        this.state.phase = this.state.phase || "Resuming status check...";
        this.startPolling();
      } else if (
        this.state.status === "completed" ||
        this.state.status === "error" ||
        this.state.status === "cancelled"
      ) {
      } else {
        this.resetInternalState();
      }
      this.updateInputFileFromUploadStore();
    },

    updateInputFileFromUploadStore() {
      const uploadStoreData = localStorage.getItem("uploadStore");
      if (uploadStoreData) {
        try {
          const uploadStore = JSON.parse(uploadStoreData);
          if (uploadStore.filename) {
            this.state.inputFile = uploadStore.filename;
          }
        } catch (e) {
          console.error("Error parsing uploadStore for input file:", e);
        }
      }
    },

    loadState() {
      const savedState = localStorage.getItem("processorStore");
      if (savedState) {
        try {
          const state = JSON.parse(savedState);
          if (
            state.status === "completed" ||
            state.status === "error" ||
            state.status === "cancelled"
          ) {
            Object.assign(this.state, state);
            this.stopPolling();
          } else if (state.taskId) {
            Object.assign(this.state, state);
          } else {
            this.resetInternalState();
          }
        } catch (e) {
          console.error("Error loading processor state from localStorage:", e);
          this.resetInternalState();
        }
      } else {
        this.resetInternalState();
      }
    },

    saveState() {
      const stateToSave = { ...this.state };
      delete stateToSave.pollTimer;
      localStorage.setItem("processorStore", JSON.stringify(stateToSave));
    },

    resetInternalState(keepTaskId = false) {
      const taskIdToKeep = keepTaskId ? this.state.taskId : null;
      this.state.status = "idle";
      this.state.phase = "Waiting to start...";
      this.state.taskId = taskIdToKeep;
      this.state.progress = 0;
      this.state.processedRows = 0;
      this.state.totalRows = 0;
      this.state.currentChunkNum = 0;
      this.state.totalChunks = 0;
      this.state.error = null;
      this.state.outputFile = "";
      this.state.processingTime = "";
      this.state.startTime = null;
      this.state.lastMessage = "";
    },

    async startProcessing() {
      this.resetInternalState(false);
      this.updateInputFileFromUploadStore();

      try {
        const uploadStoreData = localStorage.getItem("uploadStore");
        const schemaStoreData = localStorage.getItem("schemaStore");
        const metadataStoreData = localStorage.getItem("metadataStore"); 

        if (!uploadStoreData || !schemaStoreData || !metadataStoreData) {
          throw new Error(
            "Missing required data from previous steps. Please revisit earlier steps."
          );
        }

        const { filename } = JSON.parse(uploadStoreData);
        this.state.inputFile = filename || this.state.inputFile;

        const { columnMapping, ignoredColumns, selectedSchema } =
          JSON.parse(schemaStoreData);
        const metadataFromStore = JSON.parse(metadataStoreData); 

        const metadataPayload = {
          ...metadataFromStore,
          schema_type: metadataFromStore.schema_type || selectedSchema,
        };
       
        if (!this.state.inputFile) {
          throw new Error(
            "Input file name is missing. Please re-upload the file in Step 1."
          );
        }

        this.state.status = "preparing";
        this.state.phase = "Initializing processing workflow...";
        this.state.startTime = Date.now();
        this.saveState();
        console.log("Starting processing for file:", this.state.inputFile);
        console.log("Metadata payload being sent:", metadataPayload);

        const response = await fetch("/materialize/upload/api/process/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            filename: this.state.inputFile,
            columnMapping,
            ignoredColumns,
            metadata: metadataPayload,
          }),
        });

        if (!response.ok) {
          let errorText = "Failed to start processing.";
          try {
            const errorData = await response.json();
            errorText =
              errorData.message || errorData.error || JSON.stringify(errorData);
          } catch (e) {
            errorText = (await response.text()) || errorText;
          }
          throw new Error(`Server error (${response.status}): ${errorText}`);
        }

        const data = await response.json();
        if (!data.task_id) {
          throw new Error("Server did not return a task ID.");
        }
        this.state.taskId = data.task_id;
        this.state.status = "processing";
        this.state.phase =
          "Workflow initiated, awaiting first status update...";
        this.saveState();

        Alpine.store("wizard").resetState(); 
        window.location.href = "/materialize/upload/running-uploads";

      } catch (error) {
        console.error("Error starting processing:", error);
        this.state.error = error.message;
        this.state.status = "error";
        this.state.phase = "Error during initialization";
        this.stopPolling();
        this.saveState();
      }
    },

    async checkProcessingStatus() {
      if (
        !this.state.taskId ||
        this.state.status === "idle" ||
        this.state.status === "completed" ||
        this.state.status === "error" ||
        this.state.status === "cancelled"
      ) {
        this.stopPolling();
        return;
      }

      try {
        const response = await fetch(
          `/materialize/upload/api/process/status/${this.state.taskId}`
        );
        if (!response.ok) {
          if (response.status === 404)
            throw new Error(`Job ID ${this.state.taskId} not found.`);
          throw new Error(
            `Failed to get processing status (${response.status})`
          );
        }

        const data = await response.json();
        if (data) {
          this.updateProgress(data);
        } else {
          console.warn(
            "Received empty but successful status response. Waiting for next update."
          );
        }
      } catch (error) {
        console.error("Status check error:", error);
        this.state.error = `Status check failed: ${error.message}. Please check logs or try again if the issue persists.`;
        if (error.message.includes("not found")) {
          this.state.status = "error";
          this.state.phase = "Job not found";
          this.stopPolling();
        }
      }
      this.saveState();
    },

    startPolling() {
      this.stopPolling();
      if (this.state.taskId) {
        this.state.pollTimer = setInterval(() => {
          this.checkProcessingStatus();
        }, 2000);
        console.log("Polling started for task:", this.state.taskId);
      }
    },

    stopPolling() {
      if (this.state.pollTimer) {
        clearInterval(this.state.pollTimer);
        this.state.pollTimer = null;
        console.log("Polling stopped.");
      }
    },

    updateProgress(data) {
      this.state.status = data.status || this.state.status;
      this.state.phase = data.phase || this.state.phase || "Processing...";
      this.state.progress =
        data.progress !== undefined
          ? parseFloat(data.progress) || 0
          : this.state.progress;
      this.state.processedRows =
        data.processed_rows !== undefined
          ? parseInt(data.processed_rows) || 0
          : this.state.processedRows;
      this.state.totalRows =
        data.total_rows !== undefined
          ? data.total_rows === "N/A" || data.total_rows === "Calculating..."
            ? data.total_rows
            : parseInt(data.total_rows) || 0
          : this.state.totalRows;
      this.state.currentChunkNum =
        data.current_chunk_num !== undefined
          ? parseInt(data.current_chunk_num) || 0
          : this.state.currentChunkNum;
      this.state.totalChunks =
        data.total_chunks !== undefined
          ? data.total_chunks === "Calculating..."
            ? data.total_chunks
            : parseInt(data.total_chunks) || 0
          : this.state.totalChunks;
      this.state.error = data.error || null;
      this.state.lastMessage = data.message || this.state.lastMessage;

      if (this.state.status === "completed") {
        this.handleCompletion(data);
      } else if (
        this.state.status === "error" ||
        this.state.status === "cancelled"
      ) {
        this.handleErrorOrCancel(data);
      } else if (
        this.state.status === "processing" ||
        this.state.status === "pending" ||
        this.state.status === "preparing"
      ) {
        if (!this.state.pollTimer) this.startPolling();
      } else {
        console.warn(
          "Unknown job status received:",
          this.state.status,
          "Stopping polling."
        );
        this.stopPolling();
      }
    },

    handleCompletion(data) {
      this.state.phase = data.phase || "Processing Completed Successfully!";
      this.state.outputFile = data.output_file || this.state.outputFile;
      this.state.progress = 100;
      this.stopPolling();

      if (this.state.startTime) {
        const endTime = Date.now();
        const duration = endTime - this.state.startTime;
        const minutes = Math.floor(duration / 60000);
        const seconds = ((duration % 60000) / 1000).toFixed(1);
        this.state.processingTime = `${minutes}m ${seconds}s`;
      } else {
        this.state.processingTime = "N/A";
      }
      console.log("Processing completed:", data);
    },

    handleErrorOrCancel(data) {
      this.state.phase =
        data.phase ||
        (this.state.status === "error"
          ? "Processing Error"
          : "Processing Cancelled");
      this.state.error =
        data.error ||
        this.state.error ||
        "An unspecified error occurred or the job was cancelled.";
      this.stopPolling();
      console.log(`Job ${this.state.status}:`, data);
    },

    reset() {
      this.stopPolling();
      this.resetInternalState(false);
      this.updateInputFileFromUploadStore();
      localStorage.removeItem("processorStore");
      this.saveState();
    },

    isValid() {
      return this.state.status === "completed" && !this.state.error;
    },

    async handleNext() {
      if (this.isValid()) {
        this.stopPolling();
        return true;
      }
      return false;
    },
  });
});
