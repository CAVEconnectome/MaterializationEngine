document.addEventListener("alpine:init", () => {
    Alpine.store("processor", {
      state: {
        status: "idle",
        taskId: null,
        progress: 0,
        processedRows: 0,
        totalRows: 0,
        currentChunk: 0,
        error: null,
        inputFile: "",
        outputFile: "",
        processingTime: "",
        startTime: null,
        processingStatus: null,
        pollTimer: null,
      },
  
      init() {
        this.loadState();
        this.checkProcessingStatus();
      },
  
      loadState() {
        const savedState = localStorage.getItem("processorStore");
        if (savedState) {
          const state = JSON.parse(savedState);
          state.status = state.status === "processing" ? "idle" : state.status;
          Object.assign(this.state, state);
        }
      },
  
      saveState() {
        const stateToSave = { ...this.state };
        delete stateToSave.pollTimer;
        localStorage.setItem("processorStore", JSON.stringify(stateToSave));
      },
  
      async startProcessing() {
        try {
          const uploadStore = localStorage.getItem("uploadStore");
          const schemaStore = localStorage.getItem("schemaStore");
          const metadataStore = localStorage.getItem("metadataStore");
  
          if (!uploadStore || !schemaStore || !metadataStore) {
            throw new Error("Missing required data from previous steps");
          }
  
          const { filename } = JSON.parse(uploadStore);
          const { columnMapping, ignoredColumns } = JSON.parse(schemaStore);
          const metadata = JSON.parse(metadataStore);
  
          this.state.status = "preparing";
          this.state.startTime = Date.now();
          this.state.inputFile = filename;
          this.saveState();
          console.log("Starting processing...");
  
          console.log("filename", filename);
          console.log("columnMapping", columnMapping);
          console.log("ignoredColumns", ignoredColumns);
          console.log("metadata", metadata);
          const response = await fetch("/materialize/upload/api/process/start", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              filename,
              columnMapping,
              ignoredColumns,
              metadata,
            }),
          });
  
          const data = await response.json();
          this.state.processingStatus = data.status;
          this.state.taskId = data.task_id;
          this.state.status = "processing";
          this.startPolling();
          this.saveState();
  
        } catch (error) {
          console.error("Processing error:", error);
          this.state.error = error.message;
          this.state.status = "error";
          this.saveState();
        }
      },
  
      async checkProcessingStatus() {
        if (!this.state.processingStatus) return;
  
        try {
          const response = await fetch(`/materialize/upload/api/process/status/${this.state.taskId}`);
          if (!response.ok) throw new Error("Failed to get processing status");
  
          const data = await response.json();
          this.updateProgress(data);
  
        } catch (error) {
          console.error("Status check error:", error);
          this.state.error = error.message;
          this.state.status = "error";
          this.stopPolling();
        }
  
        this.saveState();
      },
  
      startPolling() {
        if (!this.state.pollTimer) {
          this.state.pollTimer = setInterval(() => {
            this.checkProcessingStatus();
          }, 1000);
        }
      },
  
      stopPolling() {
        if (this.state.pollTimer) {
          clearInterval(this.state.pollTimer);
          this.state.pollTimer = null;
        }
      },
  
      updateProgress(data) {
        this.state.progress = data.progress;
        this.state.processedRows = data.processed_rows;
        this.state.totalRows = data.total_rows;
        this.state.currentChunk = data.current_chunk;
  
        if (data.status === "completed") {
          this.handleCompletion(data);
        } else if (data.status === "error") {
          this.handleError(data);
        }
      },
  
      handleCompletion(data) {
        this.state.status = "completed";
        this.state.outputFile = data.output_file;
        this.stopPolling();
  
        const endTime = Date.now();
        const duration = endTime - this.state.startTime;
        const minutes = Math.floor(duration / 60000);
        const seconds = ((duration % 60000) / 1000).toFixed(1);
        this.state.processingTime = `${minutes}m ${seconds}s`;
      },
  
      handleError(data) {
        this.state.status = "error";
        this.state.error = data.error;
        this.stopPolling();
      },
  
      reset() {
        this.stopPolling();
        this.state = {
          status: "idle",
          taskId: null,
          progress: 0,
          processedRows: 0,
          totalRows: 0,
          currentChunk: 0,
          error: null,
          inputFile: "",
          outputFile: "",
          processingTime: "",
          startTime: null,
          processingStatus: null,
          pollTimer: null,
        };
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
      }
    });
  });