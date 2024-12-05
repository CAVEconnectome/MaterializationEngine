class SessionManager {
  constructor() {
    this.state = {
      currentStep: 0,
      totalSteps: 5,
      steps: {
        fileSelection: { complete: false, data: null },
        schemaSelection: { complete: false, data: null },
        metadataForm: { complete: false, data: null },
        upload: { complete: false, data: null },
        databaseSelection: { complete: false, data: null },
      },
      uploadState: {
        uploadId: null,
        progress: 0,
        status: "idle", // idle, uploading, paused, completed, error
        error: null,
        chunks: {
          total: 0,
          uploaded: 0,
          failed: [],
        },
      },
    };

    document.addEventListener("alpine:init", () => {
      Alpine.store("session", this.state);
      this.restoreSession();
    });

    this.setupEventListeners();
  }

  setupEventListeners() {
    window.addEventListener("beforeunload", () => this.saveSession());

    document.addEventListener("stepChange", (event) => {
      this.setStep(event.detail.step);
    });

    document.addEventListener("uploadProgress", (event) => {
      this.updateUploadState(event.detail);
    });
  }

  setStep(step) {
    if (typeof step === "number" && step >= 0 && step < this.state.totalSteps) {
      this.state.currentStep = step;
      window.Alpine.store("session", this.state);
      this.saveSession();
    }
  }

  getStepData(step) {
    return this.state.steps[step]?.data || null;
  }

  saveStepData(step, data) {
    if (this.state.steps[step]) {
      this.state.steps[step] = {
        complete: true,
        data: data,
      };
      window.Alpine.store("session", this.state);
      this.saveSession();
    }
  }

  updateUploadState(uploadInfo) {
    this.state.uploadState = {
      ...this.state.uploadState,
      ...uploadInfo,
    };
    window.Alpine.store("session", this.state);
    this.saveSession();
    this.notifyUploadStateChange();
  }

  notifyUploadStateChange() {
    document.dispatchEvent(
      new CustomEvent("uploadStateChange", {
        detail: this.state.uploadState,
      })
    );
  }

  async saveSession() {
    try {
      const sessionData = {
        ...this.state,
        lastUpdated: new Date().toISOString(),
      };

      sessionStorage.setItem("wizardSession", JSON.stringify(sessionData));

      try {
        const response = await fetch('/materialize/views/save-session', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(sessionData),
        });


        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
      } catch (serverError) {
        console.warn(
          "Failed to save session to server (continuing with local storage):",
          serverError
        );
      }
    } catch (error) {
      console.error("Error saving session:", error);
      throw error;
    }
  }

  restoreSession() {
    try {
      const savedSession = sessionStorage.getItem("wizardSession");
      if (savedSession) {
        const sessionData = JSON.parse(savedSession);
        this.state = {
          ...this.state,
          ...sessionData,
        };
        window.Alpine.store("session", this.state);
      }
    } catch (error) {
      console.error("Error restoring session:", error);
    }
  }

  clearSession() {
    sessionStorage.removeItem("wizardSession");
    this.state = {
      currentStep: 0,
      totalSteps: 5,
      steps: {
        fileSelection: { complete: false, data: null },
        schemaSelection: { complete: false, data: null },
        metadataForm: { complete: false, data: null },
        upload: { complete: false, data: null },
        databaseSelection: { complete: false, data: null },
      },
      uploadState: {
        uploadId: null,
        progress: 0,
        status: "idle",
        error: null,
        chunks: { total: 0, uploaded: 0, failed: [] },
      },
    };
    window.Alpine.store("session", this.state);
  }

  validateStep(step) {
    const stepData = this.state.steps[step]?.data;
    if (!stepData) return false;

    return true;
  }
}

export const sessionManager = new SessionManager();
