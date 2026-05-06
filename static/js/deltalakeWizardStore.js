document.addEventListener("alpine:init", () => {
  Alpine.store("dlWizard", {
    state: {
      currentStep: 1,
      totalSteps: 3,
      stepStatus: {
        1: { completed: false, valid: false },
        2: { completed: false, valid: false },
        3: { completed: false, valid: false },
      },
      // Step 1 data
      datastack: null,
      version: null,
      tableName: null,
      targetPartitionSizeMb: 256,
      outputBucket: "",
      // Step 2 data (populated by discovery)
      rowCount: null,
      bytesPerRow: null,
      availableColumns: [],
      bloomFilterFpp: 0.001,
      specs: [],
      // Export key for monitoring
      exportKey: null,
    },

    init() {
      this.loadState();
    },

    loadState() {
      const saved = localStorage.getItem("dlWizardState");
      if (saved) {
        try {
          const parsed = JSON.parse(saved);
          Object.assign(this.state, parsed);
        } catch (e) {
          console.warn("[dlWizard] Failed to parse saved state:", e);
        }
      }
    },

    saveState() {
      localStorage.setItem("dlWizardState", JSON.stringify(this.state));
    },

    clearState() {
      localStorage.removeItem("dlWizardState");
      this.state.currentStep = 1;
      this.state.stepStatus = {
        1: { completed: false, valid: false },
        2: { completed: false, valid: false },
        3: { completed: false, valid: false },
      };
      this.state.datastack = null;
      this.state.version = null;
      this.state.tableName = null;
      this.state.targetPartitionSizeMb = 256;
      this.state.rowCount = null;
      this.state.bytesPerRow = null;
      this.state.availableColumns = [];
      this.state.bloomFilterFpp = 0.001;
      this.state.specs = [];
      this.state.exportKey = null;
    },

    next() {
      if (this.state.currentStep < this.state.totalSteps) {
        this.state.stepStatus[this.state.currentStep].completed = true;
        this.state.currentStep++;
        this.saveState();
        window.location.href = `/materialize/deltalake/step${this.state.currentStep}`;
      }
    },

    prev() {
      if (this.state.currentStep > 1) {
        this.state.currentStep--;
        this.saveState();
        window.location.href = `/materialize/deltalake/step${this.state.currentStep}`;
      }
    },
  });
});
