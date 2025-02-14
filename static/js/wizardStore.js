document.addEventListener("alpine:init", () => {
  Alpine.store("wizard", {
    state: {
      currentStep: parseInt(localStorage.getItem("currentStep")) || 1,
      totalSteps: 4,
      stepStatus: {
        1: { completed: false, valid: false },
        2: { completed: false, valid: false },
        3: { completed: false, valid: false },
        4: { completed: false, valid: false },
      },
      registeredSteps: {},
    },

    init() {
      this.loadState();

      if (!this.state.registeredSteps) {
        this.state.registeredSteps = {};
      }

      this.registerStep(1, "upload");
      this.registerStep(2, "schema");
      this.registerStep(3, "metadata");
      this.registerStep(4, "processor");

      const currentPath = window.location.pathname;
      const urlStep = parseInt(currentPath.match(/step(\d+)/)?.[1]);

      if (urlStep && urlStep !== this.state.currentStep) {
        window.location.href = `/materialize/upload/step${this.state.currentStep}`;
      }
    },

    registerStep(stepNumber, storeName) {
      if (!this.state.registeredSteps) {
        this.state.registeredSteps = {};
      }
      this.state.registeredSteps[stepNumber] = storeName;
    },

    getCurrentStepStore() {
      return this.getStepStore(this.state.currentStep);
    },

    getStepStore(stepNumber) {
      if (!this.state.registeredSteps) {
        this.state.registeredSteps = {};
      }
      const storeName = this.state.registeredSteps[stepNumber];
      return storeName ? Alpine.store(storeName) : null;
    },

    getNavigationState() {
      const currentStore = this.getCurrentStepStore();
      const isStep4 = this.state.currentStep === 4;
      const processorStatus = isStep4 ? currentStore.state.status : null;

      return {
        back: {
          visible: this.state.currentStep > 1,
          disabled: isStep4 && processorStatus === "processing",
        },
        next: {
          text: this.getNextButtonText(),
          disabled:
            !currentStore.isValid() ||
            (isStep4 && processorStatus === "processing"),
        },
        reset: {
          disabled:
            isStep4 && ["preparing", "processing"].includes(processorStatus),
        },
      };
    },

    getNextButtonText() {
      switch (this.state.currentStep) {
        default:
          return "Next →";
      }
    },

    async handleNextAction() {
      const currentStore = this.getCurrentStepStore();

      try {
        const valid = await currentStore.handleNext();
        if (valid) {
          this.next();
        }
      } catch (error) {
        console.error("Error handling next action:", error);
      }
    },

    async handleResetAction() {
      const currentStore = this.getCurrentStepStore();
      if (currentStore && typeof currentStore.reset === "function") {
        await currentStore.reset();
        this.state.stepStatus[this.state.currentStep] = {
          completed: false,
          valid: false,
        };
        this.saveState();
      }
    },

    next() {
      const currentStore = this.getCurrentStepStore();

      if (currentStore && currentStore.isValid && currentStore.isValid()) {
        this.markStepComplete(this.state.currentStep);
        this.state.currentStep = Math.min(
          this.state.currentStep + 1,
          this.state.totalSteps
        );
        this.saveState();
        window.location.href = `/materialize/upload/step${this.state.currentStep}`;
      }
    },

    prev() {
      if (this.state.currentStep > 1) {
        this.state.currentStep--;
        this.saveState();
        window.location.href = `/materialize/upload/step${this.state.currentStep}`;
      }
    },

    markStepComplete(step) {
      if (this.state.stepStatus[step]) {
        this.state.stepStatus[step].completed = true;
        this.saveState();
      }
    },

    saveState() {
      localStorage.setItem("currentStep", this.state.currentStep);
      localStorage.setItem("wizardState", JSON.stringify(this.state));
    },

    loadState() {
      const savedState = localStorage.getItem("wizardState");
      if (savedState) {
        const parsedState = JSON.parse(savedState);
        if (!parsedState.registeredSteps) {
          parsedState.registeredSteps = {};
        }
        this.state = parsedState;
      }
    },
  });
});
