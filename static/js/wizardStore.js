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
        window.location.href = `/step${this.state.currentStep}`;
      }
    },

    registerStep(stepNumber, storeName) {
      if (!this.state.registeredSteps) {
        this.state.registeredSteps = {};
      }
      this.state.registeredSteps[stepNumber] = storeName;
    },

    getStepStore(stepNumber) {
      if (!this.state.registeredSteps) {
        this.state.registeredSteps = {};
      }
      const storeName = this.state.registeredSteps[stepNumber];
      return storeName ? Alpine.store(storeName) : null;
    },

    next() {
      const currentStore = this.getStepStore(this.state.currentStep);

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

    goToStep(step) {
      if (
        step >= 1 &&
        step <= this.state.totalSteps &&
        this.canGoToStep(step)
      ) {
        this.state.currentStep = step;
        this.saveState();
        window.location.href = `/materialize/upload/step${step}`;
      }
    },

    markStepComplete(step) {
      if (this.state.stepStatus[step]) {
        this.state.stepStatus[step].completed = true;
        this.saveState();
      }
    },

    markStepValid(step, isValid) {
      if (this.state.stepStatus[step]) {
        this.state.stepStatus[step].valid = isValid;
        this.saveState();
      }
    },

    canProgress() {
      const stepStore = this.getStepStore(this.state.currentStep);
      return stepStore && stepStore.isValid ? stepStore.isValid() : false;
    },

    canGoToStep(targetStep) {
      if (targetStep < this.state.currentStep) return true;
      if (targetStep > this.state.currentStep + 1) return false;
      return this.canProgress();
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

    resetStep(step) {
      const stepStore = this.getStepStore(step);
      if (stepStore && typeof stepStore.reset === "function") {
        stepStore.reset();
        this.state.stepStatus[step] = { completed: false, valid: false };
        this.saveState();
      }
    },

    resetWizard() {
      const registeredSteps = { ...this.state.registeredSteps };

      for (let i = 1; i <= this.state.totalSteps; i++) {
        this.resetStep(i);
      }

      this.state = {
        currentStep: 1,
        totalSteps: 3,
        stepStatus: {
          1: { completed: false, valid: false },
          2: { completed: false, valid: false },
          3: { completed: false, valid: false },
          4: { completed: false, valid: false },
        },
        registeredSteps: registeredSteps,
      };

      this.saveState();
      window.location.href = "/materialize/upload/step1";
    },
  });
});
