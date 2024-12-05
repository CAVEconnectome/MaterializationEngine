import { wizardNavigation } from "./wizard-navigation.js";
import { fileHandler } from "./file-handler.js";
import { schemaHandler } from "./schema-handler.js";
import { metadataForm } from "./metadata-form.js";
import { databaseHandler } from "./database-handler.js";
import { sessionManager } from "./session-manager.js";

class WizardManager {
  constructor() {
    document.addEventListener("alpine:init", () => {
      this.initializeManager();
    });
  }

  initializeManager() {
    this.steps = {
      FILE_SELECTION: 0,
      SCHEMA_SELECTION: 1,
      METADATA_FORM: 2,
      UPLOAD: 3,
      DATABASE_SELECTION: 4,
    };

    this.handlers = {
      [this.steps.FILE_SELECTION]: fileHandler,
      [this.steps.SCHEMA_SELECTION]: schemaHandler,
      [this.steps.METADATA_FORM]: metadataForm,
      [this.steps.UPLOAD]: fileHandler,
      [this.steps.DATABASE_SELECTION]: databaseHandler,
    };

    console.log("Initializing WizardManager...");
    this.setupEventListeners();
    this.validateComponents();
    this.restoreState();
  }

  validateComponents() {
    if (!this.handlers[this.steps.FILE_SELECTION]) {
      console.error("FileHandler not initialized");
    }
    if (!this.handlers[this.steps.SCHEMA_SELECTION]) {
      console.error("SchemaHandler not initialized");
    }
    if (!this.handlers[this.steps.METADATA_FORM]) {
      console.error("MetadataForm not initialized");
    }
    if (!this.handlers[this.steps.DATABASE_SELECTION]) {
      console.error("DatabaseHandler not initialized");
    }
  }

  setupEventListeners() {
    // File Selection Events
    document.addEventListener("fileSelected", (event) => {
      this.handleStepComplete(this.steps.FILE_SELECTION, event.detail);
    });

    // Schema Selection Events
    document.addEventListener("schemaSelected", (event) => {
      this.handleStepComplete(this.steps.SCHEMA_SELECTION, event.detail);
    });

    // Metadata Form Events
    document.addEventListener("metadataSubmitted", (event) => {
      this.handleStepComplete(this.steps.METADATA_FORM, event.detail);
    });

    // Upload Events
    document.addEventListener("uploadComplete", (event) => {
      this.handleStepComplete(this.steps.UPLOAD, event.detail);
    });

    // Database Selection Events
    document.addEventListener("databaseSelectionComplete", (event) => {
      this.handleStepComplete(this.steps.DATABASE_SELECTION, event.detail);
    });

    // Error Events
    document.addEventListener("wizardError", (event) => {
      this.handleError(event.detail);
    });
  }
  getCurrentStep() {
    return Alpine.store("session")?.currentStep || 0;
  }

  async handleStepComplete(step, data) {
    try {
      console.log(`Step ${step} completed:`, {
        step,
        dataPresent: !!data,
        currentStep: this.getCurrentStep(),
      });

      if (sessionManager) {
        sessionManager.saveStepData(this.getStepKey(step), data);
      }

      if (await this.validateStep(step)) {
        if (step < Object.keys(this.steps).length - 1) {
          wizardNavigation.enableNext();
        }

        if (this.shouldAutoAdvance(step)) {
          wizardNavigation.handleNext();
        }
      }
    } catch (error) {
      this.handleError({
        step,
        message: "Failed to complete step",
        error,
      });
    }
  }

  async validateStep(step) {
    const handler = this.handlers[step];
    if (!handler || typeof handler.validateStep !== "function") {
      return true;
    }

    try {
      const isValid = await handler.validateStep();
      this.updateStepIndicator(step, isValid);
      return isValid;
    } catch (error) {
      this.handleError({
        step,
        message: "Step validation failed",
        error,
      });
      return false;
    }
  }

  updateStepIndicator(step, isValid) {
    const indicator = document.querySelector(
      `.step-indicator[data-step="${step}"]`
    );
    if (indicator) {
      indicator.classList.toggle("valid", isValid);
      indicator.classList.toggle("invalid", !isValid);
    } else {
      console.error(`Element for step ${step} not found.`);
    }
  }

  shouldAutoAdvance(step) {
    // Configure which steps should automatically advance
    const autoAdvanceSteps = [this.steps.UPLOAD, this.steps.SCHEMA_SELECTION];
    return autoAdvanceSteps.includes(step);
  }

  getStepKey(step) {
    return Object.keys(this.steps).find((key) => this.steps[key] === step);
  }
  restoreState() {
    if (!sessionManager) return;

    const currentStep = Alpine.store("session")?.currentStep || 0;
    if (currentStep > 0) {
      for (let i = 0; i < currentStep; i++) {
        this.validateStep(i);
      }
    }
  }

  handleError({ step, message, error }) {
    console.error(`Error in step ${step}: ${message}`, error);

    const errorContainer = document.getElementById("wizardErrors");
    if (errorContainer) {
      errorContainer.innerHTML = `
                <div class="bg-red-50 border-l-4 border-red-400 p-4">
                    <div class="flex">
                        <div class="flex-shrink-0">
                            <svg class="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                                <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
                            </svg>
                        </div>
                        <div class="ml-3">
                            <p class="text-sm text-red-700">${message}</p>
                            ${
                              error
                                ? `<p class="mt-1 text-xs text-red-600">${error.message}</p>`
                                : ""
                            }
                        </div>
                    </div>
                </div>
            `;
      errorContainer.classList.remove("hidden");
    }
  }
}


let wizardManagerInstance;
document.addEventListener('DOMContentLoaded', () => {
    wizardManagerInstance = new WizardManager();
});

export const wizardManager = wizardManagerInstance;
