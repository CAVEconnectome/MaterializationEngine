import { wizardManager } from './wizard-manager.js';

class WizardNavigation {
    constructor() {
        this.currentStep = 0;
        this.totalSteps = 4;
        this.stepValidators = {
            0: () => wizardManager.validateStep(0),
            1: () => wizardManager.validateStep(1),
            2: () => wizardManager.validateStep(2),
            3: () => wizardManager.validateStep(3),
            4: () => wizardManager.validateStep(4)
        };
    }


    init() {
        this.bindElements();
        this.setupEventListeners();
        this.restoreState();
        this.updateUI();
    }

    bindElements() {
        this.nextBtn = document.getElementById('nextBtn');
        this.prevBtn = document.getElementById('prevBtn');
        this.steps = Array.from(document.getElementsByClassName('step'));
        this.progressIndicators = Array.from(document.getElementsByClassName('step-indicator'));
    }

    setupEventListeners() {
        console.log('Setting up WizardNavigation event listeners...');
        
        this.nextBtn?.addEventListener('click', () => {
            console.log('Next button clicked, current step:', this.currentStep);
            this.handleNext();
        });
        
        document.addEventListener('fileSelected', () => {
            console.log('File selected event received');
            this.updateStepValidity(0);
        });
        document.addEventListener('schemaSelected', () => {
            console.log('Schema selected event received');
            this.updateStepValidity(1);
        });
        document.addEventListener('metadataSubmitted', () => {
            console.log('Metadata submitted event received');
            this.updateStepValidity(2);
        });
        document.addEventListener('databaseSelected', () => {
            console.log('Database selected event received');
            this.updateStepValidity(3);
        });
    }

    async handleNext() {
        if (await this.validateCurrentStep()) {
            this.currentStep++;
            this.updateUI();
            this.saveState();
        }
    }

    handlePrevious() {
        if (this.currentStep > 0) {
            this.currentStep--;
            this.updateUI();
            this.saveState();
        }
    }

    async validateCurrentStep() {
        const validator = this.stepValidators[this.currentStep];
        return validator ? await validator() : true;
    }

    enableNext() {
        if (this.nextBtn) {
            this.nextBtn.disabled = false;
        }
    }

    disableNext() {
        if (this.nextBtn) {
            this.nextBtn.disabled = true;
        }
    }
    updateUI() {
        this.steps.forEach((step, index) => {
            step.classList.toggle('hidden', index !== this.currentStep);
        });

        this.progressIndicators.forEach((indicator, index) => {
            indicator.classList.toggle('completed', index < this.currentStep);
            indicator.classList.toggle('active', index === this.currentStep);
        });

        if (this.prevBtn) {
            this.prevBtn.disabled = this.currentStep === 0;
        }
        if (this.nextBtn) {
            this.nextBtn.disabled = this.currentStep === this.totalSteps - 1;
        }
    }


    saveState() {
        sessionStorage.setItem('wizardState', JSON.stringify({
            currentStep: this.currentStep
        }));
    }

    restoreState() {
        try {
            const savedState = sessionStorage.getItem('wizardState');
            if (savedState) {
                const { currentStep } = JSON.parse(savedState);
                this.currentStep = currentStep;
            }
        } catch (error) {
            console.warn('Failed to restore wizard state', error);
        }
    }

    async validateFileStep() {
        const fileData = sessionStorage.getItem('uploadFile');
        return !!fileData;
    }

    async validateSchemaStep() {
        const schemaData = sessionStorage.getItem('schemaData');
        return !!schemaData;
    }

    async validateMetadataStep() {
        const metadataData = sessionStorage.getItem('metadataForm');
        return !!metadataData;
    }

    async validateDatabaseStep() {
        const databaseData = sessionStorage.getItem('databaseSelection');
        return !!databaseData;
    }
}

export const wizardNavigation = new WizardNavigation();