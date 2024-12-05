class StepHandler {
    constructor() {
        this.fileHandler = null;
        this.formValidator = null;
        this.isFileValid = false;
        this.isFormValid = false;
        
        this.initializeEventListeners();
    }

    initializeEventListeners() {
        document.addEventListener('fileProcessed', (event) => {
            this.isFileValid = true;
            this.checkStepCompletion();
        });

        document.addEventListener('formValidated', (event) => {
            this.isFormValid = true;
            this.checkStepCompletion();
        });

        const nextBtn = document.getElementById('nextBtn');
        if (nextBtn) {
            nextBtn.disabled = true;
        }
    }

    checkStepCompletion() {
        const nextBtn = document.getElementById('nextBtn');
        if (!nextBtn) return;

        nextBtn.disabled = !(this.isFileValid && this.isFormValid);
    }

    getStepData() {
        if (!this.isFileValid || !this.isFormValid) {
            return null;
        }

        return {
            file: this.fileHandler.getFileData(),
            form: this.formValidator.getFormData()
        };
    }
}

export const stepHandler = new StepHandler();