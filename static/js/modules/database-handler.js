class DatabaseHandler {
    constructor() {
        this.selectedDatabases = new Set();
        this.availableDatabases = [];
        this.isLoading = false;
        this.errors = [];
        this.init();
    }


    async init() {
        try {
            this.showLoading();
            await this.loadDatabases();
            this.initializeEventListeners();
            this.renderDatabaseOptions();
        } catch (error) {
            console.warn('Failed to initialize database selection:', error);

            this.renderDatabaseOptions();
        } finally {
            this.hideLoading();
        }
    }


    async loadDatabases() {
        try {
            // Fetch available databases
            const response = await fetch('/materialize/views/databases', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            this.availableDatabases = data.databases || [];
        } catch (error) {
            this.handleError('Failed to load databases', error);
            throw error;
        }
    }

    initializeEventListeners() {
        const container = document.getElementById('databaseSelectionContainer');
        if (container) {
            container.addEventListener('change', (e) => {
                if (e.target.type === 'checkbox') {
                    this.handleDatabaseSelection(e);
                }
            });
        }

        const submitButton = document.getElementById('submitDatabases');
        if (submitButton) {
            submitButton.addEventListener('click', () => this.handleSubmit());
        }
    }

    renderDatabaseOptions() {
        const container = document.getElementById('databaseSelectionContainer');
        if (!container) return;

        container.innerHTML = `
            <div class="space-y-4">
                <div class="font-medium text-gray-700 mb-2">
                    Select target databases for your data:
                </div>
                ${this.availableDatabases.map(db => this.createDatabaseOption(db)).join('')}
            </div>
            <div id="databaseErrors" class="mt-4 text-red-600 hidden"></div>
        `;
    }

    createDatabaseOption(database) {
        const isChecked = this.selectedDatabases.has(database.id);
        const isDisabled = database.isDefault || database.isRequired;
        
        return `
            <div class="flex items-center space-x-3 p-4 border rounded-lg ${database.isDefault ? 'bg-blue-50' : 'bg-white'}">
                <input type="checkbox"
                       id="db-${database.id}"
                       value="${database.id}"
                       ${isChecked ? 'checked' : ''}
                       ${isDisabled ? 'disabled' : ''}
                       class="h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500">
                <label for="db-${database.id}" class="flex-1">
                    <div class="font-medium text-gray-900">${database.name}</div>
                    <div class="text-sm text-gray-500">${database.description || ''}</div>
                    ${database.isDefault ? '<div class="text-xs text-blue-600">Default database (required)</div>' : ''}
                </label>
            </div>
        `;
    }

    handleDatabaseSelection(event) {
        const checkbox = event.target;
        const databaseId = checkbox.value;

        if (checkbox.checked) {
            this.selectedDatabases.add(databaseId);
        } else {
            this.selectedDatabases.delete(databaseId);
        }

        this.validateSelection();
        this.updateSubmitButton();
    }

    validateSelection() {
        this.errors = [];

        if (this.selectedDatabases.size === 0) {
            this.errors.push('Please select at least one database');
        }

        const requiredDbs = this.availableDatabases.filter(db => db.isRequired);
        for (const db of requiredDbs) {
            if (!this.selectedDatabases.has(db.id)) {
                this.errors.push(`${db.name} is required`);
            }
        }

        this.showErrors();
        return this.errors.length === 0;
    }

    showErrors() {
        const errorContainer = document.getElementById('databaseErrors');
        if (!errorContainer) return;

        if (this.errors.length > 0) {
            errorContainer.innerHTML = this.errors
                .map(error => `<div class="text-sm">${error}</div>`)
                .join('');
            errorContainer.classList.remove('hidden');
        } else {
            errorContainer.classList.add('hidden');
        }
    }

    updateSubmitButton() {
        const submitButton = document.getElementById('submitDatabases');
        if (submitButton) {
            submitButton.disabled = this.errors.length > 0;
        }
    }

    async handleSubmit() {
        if (!this.validateSelection()) {
            return;
        }

        try {
            this.showLoading();
            await this.submitDatabaseSelection();
            this.dispatchCompletionEvent();
        } catch (error) {
            this.handleError('Failed to submit database selection', error);
        } finally {
            this.hideLoading();
        }
    }

    async submitDatabaseSelection() {
        const previousData = this.getPreviousStepData();
        if (!previousData) {
            throw new Error('Missing data from previous steps');
        }

        try {
            const response = await fetch('/materialize/api/v2/submit-configuration', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    ...previousData,
                    databases: Array.from(this.selectedDatabases)
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();
            return result;
        } catch (error) {
            throw new Error('Failed to submit configuration: ' + error.message);
        }
    }

    getPreviousStepData() {
        try {
            const step1Data = JSON.parse(sessionStorage.getItem('step1Data'));
            const step2Data = JSON.parse(sessionStorage.getItem('step2Data'));
            const step3Data = JSON.parse(sessionStorage.getItem('step3Data'));
            
            if (!step1Data || !step2Data || !step3Data) {
                throw new Error('Missing required data from previous steps');
            }

            return {
                file: step1Data.file,
                metadata: {
                    ...step1Data.form,
                    schema_mapping: step2Data.mappings
                },
                upload_info: step3Data
            };
        } catch (error) {
            console.error('Error getting previous step data:', error);
            return null;
        }
    }

    dispatchCompletionEvent() {
        const event = new CustomEvent('databaseSelectionComplete', {
            detail: {
                selectedDatabases: Array.from(this.selectedDatabases),
                success: true
            }
        });
        document.dispatchEvent(event);
    }

    showLoading() {
        this.isLoading = true;
        const loadingElement = document.getElementById('databaseLoadingIndicator');
        if (loadingElement) {
            loadingElement.classList.remove('hidden');
        }
    }

    hideLoading() {
        this.isLoading = false;
        const loadingElement = document.getElementById('databaseLoadingIndicator');
        if (loadingElement) {
            loadingElement.classList.add('hidden');
        }
    }

    handleError(message, error) {
        console.error(message, error);
        this.errors.push(message);
        this.showErrors();
    }

    getStepData() {
        return {
            selectedDatabases: Array.from(this.selectedDatabases),
            isValid: this.validateSelection()
        };
    }
}

export const databaseHandler = new DatabaseHandler();