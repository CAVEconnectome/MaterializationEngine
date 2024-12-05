import { sessionManager } from './session-manager.js';

class SchemaHandler {
    constructor() {
        this.selectedSchema = null;
        this.availableSchemas = [];
        this.columnMappings = new Map();
        this.validationErrors = [];
        this.isLoading = false;
        
        this.init();
    }

    async init() {
        try {
            this.isLoading = true;
            await this.loadSchemaTypes();
            this.initializeEventListeners();
            this.restoreFromSession();
        } catch (error) {
            this.handleError('Failed to initialize schema handler', error);
        } finally {
            this.isLoading = false;
        }
    }

    async loadSchemaTypes() {
        try {
            const response = await fetch('/materialize/views/get-schema-types');
            const data = await response.json();
            console.log('Fetched schema types:', data); // Debugging
            if (data.status === 'success') {
                this.availableSchemas = data.schemas;
                this.renderSchemaOptions();
            } else {
                throw new Error(data.message || 'Failed to load schemas');
            }
        } catch (error) {
            this.handleError('Failed to load schema types', error);
            throw error;
        }
    }

    async handleSchemaSelection(schemaName) {
        console.log(`Schema selected: ${schemaName}`); // Debugging
    
        try {
            this.isLoading = true;
            const schemaDetails = await this.fetchSchemaDetails(schemaName);
            console.log('Schema details fetched:', schemaDetails); // Debugging
    
            this.selectedSchema = schemaDetails;
            this.validateCurrentMappings();
            this.saveToSession();
            this.dispatchEvent('schemaSelected', { schema: schemaDetails });
    
            this.updateColumnMappingInterface();
    
            const columnMappingSection = document.getElementById('columnMappingSection');
            if (columnMappingSection) {
                columnMappingSection.classList.remove('hidden');
            } else {
                console.error('Column mapping section not found');
            }
        } catch (error) {
            this.handleError('Failed to load schema details', error);
        } finally {
            this.isLoading = false;
        }
    }
    
    async fetchSchemaDetails(schemaName) {
        try {
            const response = await fetch(`/materialize/views/get-schema-model?schema_name=${schemaName}`);
            const data = await response.json();
            console.log('Schema details response:', data); // Debugging
    
            if (response.ok && data.fields) {
                return data;
            } else {
                throw new Error(data.message || 'Schema details not found');
            }
        } catch (error) {
            this.handleError('Failed to fetch schema details', error);
            throw error;
        }
    }
    initializeEventListeners() {
        // Listen for file processing to get available columns
        document.addEventListener('fileSelected', (event) => {
            const { preview } = event.detail;
            this.availableColumns = preview.headers;
            console.log('Available columns:', this.availableColumns); // Debugging
            this.updateColumnMappingInterface();
        });
    }

    validateCurrentMappings() {
        this.validationErrors = [];

        if (!this.selectedSchema) {
            this.validationErrors.push('No schema selected');
            return false;
        }

        for (const [fieldName, fieldInfo] of Object.entries(this.selectedSchema.fields)) {
            if (fieldInfo.required && !this.columnMappings.has(fieldName)) {
                this.validationErrors.push(`Required field "${fieldName}" is not mapped`);
            }
        }

        this.dispatchEvent('validationComplete', {
            isValid: this.validationErrors.length === 0,
            errors: this.validationErrors
        });

        return this.validationErrors.length === 0;
    }

    saveToSession() {
        try {
            sessionManager.saveStepData('schemaSelection', {
                schema: this.selectedSchema,
                mappings: Array.from(this.columnMappings.entries()),
                isValid: this.validateCurrentMappings(),
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            console.warn('Failed to save schema data to session', error);
        }
    }

    restoreFromSession() {
        const savedData = sessionManager.getStepData('schemaSelection');
        if (savedData) {
            this.selectedSchema = savedData.schema;
            this.columnMappings = new Map(savedData.mappings);
            this.updateColumnMappingInterface();
        }
    }

    renderSchemaOptions() {
        const select = document.getElementById('schema_type');
        if (!select) {
            console.error('Schema type dropdown not found!');
            return;
        }
    
        console.log('Available schemas:', this.availableSchemas); // Debugging
        select.innerHTML = '<option value="">Select a schema type</option>';
        this.availableSchemas.forEach(schema => {
            const option = document.createElement('option');
            if (typeof schema === 'string') {
                option.value = schema;
                option.textContent = schema;
            } else if (typeof schema === 'object' && schema.name) {
                option.value = schema.name;
                option.textContent = schema.name;
            } else {
                console.warn('Invalid schema entry:', schema);
                return;
            }
            select.appendChild(option);
        });
    }

    updateColumnMappingInterface() {
        if (!this.selectedSchema || !this.availableColumns) return;

        const container = document.getElementById('columnMappingContainer');
        if (!container) return;

        container.innerHTML = '';
        Object.entries(this.selectedSchema.fields).forEach(([fieldName, fieldInfo]) => {
            container.appendChild(this.createMappingRow(fieldName, fieldInfo));
        });

        this.updateValidationStatus();
    }

    createMappingRow(fieldName, fieldInfo) {
        const row = document.createElement('div');
        row.className = 'flex items-center space-x-4 mb-4 p-4 bg-gray-50 rounded-lg';

        const labelDiv = document.createElement('div');
        labelDiv.className = 'w-1/3';
        labelDiv.innerHTML = `
            <label class="block text-sm font-medium text-gray-700">
                ${fieldName} ${fieldInfo.required ? '*' : ''}
            </label>
            <p class="text-xs text-gray-500">${fieldInfo.description || ''}</p>
        `;

        const selectDiv = document.createElement('div');
        selectDiv.className = 'w-2/3';
        const select = document.createElement('select');
        select.className = 'mt-1 block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm rounded-md';
        select.innerHTML = '<option value="">Select CSV column</option>';
        
        this.availableColumns.forEach(column => {
            const option = document.createElement('option');
            option.value = column;
            option.textContent = column;
            if (this.columnMappings.get(fieldName) === column) {
                option.selected = true;
            }
            select.appendChild(option);
        });

        select.addEventListener('change', (e) => {
            if (e.target.value) {
                this.columnMappings.set(fieldName, e.target.value);
            } else {
                this.columnMappings.delete(fieldName);
            }
            this.validateCurrentMappings();
            this.saveToSession();
        });

        selectDiv.appendChild(select);
        row.appendChild(labelDiv);
        row.appendChild(selectDiv);

        return row;
    }

    updateValidationStatus() {
        const errorContainer = document.getElementById('schemaValidationErrors');
        if (!errorContainer) return;

        if (this.validationErrors.length > 0) {
            errorContainer.innerHTML = this.validationErrors
                .map(error => `<div class="text-sm text-red-600">${error}</div>`)
                .join('');
            errorContainer.classList.remove('hidden');
        } else {
            errorContainer.classList.add('hidden');
        }
    }

    handleError(message, error) {
        console.error(message, error);
        this.dispatchEvent('schemaError', { message, error: error.message });
    }

    dispatchEvent(eventName, detail) {
        const event = new CustomEvent(eventName, { detail });
        document.dispatchEvent(event);
    }
}

export const schemaHandler = new SchemaHandler();