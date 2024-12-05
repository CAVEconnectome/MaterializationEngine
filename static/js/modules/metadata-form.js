import { sessionManager } from './session-manager.js';

class MetadataForm {
    constructor() {
        this.formData = {
            tableName: '',
            description: '',
            voxelResolution: {
                x: 1.0,
                y: 1.0,
                z: 1.0
            },
            permissions: {
                read: 'PRIVATE',
                write: 'PRIVATE'
            },
            referenceTable: '',
            noticeText: '',
            flatSegmentationSource: ''
        };

        this.validationRules = {
            tableName: {
                required: true,
                pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
                message: 'Table name must start with a letter and contain only letters, numbers, and underscores'
            },
            description: {
                required: true,
                minLength: 10,
                maxLength: 1000,
                message: 'Description must be between 10 and 1000 characters'
            },
            voxelResolution: {
                required: true,
                min: 0.0001,
                message: 'Voxel resolution must be greater than 0'
            }
        };

        this.init();
    }

    init() {
        this.bindFormElements();
        this.restoreFromSession();
        this.setupEventListeners();
    }

    bindFormElements() {
        this.form = document.getElementById('annotationMetadataForm');
        this.tableNameInput = document.getElementById('table_name');
        this.descriptionInput = document.getElementById('description');
        this.noticeTextInput = document.getElementById('notice_text');
        this.referenceTableInput = document.getElementById('reference_table');
        this.flatSegSourceInput = document.getElementById('flat_segmentation_source');
        
        this.voxelInputs = {
            x: document.getElementById('voxel_resolution_x'),
            y: document.getElementById('voxel_resolution_y'),
            z: document.getElementById('voxel_resolution_z')
        };

        this.permissionSelects = {
            read: document.getElementById('read_permission'),
            write: document.getElementById('write_permission')
        };
    }

    setupEventListeners() {
        if (!this.form) return;

        const handleInput = (element, fieldName) => {
            element?.addEventListener('input', () => {
                this.validateField(fieldName, element.value);
                this.saveFormState();
            });
        };

        handleInput(this.tableNameInput, 'tableName');
        handleInput(this.descriptionInput, 'description');

        Object.entries(this.voxelInputs).forEach(([axis, input]) => {
            input?.addEventListener('input', () => {
                this.validateVoxelResolution(axis, input.value);
                this.saveFormState();
            });
        });

        Object.values(this.permissionSelects).forEach(select => {
            select?.addEventListener('change', () => this.saveFormState());
        });

        this.form.addEventListener('submit', (e) => this.handleSubmit(e));
        document.getElementById('resetMetadataForm')?.addEventListener('click', 
            () => this.resetForm());
    }

    validateField(fieldName, value) {
        const errors = [];
        const rules = this.validationRules[fieldName];
        
        if (!rules) return true;

        if (rules.required && !value.trim()) {
            errors.push(`${fieldName} is required`);
        }

        if (rules.pattern && !rules.pattern.test(value)) {
            errors.push(rules.message);
        }

        if (rules.minLength && value.length < rules.minLength) {
            errors.push(`Minimum length is ${rules.minLength} characters`);
        }

        if (rules.maxLength && value.length > rules.maxLength) {
            errors.push(`Maximum length is ${rules.maxLength} characters`);
        }

        this.showFieldErrors(fieldName, errors);
        return errors.length === 0;
    }

    validateVoxelResolution(axis, value) {
        const numValue = parseFloat(value);
        const errors = [];

        if (isNaN(numValue) || numValue <= 0) {
            errors.push(`Voxel resolution ${axis} must be a positive number`);
        }

        this.showFieldErrors(`voxel_resolution_${axis}`, errors);
        return errors.length === 0;
    }

    showFieldErrors(fieldName, errors) {
        const errorElement = document.getElementById(`${fieldName}_error`);
        if (!errorElement) return;

        if (errors.length > 0) {
            errorElement.textContent = errors[0];
            errorElement.classList.remove('hidden');
        } else {
            errorElement.classList.add('hidden');
        }
    }

    validateForm() {
        let isValid = true;

        isValid = this.validateField('tableName', this.tableNameInput.value) && isValid;
        isValid = this.validateField('description', this.descriptionInput.value) && isValid;

        Object.entries(this.voxelInputs).forEach(([axis, input]) => {
            isValid = this.validateVoxelResolution(axis, input.value) && isValid;
        });

        return isValid;
    }

    gatherFormData() {
        this.formData = {
            tableName: this.tableNameInput.value,
            description: this.descriptionInput.value,
            noticeText: this.noticeTextInput.value,
            referenceTable: this.referenceTableInput.value,
            flatSegmentationSource: this.flatSegSourceInput.value,
            voxelResolution: {
                x: parseFloat(this.voxelInputs.x.value),
                y: parseFloat(this.voxelInputs.y.value),
                z: parseFloat(this.voxelInputs.z.value)
            },
            permissions: {
                read: this.permissionSelects.read.value,
                write: this.permissionSelects.write.value
            }
        };

        return this.formData;
    }

    async handleSubmit(event) {
        event.preventDefault();

        if (!this.validateForm()) {
            this.dispatchEvent('validationFailed', {
                message: 'Please fix validation errors before proceeding'
            });
            return;
        }

        const formData = this.gatherFormData();
        const fileData = sessionManager.getStepData('fileSelection');
        const schemaData = sessionManager.getStepData('schemaSelection');

        if (!fileData || !schemaData) {
            this.dispatchEvent('submitError', {
                message: 'Missing required file or schema data'
            });
            return;
        }

        try {
            const response = await fetch('/materialize/views/store-metadata', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    filename: fileData.file.name,
                    metadata: {
                        ...formData,
                        schema_info: schemaData.schema
                    }
                })
            });

            const result = await response.json();

            if (result.status === 'success') {
                this.saveFormState();
                sessionManager.saveStepData('metadataForm', {
                    data: formData,
                    isValid: true,
                    metadataFile: result.metadata_file,
                    timestamp: new Date().toISOString()
                });

                this.dispatchEvent('metadataSubmitted', { 
                    formData,
                    metadataFile: result.metadata_file
                });
            } else {
                throw new Error(result.message);
            }
        } catch (error) {
            this.dispatchEvent('submitError', {
                message: 'Failed to save metadata',
                error: error.message
            });
        }
    }

    saveToSession() {
        try {
            const formData = this.gatherFormData();
            sessionStorage.setItem('metadataForm', JSON.stringify(formData));
        } catch (error) {
            console.warn('Failed to save form data to session', error);
        }
    }

    saveFormState() {
        const formData = this.gatherFormData();
        const isValid = this.validateForm();

        sessionManager.saveStepData('metadataForm', {
            data: formData,
            isValid: isValid,
            lastUpdated: new Date().toISOString()
        });

        this.updateSubmitButtonState(isValid);
    }


    updateSubmitButtonState(isValid) {
        const submitButton = document.getElementById('submitMetadataForm');
        if (submitButton) {
            submitButton.disabled = !isValid;
        }
    }


    restoreFromSession() {
        const savedData = sessionManager.getStepData('metadataForm');
        if (savedData && savedData.data) {
            this.populateForm(savedData.data);
            
            this.validateForm();
            this.updateSubmitButtonState(savedData.isValid);
        }
    }

    populateForm(data) {
        if (!data) return;

        this.tableNameInput.value = data.tableName || '';
        this.descriptionInput.value = data.description || '';
        this.noticeTextInput.value = data.noticeText || '';
        this.referenceTableInput.value = data.referenceTable || '';
        this.flatSegSourceInput.value = data.flatSegmentationSource || '';

        if (data.voxelResolution) {
            Object.entries(data.voxelResolution).forEach(([axis, value]) => {
                if (this.voxelInputs[axis]) {
                    this.voxelInputs[axis].value = value;
                }
            });
        }

        if (data.permissions) {
            Object.entries(data.permissions).forEach(([type, value]) => {
                if (this.permissionSelects[type]) {
                    this.permissionSelects[type].value = value;
                }
            });
        }
    }

    dispatchEvent(eventName, detail) {
        const event = new CustomEvent(eventName, { detail });
        document.dispatchEvent(event);
    }

    resetForm() {
        this.form.reset();
        
        Object.entries(this.voxelInputs).forEach(([_, input]) => {
            input.value = '1.0';
        });

        sessionManager.saveStepData('metadataForm', null);
        
        document.querySelectorAll('[id$="_error"]').forEach(errorElement => {
            errorElement.classList.add('hidden');
        });

        this.formData = {
            ...this.formData,
            voxelResolution: { x: 1.0, y: 1.0, z: 1.0 },
            permissions: { read: 'PRIVATE', write: 'PRIVATE' }
        };

        this.saveFormState();
        this.dispatchEvent('formReset', null);
    }

}

export const metadataForm = new MetadataForm();