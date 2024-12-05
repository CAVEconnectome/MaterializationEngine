const TABLE_NAME_REGEX = /^[a-zA-Z][a-zA-Z0-9_]*$/;
const MIN_DESCRIPTION_LENGTH = 10;
const MAX_DESCRIPTION_LENGTH = 1000;

function showError(element, message) {
    if (element) {
        element.textContent = message;
        element.classList.remove('hidden');
    }
}

function hideError(element) {
    if (element) {
        element.classList.add('hidden');
        element.textContent = '';
    }
}

export function validateTableName(value, showErrorUI = false) {
    const errorElement = document.getElementById('tableNameError');
    
    hideError(errorElement);

    if (!value?.trim()) {
        if (showErrorUI && errorElement) {
            showError(errorElement, 'Table name is required');
        }
        return false;
    }

    if (!TABLE_NAME_REGEX.test(value)) {
        if (showErrorUI && errorElement) {
            showError(errorElement, 
                'Table name must start with a letter and contain only letters, numbers, and underscores');
        }
        return false;
    }

    return true;
}

export function validateDescription(value, showErrorUI = false) {
    const errorElement = document.getElementById('descriptionError');
    
    hideError(errorElement);

    if (!value?.trim()) {
        if (showErrorUI && errorElement) {
            showError(errorElement, 'Description is required');
        }
        return false;
    }

    if (value.length < MIN_DESCRIPTION_LENGTH) {
        if (showErrorUI && errorElement) {
            showError(errorElement, 
                `Description must be at least ${MIN_DESCRIPTION_LENGTH} characters`);
        }
        return false;
    }

    if (value.length > MAX_DESCRIPTION_LENGTH) {
        if (showErrorUI && errorElement) {
            showError(errorElement, 
                `Description must be less than ${MAX_DESCRIPTION_LENGTH} characters`);
        }
        return false;
    }

    return true;
}

export function validateForm() {
    const tableNameInput = document.getElementById('tableName');
    const descriptionInput = document.getElementById('description');
    
    const tableNameValid = validateTableName(tableNameInput?.value, true);
    const descriptionValid = validateDescription(descriptionInput?.value, true);

    return tableNameValid && descriptionValid;
}

export function getFormData() {
    return {
        tableName: document.getElementById('tableName')?.value || '',
        description: document.getElementById('description')?.value || '',
        noticeText: document.getElementById('noticeText')?.value || ''
    };
}

document.addEventListener('DOMContentLoaded', () => {
    const tableNameInput = document.getElementById('tableName');
    const descriptionInput = document.getElementById('description');
    const form = document.getElementById('basicInfoForm');

    tableNameInput?.addEventListener('input', (e) => validateTableName(e.target.value));
    tableNameInput?.addEventListener('blur', (e) => validateTableName(e.target.value, true));

    descriptionInput?.addEventListener('input', (e) => validateDescription(e.target.value));
    descriptionInput?.addEventListener('blur', (e) => validateDescription(e.target.value, true));

    form?.addEventListener('submit', (e) => {
        e.preventDefault();
        if (validateForm()) {
            const formData = getFormData();
            document.dispatchEvent(new CustomEvent('formValidated', { detail: formData }));
        }
    });
});