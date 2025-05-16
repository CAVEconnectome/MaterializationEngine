document.addEventListener("alpine:init", () => {
  Alpine.store("schema", {
    state: {
      schemas: [],
      selectedSchema: "",
      csvColumns: [],
      columnMapping: {},
      ignoredColumns: [],
      displayFields: [],
      schemaModel: null,
      validationErrors: [],
      mappingSaved: false,
      useMergedPositions: false,
      hasPositionFields: false,
    },

    init() {
      this.loadInitialState();

      if (window.location.pathname.endsWith("/materialize/upload/step2")) {
        console.log(
          "[SchemaStore] On step2 page, performing full init. Path:",
          window.location.pathname
        );
        this.loadSchemas();
        this.loadCsvColumns();
      } else {
        console.log(
          "[SchemaStore] Not on step2 page, only loaded initial state. Path:",
          window.location.pathname
        );
      }
    },

    loadInitialState() {
      const savedState = localStorage.getItem("schemaStore");
      if (savedState) {
        try {
          const state = JSON.parse(savedState);
          Object.keys(state).forEach((key) => {
            if (key !== "validationErrors" && this.state.hasOwnProperty(key)) {
              this.state[key] = state[key];
            }
          });
          this.state.validationErrors = Array.isArray(
            this.state.validationErrors
          )
            ? this.state.validationErrors
            : [];
        } catch (e) {
          console.error("Error loading schemaStore from localStorage:", e);
          this.state.validationErrors = [];
        }
      }
    },

    saveState() {
      const stateToSave = { ...this.state };
      delete stateToSave.validationErrors;
      localStorage.setItem("schemaStore", JSON.stringify(stateToSave));
    },

    async loadSchemas() {
      try {
        const response = await fetch(
          "/materialize/upload/api/get-schema-types"
        );
        const data = await response.json();
        if (data.status === "success") {
          this.state.schemas = data.schemas || [];
          this.saveState();
        }
      } catch (error) {
        console.error("Error loading schemas:", error);
      }
    },

    loadCsvColumns() {
      const uploadStore = localStorage.getItem("uploadStore");
      if (uploadStore) {
        const { previewRows } = JSON.parse(uploadStore);
        if (previewRows?.length > 0) {
          this.state.csvColumns = previewRows[0];
          this.saveState();
        }
      }
    },

    async selectSchema(schemaName, isRestoring = false) {
      if (!schemaName) {
        this.resetSchemaSelection();
        return;
      }

      try {
        const response = await fetch(
          `/materialize/upload/api/get-schema-model?schema_name=${schemaName}`
        );
        const data = await response.json();

        if (data.status === "success" && data.model) {
          this.state.selectedSchema = schemaName;
          this.state.schemaModel = data.model;

          this.state.hasPositionFields = Object.values(data.model.fields).some(
            (field) => field.type.toLowerCase().includes("geometry")
          );
          this.updateDisplayFields();
          this.updateIgnoredColumns();
          this.state.validationErrors = [];
          this.saveState();
        }
      } catch (error) {
        console.error("Error selecting schema:", error);
      }
    },

    updateDisplayFields() {
      const fields = [];
      const modelFields = Object.entries(this.state.schemaModel?.fields || {});

      modelFields.forEach(([fieldName, field]) => {
        const isGeometry = field.type.toLowerCase().includes("geometry");

        if (isGeometry) {
          if (this.state.useMergedPositions) {
            // Single field for merged positions
            fields.push({
              name: fieldName,
              type: "POSITION (X,Y,Z)",
              isGeometry: true,
              required: field.required,
            });
          } else {
            // Split into x, y, z components
            ["x", "y", "z"].forEach((coord) => {
              fields.push({
                name: `${fieldName}_${coord}`,
                type: "POSITION",
                isGeometry: true,
                required: field.required,
              });
            });
          }
        } else {
          fields.push({
            name: fieldName,
            type: field.type,
            isGeometry: false,
            required: field.required,
          });
        }
      });

      this.state.displayFields = fields;
    },

    togglePositionFormat() {
      this.state.useMergedPositions = !this.state.useMergedPositions;
      const currentMapping = { ...this.state.columnMapping };
      this.updateDisplayFields();
      const newMapping = {};
      this.state.displayFields.forEach((field) => {
        newMapping[field.name] = currentMapping[field.name] || "";
      });
      this.state.columnMapping = newMapping;
      this.updateIgnoredColumns();
      this.saveState();
    },

    updateColumnMapping(fieldName, columnName) {
      this.state.columnMapping[fieldName] = columnName;
      this.updateIgnoredColumns();
      this.saveState();
    },

    updateIgnoredColumns() {
      const mappedColumns = Object.values(this.state.columnMapping).filter(
        Boolean
      );
      this.state.ignoredColumns = this.state.csvColumns.filter(
        (col) => !mappedColumns.includes(col)
      );
      this.saveState();
    },

    validateMapping() {
      const errors = [];

      if (!this.state.selectedSchema) {
        errors.push("Please select a schema type");
        this.state.validationErrors = errors;
        return false;
      }

      const unmappedRequiredFields = this.state.displayFields.filter(
        (field) => {
          return field.name !== "id" && !this.state.columnMapping[field.name];
        }
      );

      if (unmappedRequiredFields.length > 0) {
        unmappedRequiredFields.forEach((field) => {
          errors.push(`Required field not mapped: ${field.name}`);
        });
      }

      this.state.validationErrors = errors;
      return errors.length === 0;
    },

    async prepareMappingForProcessor() {
      if (this.state.mappingSaved) return true;

      this.updateIgnoredColumns();

      try {
        const response = await fetch("/materialize/upload/api/save-mapping", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            columnMapping: this.state.columnMapping,
            ignoredColumns: this.state.ignoredColumns,
          }),
        });

        if (!response.ok) throw new Error("Failed to save mapping");

        const data = await response.json();
        this.state.mappingSaved = true;
        this.saveState();
        return true;
      } catch (error) {
        console.error("Error saving mapping:", error);
        return false;
      }
    },

    getUnmappedColumns() {
      const mappedColumns = Object.values(this.state.columnMapping);
      return this.state.csvColumns.filter(
        (col) => !mappedColumns.includes(col)
      );
    },

    resetSchemaSelection() {
      this.state.selectedSchema = "";
      this.state.schemaModel = null;
      this.state.displayFields = [];
      this.state.columnMapping = {};
      this.state.ignoredColumns = [];
      this.state.validationErrors = [];
      this.state.mappingSaved = false;
      this.saveState();
    },

    isValid() {
      return this.validateMapping();
    },

    async handleNext() {
      if (!this.validateMapping()) {
        return false;
      }
      return await this.prepareMappingForProcessor();
    },

    reset() {
      this.state = {
        schemas: [],
        selectedSchema: "",
        csvColumns: [],
        columnMapping: {},
        ignoredColumns: [],
        displayFields: [],
        schemaModel: null,
        validationErrors: [],
        mappingSaved: false,
      };
      localStorage.removeItem("schemaStore");
      this.init();
    },
  });
});
