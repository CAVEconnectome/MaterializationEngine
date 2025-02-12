document.addEventListener("alpine:init", () => {
  Alpine.store("metadata", {
    state: {
      schema_type: "",
      table_name: "",
      description: "",
      notice_text: "",
      reference_table: "",
      flat_segmentation_source: "",
      voxel_resolution_x: 0,
      voxel_resolution_y: 0,
      voxel_resolution_z: 0,
      write_permission: "PRIVATE",
      read_permission: "PRIVATE",
      validationErrors: {},
      isReferenceSchema: false,
      metadataSaved: false,
    },

    init() {
      this.loadInitialState();
      this.checkIfReferenceSchema();
      if (Object.keys(this.state).some((key) => this.state[key])) {
        this.validateForm();
      }
    },

    loadInitialState() {
      const savedState = localStorage.getItem("metadataStore");
      if (savedState) {
        const state = JSON.parse(savedState);
        Object.keys(state).forEach((key) => {
          if (key !== "validationErrors") {
            this.state[key] = state[key];
          }
        });
        this.state.validationErrors = {};
      }
    },

    saveState() {
      const stateToSave = { ...this.state };
      delete stateToSave.validationErrors;
      localStorage.setItem("metadataStore", JSON.stringify(stateToSave));
    },

    async checkIfReferenceSchema() {
      const schemaStore = localStorage.getItem("schemaStore");
      if (schemaStore) {
        const { selectedSchema, schemaModel } = JSON.parse(schemaStore);

        if (!this.state.schema_type) {
          this.state.schema_type = selectedSchema;
        }

        if (schemaModel && schemaModel.fields) {
          this.state.isReferenceSchema = "target_id" in schemaModel.fields;
        }
      }
    },

    validateForm() {
      const errors = {};

      if (!this.state.table_name) {
        errors.table_name = "Table name is required";
      }

      if (!this.state.description) {
        errors.description = "Description is required";
      }

      if (this.state.isReferenceSchema && !this.state.reference_table) {
        errors.reference_table =
          "Reference table is required for this schema type";
      }

      ["x", "y", "z"].forEach((dim) => {
        const value = this.state[`voxel_resolution_${dim}`];
        if (!value || value <= 0) {
          errors[`voxel_resolution_${dim}`] = "Must be a positive number";
        }
      });

      this.state.validationErrors = errors;
      return Object.keys(errors).length === 0;
    },

    async saveMetadata() {
      if (!this.validateForm()) {
        return false;
      }

      try {
        const response = await fetch("/materialize/upload/api/save-metadata", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            schema_type: this.state.schema_type,
            table_name: this.state.table_name,
            description: this.state.description,
            notice_text: this.state.notice_text,
            reference_table: this.state.reference_table,
            flat_segmentation_source: this.state.flat_segmentation_source,
            voxel_resolution_x: parseFloat(this.state.voxel_resolution_x),
            voxel_resolution_y: parseFloat(this.state.voxel_resolution_y),
            voxel_resolution_z: parseFloat(this.state.voxel_resolution_z),
            write_permission: this.state.write_permission,
            read_permission: this.state.read_permission,
          }),
        });

        if (!response.ok) {
          throw new Error("Failed to save metadata");
        }

        const data = await response.json();
        this.state.metadataSaved = true;
        this.saveState();
        return true;
      } catch (error) {
        console.error("Error saving metadata:", error);
        this.state.validationErrors.general = error.message;
        return false;
      }
    },

    isValid() {
      return this.validateForm();
    },

    async handleNext() {
      return await this.saveMetadata();
    },

    reset() {
      this.state = {
        schema_type: "",
        table_name: "",
        description: "",
        notice_text: "",
        reference_table: "",
        flat_segmentation_source: "",
        voxel_resolution_x: 0,
        voxel_resolution_y: 0,
        voxel_resolution_z: 0,
        write_permission: "PRIVATE",
        read_permission: "PRIVATE",
        validationErrors: {},
        isReferenceSchema: false,
        metadataSaved: false,
      };
      localStorage.removeItem("metadataStore");
      this.init();
    },
  });
});
