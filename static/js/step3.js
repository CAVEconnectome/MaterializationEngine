document.addEventListener("alpine:init", () => {
  Alpine.store("metadata", {
    state: {
      schema_type: "",
      datastack_name: "",
      table_name: "",
      description: "",
      notice_text: "",
      reference_table: "",
      referenceTableOptions: [],
      flat_segmentation_source: "",
      voxel_resolution_nm_x: 1,
      voxel_resolution_nm_y: 1,
      voxel_resolution_nm_z: 1,
      write_permission: "PUBLIC",
      read_permission: "PUBLIC",
      validationErrors: {},
      isReferenceSchema: false,
      metadataSaved: false,
    },

    async init() {
      await this.loadInitialState();

      if (Object.keys(this.state).some((key) => this.state[key])) {
        this.validateForm();
      }
    },

    async loadInitialState() {
      const savedState = localStorage.getItem("metadataStore");
      if (savedState) {
        const state = JSON.parse(savedState);
        Object.keys(state).forEach((key) => {
          if (key !== "validationErrors" && key !== "referenceTableOptions") {
            this.state[key] = state[key];
          }
        });
        this.state.validationErrors = {};
      }

      if (!this.state.datastack_name) {
        const uploadStoreState = localStorage.getItem("uploadStore");
        if (uploadStoreState) {
          const { selectedDatastack } = JSON.parse(uploadStoreState);
          if (selectedDatastack) {
            this.state.datastack_name = selectedDatastack;
          }
        }
      }
      await this.checkIfReferenceSchema();
    },

    saveState() {
      const stateToSave = { ...this.state };
      delete stateToSave.validationErrors;
      delete stateToSave.referenceTableOptions;
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
        } else {
          this.state.isReferenceSchema = false;
        }
      } else {
        this.state.isReferenceSchema = false;
      }

      if (this.state.isReferenceSchema) {
        await this.fetchReferenceTableOptions();
      } else {
        this.state.referenceTableOptions = [];
        this.state.reference_table = "";
      }
    },

    async fetchReferenceTableOptions() {
      if (!this.state.datastack_name) {
        console.warn("Datastack name not set, cannot fetch reference tables.");
        this.state.referenceTableOptions = [];
        return;
      }
      try {
        const response = await fetch(
          `/materialize/upload/api/datastack/${this.state.datastack_name}/valid-annotation-tables`
        );
        if (!response.ok) {
          const errorData = await response
            .json()
            .catch(() => ({ message: "Failed to fetch reference tables" }));
          throw new Error(
            errorData.message || "Failed to fetch reference tables"
          );
        }
        const data = await response.json();
        if (data.status === "success" && Array.isArray(data.table_names)) {
          this.state.referenceTableOptions = data.table_names;
        } else {
          this.state.referenceTableOptions = [];
          console.error("Error in fetching reference tables:", data.message);
        }
      } catch (error) {
        console.error("Error fetching reference table options:", error);
        this.state.referenceTableOptions = [];
        this.state.validationErrors.reference_table =
          "Could not load reference tables.";
      }
    },

    validateForm() {
      const errors = {};

      if (!this.state.datastack_name) {
        errors.datastack_name = "Datastack name is required";
      }

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

      if (this.state.isReferenceSchema && this.state.referenceTableOptions.length > 0 && !this.state.reference_table) {
          errors.reference_table = "Please select a reference table.";
      }

      ["x", "y", "z"].forEach((dim) => {
        const value = this.state[`voxel_resolution_nm_${dim}`];
        if (!value || value <= 0) {
          errors[`voxel_resolution_nm_${dim}`] = "Must be a positive number";
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
            datastack_name: this.state.datastack_name,
            table_name: this.state.table_name,
            description: this.state.description,
            notice_text: this.state.notice_text,
            reference_table: this.state.reference_table,
            flat_segmentation_source: this.state.flat_segmentation_source,
            voxel_resolution_nm_x: parseFloat(this.state.voxel_resolution_nm_x),
            voxel_resolution_nm_y: parseFloat(this.state.voxel_resolution_nm_y),
            voxel_resolution_nm_z: parseFloat(this.state.voxel_resolution_nm_z),
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
        datastack_name: "",
        table_name: "",
        description: "",
        notice_text: "",
        reference_table: "",
        flat_segmentation_source: "",
        voxel_resolution_nm_x: 1,
        voxel_resolution_nm_y: 1,
        voxel_resolution_nm_z: 1,
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
