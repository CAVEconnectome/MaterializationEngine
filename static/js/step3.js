document.addEventListener("alpine:init", () => {
  Alpine.store("metadata", {
    state: {
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
      read_permission: "PUBLIC",
      validationErrors: {},
      isReferenceSchema: false,
      metadataSaved: false,
      stagingConflict: null,  // { message, row_count } when staging table exists
      // when the file's root id columns were looked up (asked only when some are mapped)
      root_ids_lookup_mode: "unknown",
      root_ids_lookup_at: "",
      root_ids_lookup_start: "",
      root_ids_lookup_end: "",
      hasSuppliedRootIds: false,
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

      if (!this.state.datastack_name) {
        const uploadStoreState = localStorage.getItem("uploadStore");
        if (uploadStoreState) {
          const { selectedDatastack } = JSON.parse(uploadStoreState);
          if (selectedDatastack) {
            this.state.datastack_name = selectedDatastack;
          }
        }
      }
    },

    saveState() {
      const stateToSave = { ...this.state };
      delete stateToSave.validationErrors;
      delete stateToSave.stagingConflict;
      delete stateToSave.isReferenceSchema;
      delete stateToSave.metadataSaved;
      delete stateToSave.hasSuppliedRootIds;
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
        this.state.hasSuppliedRootIds = Alpine.store("schema")
          ? Alpine.store("schema").hasSuppliedRootIds()
          : false;
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

      ["x", "y", "z"].forEach((dim) => {
        const value = this.state[`voxel_resolution_nm_${dim}`];
        if (!value || value <= 0) {
          errors[`voxel_resolution_nm_${dim}`] = "Must be a positive number";
        }
      });

      if (this.state.hasSuppliedRootIds) {
        const now = Date.now();
        const mode = this.state.root_ids_lookup_mode;
        if (mode === "at") {
          const at = Date.parse(this.state.root_ids_lookup_at);
          if (isNaN(at)) {
            errors.root_ids_lookup = "Give the time the root ids were looked up at";
          } else if (at > now) {
            errors.root_ids_lookup = "The time is in the future";
          }
        } else if (mode === "between") {
          const start = Date.parse(this.state.root_ids_lookup_start);
          const end = Date.parse(this.state.root_ids_lookup_end);
          if (isNaN(start) || isNaN(end)) {
            errors.root_ids_lookup = "Give both ends of the time range";
          } else if (start >= end) {
            errors.root_ids_lookup = "The range must start before it ends";
          } else if (end > now) {
            errors.root_ids_lookup = "The range ends in the future";
          }
        }
      }

      this.state.validationErrors = errors;
      return Object.keys(errors).length === 0;
    },

    // The request's root_ids_lookup: times as UTC ISO strings (the inputs are local
    // time), or null when no root id column is mapped.
    rootIdsLookup() {
      if (!this.state.hasSuppliedRootIds) return null;
      const iso = (value) => (value ? new Date(value).toISOString() : null);
      const mode = this.state.root_ids_lookup_mode || "unknown";
      return {
        mode,
        at: mode === "at" ? iso(this.state.root_ids_lookup_at) : null,
        start: mode === "between" ? iso(this.state.root_ids_lookup_start) : null,
        end: mode === "between" ? iso(this.state.root_ids_lookup_end) : null,
      };
    },

    _buildPayload(forceOverwrite = false) {
      return {
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
        ...(forceOverwrite ? { force_overwrite: true } : {}),
      };
    },

    async saveMetadata(forceOverwrite = false) {
      if (!this.validateForm()) {
        return false;
      }

      this.state.stagingConflict = null;

      try {
        const response = await fetch("/materialize/upload/api/save-metadata", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(this._buildPayload(forceOverwrite)),
        });

        if (!response.ok) {
          let errorData = {};
          try { errorData = await response.json(); } catch (_) {}

          // Staging conflict: table exists from a previous run — ask user to confirm overwrite
          if (response.status === 409 && errorData.staging_exists) {
            this.state.stagingConflict = {
              message: errorData.message,
              row_count: errorData.row_count,
            };
            return false;
          }

          throw new Error(errorData.message || "Failed to save metadata");
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

    async confirmOverwrite() {
      this.state.stagingConflict = null;
      return await this.saveMetadata(true);
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
