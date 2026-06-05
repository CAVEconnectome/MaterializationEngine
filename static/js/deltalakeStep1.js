document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeStep1", () => ({
    datastack: Alpine.store("dlWizard").state.datastack || "",
    version: Alpine.store("dlWizard").state.version || "",
    tableName: Alpine.store("dlWizard").state.tableName || "",
    targetPartitionSizeMb: Alpine.store("dlWizard").state.targetPartitionSizeMb || 256,
    bloomFilterFpp: Alpine.store("dlWizard").state.bloomFilterFpp || 0.001,
    versions: [],
    tables: [],
    loadingVersions: false,
    loadingTables: false,
    discovering: false,
    error: null,
    existingExports: [],
    checkingExists: false,

    init() {
      if (this.datastack) {
        this.fetchVersions();
      }
      if (this.version) {
        this.fetchTables();
      }
    },

    async onDatastackChange() {
      this.version = "";
      this.tableName = "";
      this.versions = [];
      this.tables = [];
      this.error = null;
      if (this.datastack) {
        await this.fetchVersions();
      }
    },

    async onVersionChange() {
      this.tableName = "";
      this.tables = [];
      this.error = null;
      this.existingExports = [];
      if (this.version) {
        await this.fetchTables();
      }
    },

    async fetchVersions() {
      this.loadingVersions = true;
      try {
        const resp = await fetch(
          `/materialize/api/v3/datastack/${this.datastack}/versions`
        );
        if (!resp.ok) throw new Error("Failed to fetch versions");
        const data = await resp.json();
        this.versions = data.sort((a, b) => b - a);
        if (this.versions.length > 0 && !this.version) {
          this.version = this.versions[0];
          await this.fetchTables();
        }
      } catch (e) {
        this.error = `Error loading versions: ${e.message}`;
      } finally {
        this.loadingVersions = false;
      }
    },

    async fetchTables() {
      this.loadingTables = true;
      try {
        const [tablesResp, viewsResp] = await Promise.all([
          fetch(
            `/materialize/api/v3/datastack/${this.datastack}/version/${this.version}/tables`
          ),
          fetch(
            `/materialize/api/v3/datastack/${this.datastack}/version/${this.version}/views`
          ),
        ]);
        if (!tablesResp.ok) throw new Error("Failed to fetch tables");
        const tableNames = await tablesResp.json();
        const tables = tableNames.map((name) => ({ name, type: "table" }));

        let views = [];
        if (viewsResp.ok) {
          const viewData = await viewsResp.json();
          views = Object.keys(viewData).map((name) => ({ name, type: "view" }));
        }

        this.tables = [...tables, ...views].sort((a, b) =>
          a.name.localeCompare(b.name)
        );
      } catch (e) {
        this.error = `Error loading tables: ${e.message}`;
      } finally {
        this.loadingTables = false;
      }
    },

    async onTableChange() {
      this.existingExports = [];
      this.error = null;
      if (this.tableName && this.version && this.datastack) {
        await this.checkExists();
      }
    },

    async checkExists() {
      this.checkingExists = true;
      try {
        const resp = await fetch(`/materialize/deltalake/api/${this.datastack}/check-exists`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            version: parseInt(this.version),
            table_name: this.tableName,
          }),
        });
        if (resp.ok) {
          const data = await resp.json();
          this.existingExports = data.existing_specs || [];
        }
      } catch (e) {
        // Non-critical — don't block the user on check failure.
        console.warn("[DeltaLake] check-exists failed:", e);
      } finally {
        this.checkingExists = false;
      }
    },

    async discoverSpecs() {
      this.discovering = true;
      this.error = null;
      try {
        const resp = await fetch(`/materialize/deltalake/api/${this.datastack}/discover-specs`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            version: parseInt(this.version),
            table_name: this.tableName,
            target_partition_size_mb: this.targetPartitionSizeMb,
          }),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(data.error || "Discovery failed");
        }

        // Save to wizard store
        const store = Alpine.store("dlWizard");
        store.state.datastack = this.datastack;
        store.state.version = parseInt(this.version);
        store.state.tableName = this.tableName;
        store.state.targetPartitionSizeMb = this.targetPartitionSizeMb;
        store.state.bloomFilterFpp = this.bloomFilterFpp;
        store.state.rowCount = data.row_count;
        store.state.bytesPerRow = data.bytes_per_row;
        store.state.availableColumns = data.available_columns || [];
        store.state.geometryColumns = data.geometry_columns || [];
        store.state.specs = data.specs;
        store.state.stepStatus[1].completed = true;
        store.saveState();

        // Navigate to step 2
        store.state.currentStep = 2;
        store.saveState();
        window.location.href = "/materialize/deltalake/step2";
      } catch (e) {
        this.error = e.message;
      } finally {
        this.discovering = false;
      }
    },
  }));
});
