document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeStep3", () => ({
    launching: false,
    error: null,
    existingExports: [],
    checkingExists: false,

    init() {
      this.checkExists();
    },

    outputUri(spec) {
      const store = Alpine.store("dlWizard").state;
      return `${store.datastack}/v${store.version}/${store.tableName}/${spec.name}`;
    },

    async checkExists() {
      this.checkingExists = true;
      const state = Alpine.store("dlWizard").state;
      const specNames = state.specs.map((s) => s.name);
      try {
        const resp = await fetch("/materialize/deltalake/api/check-exists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            datastack: state.datastack,
            version: state.version,
            table_name: state.tableName,
            spec_names: specNames,
          }),
        });
        if (resp.ok) {
          const data = await resp.json();
          this.existingExports = data.existing_specs || [];
        }
      } catch (e) {
        console.warn("[DeltaLake] check-exists failed:", e);
      } finally {
        this.checkingExists = false;
      }
    },

    async launchExport() {
      this.launching = true;
      this.error = null;
      const store = Alpine.store("dlWizard");
      const state = store.state;

      try {
        const url = `/materialize/api/v2/materialize/run/write_deltalake/datastack/${state.datastack}/version/${state.version}/table_name/${state.tableName}/`;

        const resp = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            output_specs: state.specs.map((s) => {
              const { _editable, ...spec } = s;
              return {
                ...spec,
                bloom_filter_fpp: spec.bloom_filter_columns?.length
                  ? (spec.bloom_filter_fpp || state.bloomFilterFpp)
                  : null,
              };
            }),
          }),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(data.message || data.error || "Export launch failed");
        }

        // Add to exports list for monitoring page
        const newExport = {
          datastack: state.datastack,
          version: state.version,
          tableName: state.tableName,
          jobId: data.job_id,
          submittedAt: new Date().toISOString(),
        };
        // Clear wizard form state, then add the export
        store.clearState();
        store.state.exports.push(newExport);
        store.saveState();

        // Redirect to monitoring page
        window.location.href = "/materialize/deltalake/running-exports";
      } catch (e) {
        this.error = e.message;
      } finally {
        this.launching = false;
      }
    },
  }));
});
