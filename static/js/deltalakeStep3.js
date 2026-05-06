document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeStep3", () => ({
    launching: false,
    error: null,

    outputUri(spec) {
      const store = Alpine.store("dlWizard").state;
      const lakeName = spec.partition_by || "flat";
      return `${store.datastack}/v${store.version}/${store.tableName}/${lakeName}`;
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
            output_specs: state.specs.map((s) => ({
              ...s,
              bloom_filter_fpp: s.bloom_filter_columns?.length
                ? (s.bloom_filter_fpp || state.bloomFilterFpp)
                : null,
            })),
          }),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(data.message || data.error || "Export launch failed");
        }

        // Store export key info for monitoring page
        store.state.exportKey = {
          datastack: state.datastack,
          version: state.version,
          tableName: state.tableName,
        };
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
