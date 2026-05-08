document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeRunningExports", () => ({
    exports: [],
    progress: {},  // keyed by "datastack/version/tableName"
    poller: null,

    init() {
      const store = Alpine.store("dlWizard");
      this.exports = store.state.exports || [];

      if (this.exports.length > 0) {
        this.pollAll();
        this.poller = setInterval(() => this.pollAll(), 5000);
      }
    },

    exportId(exp) {
      return `${exp.datastack}/${exp.version}/${exp.tableName}/${exp.jobId || "default"}`;
    },

    getProgress(exp) {
      return this.progress[this.exportId(exp)] || {};
    },

    async pollAll() {
      await Promise.all(this.exports.map((exp) => this.pollOne(exp)));
      // Stop polling if all are terminal
      const allDone = this.exports.every((exp) => {
        const p = this.getProgress(exp);
        return p.status === "complete" || p.status === "failed";
      });
      if (allDone) this.stopPolling();
    },

    async pollOne(exp) {
      const { datastack, version, tableName, jobId } = exp;
      let url = `/materialize/api/v2/materialize/run/write_deltalake/datastack/${datastack}/version/${version}/table_name/${tableName}/`;
      if (jobId) {
        url += `?job_id=${encodeURIComponent(jobId)}`;
      }
      const key = this.exportId(exp);

      try {
        const resp = await fetch(url);
        if (!resp.ok) {
          if (resp.status === 404) {
            this.progress = { ...this.progress, [key]: { status: "pending", phase: "pending" } };
          }
          return;
        }
        const data = await resp.json();
        this.progress = { ...this.progress, [key]: data };
      } catch (e) {
        console.error("[DeltaLake] Polling error:", e);
      }
    },

    removeExport(idx) {
      this.exports.splice(idx, 1);
      // Persist removal
      const store = Alpine.store("dlWizard");
      store.state.exports = this.exports;
      store.saveState();
    },

    stopPolling() {
      if (this.poller) {
        clearInterval(this.poller);
        this.poller = null;
      }
    },

    destroy() {
      this.stopPolling();
    },
  }));
});
