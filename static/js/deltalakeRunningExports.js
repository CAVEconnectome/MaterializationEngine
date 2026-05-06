document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeRunningExports", () => ({
    exportKey: null,
    progress: {},
    poller: null,
    userScrolled: false,

    init() {
      const store = Alpine.store("dlWizard");
      this.exportKey = store.state.exportKey;

      if (this.exportKey) {
        this.pollProgress();
        this.poller = setInterval(() => this.pollProgress(), 5000);
      }
    },

    async pollProgress() {
      if (!this.exportKey) return;
      const { datastack, version, tableName } = this.exportKey;
      const url = `/materialize/api/v3/materialize/run/write_deltalake/datastack/${datastack}/version/${version}/table_name/${tableName}/`;

      try {
        const resp = await fetch(url);
        if (!resp.ok) {
          if (resp.status === 404) {
            this.progress = { status: "pending", phase: "pending" };
          }
          return;
        }
        const data = await resp.json();
        this.progress = data;

        // Auto-scroll log panel
        this.$nextTick(() => {
          const panel = this.$refs.logPanel;
          if (panel && !this.userScrolled) {
            panel.scrollTop = panel.scrollHeight;
          }
        });

        // Stop polling on terminal status
        if (data.status === "complete" || data.status === "failed") {
          this.stopPolling();
        }
      } catch (e) {
        console.error("[DeltaLake] Polling error:", e);
      }
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

  // Handle log panel scroll detection
  document.addEventListener("scroll", (e) => {
    if (e.target.classList && e.target.classList.contains("log-panel")) {
      const panel = e.target;
      const atBottom =
        panel.scrollHeight - panel.scrollTop - panel.clientHeight < 20;
      // Find the Alpine component — best-effort
      const component = Alpine.$data(
        document.querySelector("[x-data='deltalakeRunningExports']")
      );
      if (component) {
        component.userScrolled = !atBottom;
      }
    }
  }, true);
});
