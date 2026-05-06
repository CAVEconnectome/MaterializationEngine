document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeStep2", () => ({
    specs: Alpine.store("dlWizard").state.specs || [],
    rowCount: Alpine.store("dlWizard").state.rowCount,
    bytesPerRow: Alpine.store("dlWizard").state.bytesPerRow,
    recalculating: false,
    error: null,

    removeSpec(idx) {
      if (this.specs.length <= 1) return;
      this.specs.splice(idx, 1);
      Alpine.store("dlWizard").state.specs = this.specs;
      Alpine.store("dlWizard").saveState();
    },

    async recalculate() {
      this.recalculating = true;
      this.error = null;
      try {
        // Mark specs with explicit n_partitions as fixed, set others to "auto"
        const specsForCalc = this.specs.map((s) => ({ ...s }));

        const resp = await fetch("/materialize/deltalake/api/recalculate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            row_count: this.rowCount,
            bytes_per_row: this.bytesPerRow,
            specs: specsForCalc,
          }),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(data.error || "Recalculation failed");
        }

        this.specs = data.specs;
        Alpine.store("dlWizard").state.specs = this.specs;
        Alpine.store("dlWizard").saveState();
      } catch (e) {
        this.error = e.message;
      } finally {
        this.recalculating = false;
      }
    },
  }));
});
