document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeStep2", () => ({
    specs: Alpine.store("dlWizard").state.specs || [],
    rowCount: Alpine.store("dlWizard").state.rowCount,
    bytesPerRow: Alpine.store("dlWizard").state.bytesPerRow,
    availableColumns: Alpine.store("dlWizard").state.availableColumns || [],
    recalculating: false,
    error: null,

    // --- Z-order column management ---
    addZorderColumn(specIdx, col) {
      if (!col) return;
      const spec = this.specs[specIdx];
      if (!spec.zorder_columns) spec.zorder_columns = [];
      if (!spec.zorder_columns.includes(col)) {
        spec.zorder_columns.push(col);
        this.syncStore();
      }
    },

    removeZorderColumn(specIdx, colIdx) {
      this.specs[specIdx].zorder_columns.splice(colIdx, 1);
      this.syncStore();
    },

    moveZorderColumn(specIdx, colIdx, direction) {
      const cols = this.specs[specIdx].zorder_columns;
      const newIdx = colIdx + direction;
      if (newIdx < 0 || newIdx >= cols.length) return;
      [cols[colIdx], cols[newIdx]] = [cols[newIdx], cols[colIdx]];
      this.syncStore();
    },

    // --- Bloom filter column management ---
    toggleBloomColumn(specIdx, col) {
      const spec = this.specs[specIdx];
      if (!spec.bloom_filter_columns) spec.bloom_filter_columns = [];
      const idx = spec.bloom_filter_columns.indexOf(col);
      if (idx === -1) {
        spec.bloom_filter_columns.push(col);
      } else {
        spec.bloom_filter_columns.splice(idx, 1);
      }
      this.syncStore();
    },

    hasBloomColumn(specIdx, col) {
      const spec = this.specs[specIdx];
      return (spec.bloom_filter_columns || []).includes(col);
    },

    // --- Helpers ---
    syncStore() {
      Alpine.store("dlWizard").state.specs = this.specs;
      Alpine.store("dlWizard").saveState();
    },

    addSpec() {
      this.specs.push({
        _editable: true,
        name: "",
        partition_by: null,
        partition_strategy: "percentile_range",
        n_partitions: null,
        target_file_size_mb: null,
        bloom_filter_fpp: null,
        zorder_columns: [],
        bloom_filter_columns: [],
        source_geometry_column: null,
      });
      this.syncStore();
    },

    unusedPartitionColumns(currentIdx) {
      return this.availableColumns;
    },

    removeSpec(idx) {
      if (this.specs.length <= 1) return;
      this.specs.splice(idx, 1);
      this.syncStore();
    },

    async recalculate() {
      this.recalculating = true;
      this.error = null;
      try {
        // Only recalculate specs where n_partitions is "auto" or null
        const specsForCalc = this.specs.map((s) => ({ ...s }));

        const resp = await fetch("/materialize/deltalake/api/recalculate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            row_count: this.rowCount,
            bytes_per_row: this.bytesPerRow,
            target_partition_size_mb:
              Alpine.store("dlWizard").state.targetPartitionSizeMb,
            specs: specsForCalc,
          }),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(data.error || "Recalculation failed");
        }

        this.specs = data.specs;
        this.syncStore();
      } catch (e) {
        this.error = e.message;
      } finally {
        this.recalculating = false;
      }
    },
  }));
});
