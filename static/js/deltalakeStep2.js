document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeStep2", () => ({
    specs: Alpine.store("dlWizard").state.specs || [],
    rowCount: Alpine.store("dlWizard").state.rowCount,
    bytesPerRow: Alpine.store("dlWizard").state.bytesPerRow,
    availableColumns: Alpine.store("dlWizard").state.availableColumns || [],
    geometryColumns: Alpine.store("dlWizard").state.geometryColumns || [],
    recalculating: false,
    error: null,

    // --- Partition column helpers ---

    /**
     * Columns shown in the partition dropdown.
     * Hides internal computed columns (_morton, _x, _y, _z suffixes of geometry cols).
     * Shows the original geometry column names (selectable for spatial partitioning).
     * For specs that already have a morton partition_by, shows the source geometry col.
     */
    partitionColumns(spec) {
      const hidden = new Set();
      for (const geo of this.geometryColumns) {
        hidden.add(`${geo}_morton`);
        hidden.add(`${geo}_x`);
        hidden.add(`${geo}_y`);
        hidden.add(`${geo}_z`);
      }
      return this.availableColumns.filter((c) => !hidden.has(c));
    },

    /**
     * Display value for the partition dropdown.
     * For spatial specs, shows the geometry column name rather than the internal _morton name.
     */
    partitionDisplayValue(spec) {
      if (spec.source_geometry_column) {
        return spec.source_geometry_column;
      }
      return spec.partition_by;
    },

    /**
     * Handle partition column change. Auto-derives morton config for geometry columns.
     */
    onPartitionChange(specIdx, selectedCol) {
      const spec = this.specs[specIdx];
      if (this.geometryColumns.includes(selectedCol)) {
        // Spatial column selected → auto-derive morton partitioning
        spec.source_geometry_column = selectedCol;
        spec.partition_by = `${selectedCol}_morton`;
        spec.partition_strategy = "uniform_range";
        // Auto-add the morton column to z-order if not already present
        if (!spec.zorder_columns) spec.zorder_columns = [];
        const mortonCol = `${selectedCol}_morton`;
        if (!spec.zorder_columns.includes(mortonCol)) {
          spec.zorder_columns = [mortonCol, ...spec.zorder_columns.filter(c => c !== mortonCol)];
        }
      } else {
        // Non-spatial column (or flat)
        spec.source_geometry_column = null;
        spec.partition_by = selectedCol || null;
      }
      this.syncStore();
    },

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
        const store = Alpine.store("dlWizard").state;
        const globalTarget = store.targetPartitionSizeMb || 256;

        for (const spec of this.specs) {
          if (spec.n_partitions === "auto" || spec.n_partitions == null) {
            const targetMb = spec.target_file_size_mb || globalTarget;
            const targetBytes = targetMb * 1024 * 1024;
            const totalBytes = this.rowCount * this.bytesPerRow;
            spec.n_partitions = Math.max(1, Math.ceil(totalBytes / targetBytes));
          }
        }

        this.syncStore();
      } catch (e) {
        this.error = e.message;
      } finally {
        this.recalculating = false;
      }
    },
  }));
});
