document.addEventListener("alpine:init", () => {
  Alpine.data("deltalakeStep2", () => ({
    specs: Alpine.store("dlWizard").state.specs || [],
    rowCount: Alpine.store("dlWizard").state.rowCount,
    bytesPerRow: Alpine.store("dlWizard").state.bytesPerRow,
    availableColumns: Alpine.store("dlWizard").state.availableColumns || [],
    geometryColumns: Alpine.store("dlWizard").state.geometryColumns || [],
    recalculating: false,
    error: null,

    init() {
      // Ensure source_geometry_column is populated for specs whose
      // partition_by matches a known geometry column's morton form.
      // Guards against stale caches or serialization round-trips that
      // may have dropped the field.
      for (const spec of this.specs) {
        if (!spec.source_geometry_column && spec.partition_by) {
          for (const geo of this.geometryColumns) {
            if (spec.partition_by === `${geo}_morton`) {
              spec.source_geometry_column = geo;
              break;
            }
          }
        }
      }
    },

    // --- Shared column helpers ---

    /** Set of internal computed columns to hide from dropdowns. */
    _hiddenGeoColumns() {
      const hidden = new Set();
      for (const geo of this.geometryColumns) {
        hidden.add(`${geo}_morton`);
        hidden.add(`${geo}_x`);
        hidden.add(`${geo}_y`);
        hidden.add(`${geo}_z`);
      }
      return hidden;
    },

    /** Display name for a column in dropdowns. Appends * to spatial columns. */
    columnDisplayName(col) {
      if (this.geometryColumns.includes(col)) {
        return `${col} *`;
      }
      return col;
    },

    /** Display label for z-order badges. Maps _morton columns to friendly name with *. */
    zorderBadgeLabel(col) {
      for (const geo of this.geometryColumns) {
        if (col === `${geo}_morton`) {
          return `${geo} *`;
        }
      }
      return col;
    },

    /** Check if a z-order column is a morton-encoded spatial column. */
    isMortonZorderCol(col) {
      return this.geometryColumns.some(geo => col === `${geo}_morton`);
    },

    // --- Partition column helpers ---

    /**
     * Columns shown in the partition dropdown.
     * Hides internal computed columns (_morton, _x, _y, _z suffixes of geometry cols).
     * Shows the original geometry column names (selectable for spatial partitioning).
     */
    partitionColumns(spec) {
      const hidden = this._hiddenGeoColumns();
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
      } else {
        // Non-spatial column (or flat)
        spec.source_geometry_column = null;
        spec.partition_by = selectedCol || null;
      }
      this.syncStore();
    },

    // --- Z-order column management ---

    /**
     * Columns shown in the z-order dropdown.
     * Hides internal computed columns and already-selected columns.
     * If a geometry column's _morton version is already selected, hides the original too.
     */
    zorderSelectableColumns(spec) {
      const hidden = this._hiddenGeoColumns();
      const selected = new Set(spec.zorder_columns || []);
      const selectedGeos = new Set();
      for (const geo of this.geometryColumns) {
        if (selected.has(`${geo}_morton`)) {
          selectedGeos.add(geo);
        }
      }
      return this.availableColumns.filter(c => !hidden.has(c) && !selected.has(c) && !selectedGeos.has(c));
    },

    addZorderColumn(specIdx, col) {
      if (!col) return;
      const spec = this.specs[specIdx];
      if (!spec.zorder_columns) spec.zorder_columns = [];
      // Geometry columns are stored as their morton-encoded equivalent
      const storeCol = this.geometryColumns.includes(col) ? `${col}_morton` : col;
      if (!spec.zorder_columns.includes(storeCol)) {
        spec.zorder_columns.push(storeCol);
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
