// Suggest which file column feeds each schema field, from their names.
//
// Names are split into components (on _, -, spaces, dots and camelCase). A column
// component matches a field component when they are equal or one is a prefix of the
// other ("pos" ~ "position"), worth the shorter one's length in characters. The score
// is 2 * matched characters / (characters in both names): identical names score 1.
//
// A pair is only suggested when every component of one name matched, and it scores at
// least MIN_SCORE: "pre_x" suits "pre_pt_position_x" (0.44), but neither
// "pre_pt_root_id" (its "root" and "id" match nothing there) nor "id_partition" for
// "id" (0.31). Counting characters in order without components would not work:
// "post_x" spells out a subsequence of "pre_pt_position_x".
(function (root) {
  const MIN_SCORE = 0.4;

  function components(name) {
    return String(name)
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter(Boolean);
  }

  function componentMatch(a, b) {
    if (a === b) return a.length;
    if (a.startsWith(b) || b.startsWith(a)) return Math.min(a.length, b.length);
    return 0;
  }

  // { score, allColumn, allField } for one column against one field.
  function scoreNames(column, field) {
    const columnParts = components(column);
    const fieldParts = components(field);
    const columnChars = columnParts.join("").length;
    const fieldChars = fieldParts.join("").length;
    if (!columnChars || !fieldChars) return { score: 0, allColumn: false, allField: false };

    // pair components one to one, best matches first
    const pairs = [];
    columnParts.forEach((c, i) =>
      fieldParts.forEach((f, j) => {
        const chars = componentMatch(c, f);
        if (chars) pairs.push({ i, j, chars, exact: c === f });
      })
    );
    pairs.sort((p, q) => q.chars - p.chars || q.exact - p.exact);
    const usedColumn = new Set();
    const usedField = new Set();
    let matched = 0;
    for (const p of pairs) {
      if (usedColumn.has(p.i) || usedField.has(p.j)) continue;
      usedColumn.add(p.i);
      usedField.add(p.j);
      matched += p.chars;
    }
    return {
      score: (2 * matched) / (columnChars + fieldChars),
      allColumn: usedColumn.size === columnParts.length,
      allField: usedField.size === fieldParts.length,
    };
  }

  // { field: column } for the fields that have a good enough match; each column is
  // used once, the best-scoring pairs first. Fields in `skip` are left out.
  function suggestColumnMapping(fieldNames, columns, skip) {
    const skipFields = new Set(skip || []);
    const candidates = [];
    fieldNames.forEach((field) => {
      if (skipFields.has(field)) return;
      columns.forEach((column, order) => {
        const exact = String(column).toLowerCase() === String(field).toLowerCase();
        const s = scoreNames(column, field);
        if (exact || ((s.allColumn || s.allField) && s.score >= MIN_SCORE)) {
          candidates.push({ field, column, score: exact ? 2 : s.score, order });
        }
      });
    });
    candidates.sort((a, b) => b.score - a.score || a.order - b.order);
    const mapping = {};
    const usedColumns = new Set();
    for (const c of candidates) {
      if (c.field in mapping || usedColumns.has(c.column)) continue;
      mapping[c.field] = c.column;
      usedColumns.add(c.column);
    }
    return mapping;
  }

  const api = { suggestColumnMapping, scoreNames, components, MIN_SCORE };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.columnMatch = api;
})(typeof window !== "undefined" ? window : this);
