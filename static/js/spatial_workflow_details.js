let workflowDetailsData = {
  summary: {},
  chunks: [],
  statusCounts: {},
  loading: true,
  error: null,
  currentJobDetails: null,
  minimapFilters: {
    PENDING: true,
    PROCESSING: true,
    PROCESSING_SUBTASKS: true,
    COMPLETED: true,
    FAILED_RETRYABLE: true,
    FAILED_PERMANENT: true,
    ERROR: true,
    UNKNOWN: true,
  },
};

const MAX_CHUNKS_FOR_GRID_DISPLAY = 5000;

let errorModal = null;
let minimapCanvas = null;
let minimapCtx = null;
let minimapHoverInfo = null;

const MINIMAP_ZOOM_LEVELS = [1, 2, 3, 4, 6, 8, 10, 12, 16];
let currentMinimapZoomIndex = 1;
let minimapSquareSize = MINIMAP_ZOOM_LEVELS[currentMinimapZoomIndex];

let minimapCols = 0;

const MINIMAP_MIN_CANVAS_HEIGHT = 100;
const MINIMAP_MAX_CANVAS_HEIGHT = 700;
const MINIMAP_MAX_CANVAS_WIDTH = 800;

const STATUS_COLORS = {
  PENDING: "#e0e0e0",
  PROCESSING: "#64b5f6",
  PROCESSING_SUBTASKS: "#64b5f6",
  COMPLETED: "#81c784",
  FAILED_RETRYABLE: "#ffb74d",
  FAILED_PERMANENT: "#e57373",
  ERROR: "#b33939",
  UNKNOWN: "#bdbdbd",
};

/**
 * Initializes the spatial workflow details page.
 * Sets up event listeners and triggers the first data fetch.
 * @param {object} initialData - Data passed from the HTML (datastackName, workflowName, etc.).
 */
function initSpatialWorkflowDetailsPage(initialData) {
  console.log(
    "[SpatialWorkflowDetailsScript] Initializing page with data:",
    initialData
  );
  workflowDetailsData.currentJobDetails = initialData;

  const modalElement = document.getElementById("chunkErrorModal");
  if (modalElement) {
    errorModal = new bootstrap.Modal(modalElement);
  } else {
    console.error(
      "[SpatialWorkflowDetailsScript] Chunk error modal element not found."
    );
  }

  minimapCanvas = document.getElementById("chunkMinimapCanvas");
  minimapHoverInfo = document.getElementById("minimapHoverInfo");

  if (minimapCanvas) {
    minimapCtx = minimapCanvas.getContext("2d");

    minimapCanvas.addEventListener("mousemove", handleMinimapMouseMove);
    minimapCanvas.addEventListener("mouseout", handleMinimapMouseOut);
    minimapCanvas.addEventListener("click", handleMinimapClick);
    minimapCanvas.addEventListener("wheel", handleMinimapWheelZoom, {
      passive: false,
    }); // passive:false to allow preventDefault.
  } else {
    console.warn(
      "[SpatialWorkflowDetailsScript] Minimap canvas element not found."
    );
  }

  const maxChunksForGridEl = document.getElementById("maxChunksForGrid");
  if (maxChunksForGridEl) {
    maxChunksForGridEl.textContent =
      MAX_CHUNKS_FOR_GRID_DISPLAY.toLocaleString();
  }

  const zoomInButton = document.getElementById("minimapZoomInButton");
  const zoomOutButton = document.getElementById("minimapZoomOutButton");
  if (zoomInButton) {
    zoomInButton.addEventListener("click", () => {
      if (currentMinimapZoomIndex < MINIMAP_ZOOM_LEVELS.length - 1) {
        currentMinimapZoomIndex++;
        minimapSquareSize = MINIMAP_ZOOM_LEVELS[currentMinimapZoomIndex];
        renderChunkMinimap(workflowDetailsData.chunks);
      }
      updateZoomButtonStates();
    });
  }
  if (zoomOutButton) {
    zoomOutButton.addEventListener("click", () => {
      if (currentMinimapZoomIndex > 0) {
        currentMinimapZoomIndex--;
        minimapSquareSize = MINIMAP_ZOOM_LEVELS[currentMinimapZoomIndex];
        renderChunkMinimap(workflowDetailsData.chunks);
      }
      updateZoomButtonStates();
    });
  }
  updateZoomButtonStates();

  const filterCheckboxes = document.querySelectorAll(
    "#minimapFiltersContainer .form-check-input"
  );
  filterCheckboxes.forEach((checkbox) => {
    checkbox.checked = workflowDetailsData.minimapFilters[checkbox.value];
    checkbox.addEventListener("change", handleMinimapFilterChange);
  });

  const refreshButton = document.getElementById("refreshDetailsButton");
  if (refreshButton) {
    refreshButton.addEventListener("click", () => fetchWorkflowDetails(true));
  }

  fetchWorkflowDetails(true);
  setInterval(() => fetchWorkflowDetails(false), 10000); // Poll for updates every 10 seconds
}

/**
 * Fetches workflow details from the API.
 * @param {boolean} showLoading - If true, displays a loading indicator during the fetch.
 */
async function fetchWorkflowDetails(showLoading = true) {
  if (!workflowDetailsData.currentJobDetails) {
    console.error(
      "[SpatialWorkflowDetailsScript] Job details not available for fetching."
    );
    workflowDetailsData.error =
      "Initial job parameters not found. Cannot fetch details.";
    workflowDetailsData.loading = false;
    renderWorkflowDetails();
    return;
  }

  if (showLoading) {
    workflowDetailsData.loading = true;
  }
  workflowDetailsData.error = null;
  if (showLoading) renderWorkflowDetails();

  const { datastackName, workflowName, databaseName, useStagingDatabase } =
    workflowDetailsData.currentJobDetails;

  let apiUrl = `/materialize/api/v2/workflow-details/datastack/${datastackName}/workflow/${workflowName}`;
  const queryParams = [];
  if (databaseName) {
    queryParams.push(`database_name=${encodeURIComponent(databaseName)}`);
  }

  if (useStagingDatabase === true || useStagingDatabase === false) {
    queryParams.push(`use_staging_database=${useStagingDatabase}`);
  }
  if (queryParams.length > 0) {
    apiUrl += `?${queryParams.join("&")}`;
  }

  try {
    console.log(`[SpatialWorkflowDetailsScript] Fetching from ${apiUrl}`);
    const response = await fetch(apiUrl);
    if (!response.ok) {
      let errorText = `Failed to fetch workflow details. Server responded with status ${response.status}.`;
      try {
        const errorData = await response.json();
        errorText = errorData.message || JSON.stringify(errorData);
      } catch (e) {
        const rawText = await response.text();
        errorText = rawText || errorText;
        console.warn(
          "[SpatialWorkflowDetailsScript] Could not parse error response as JSON.",
          e,
          rawText
        );
      }
      throw new Error(errorText);
    }
    const data = await response.json();
    if (data.status === "success" && data.workflow_summary) {
      workflowDetailsData.summary = data.workflow_summary;

      workflowDetailsData.chunks = (data.chunks || []).sort(
        (a, b) => a.chunk_index - b.chunk_index
      );
      workflowDetailsData.statusCounts = data.status_counts || {};

      if (data.target_database) {
        const targetDbEl = document.getElementById("targetDatabase");
        if (targetDbEl && targetDbEl.textContent !== data.target_database) {
          targetDbEl.textContent = data.target_database;
        }
      }
    } else {
      throw new Error(
        data.message ||
          "API indicated failure but provided no specific message."
      );
    }
  } catch (error) {
    console.error(
      "[SpatialWorkflowDetailsScript] Error fetching workflow details:",
      error
    );
    workflowDetailsData.error = error.message;
  }
  workflowDetailsData.loading = false;
  renderWorkflowDetails();
}

function renderWorkflowDetails() {
  const loadingIndicator = document.getElementById("loadingIndicatorDetails");
  const errorState = document.getElementById("errorStateDetails");
  const errorStateMessage = document.getElementById("errorStateMessageDetails");
  const detailsContainer = document.getElementById("workflowDetailsContainer");

  const chunkGridContainer = document.getElementById("chunkGridContainer");
  const chunkMinimapContainer = document.getElementById(
    "chunkMinimapContainer"
  );
  const minimapFiltersContainer = document.getElementById(
    "minimapFiltersContainer"
  );

  const tooManyChunksMessage = document.getElementById("tooManyChunksMessage");
  const failedChunksInspector = document.getElementById(
    "failedChunksInspector"
  );
  const failedChunksListContainer = document.getElementById(
    "failedChunksListContainer"
  );
  const noFailedChunksMessage = document.getElementById(
    "noFailedChunksMessage"
  );

  const displayedChunkCountVizEl = document.getElementById(
    "displayedChunkCountViz"
  );
  const totalChunkCountVizEl = document.getElementById("totalChunkCountViz");
  const displayedChunkCountTextEl = document.getElementById(
    "displayedChunkCountText"
  );
  const totalChunkCountTextEl = document.getElementById("totalChunkCountText");

  if (
    !loadingIndicator ||
    !errorState ||
    !detailsContainer ||
    !chunkGridContainer ||
    !chunkMinimapContainer ||
    !tooManyChunksMessage ||
    !failedChunksInspector ||
    !failedChunksListContainer ||
    !noFailedChunksMessage ||
    !displayedChunkCountVizEl ||
    !totalChunkCountVizEl ||
    !displayedChunkCountTextEl ||
    !totalChunkCountTextEl ||
    !minimapFiltersContainer
  ) {
    console.error(
      "[SpatialWorkflowDetailsScript] Critical HTML elements for rendering are missing! Aborting render."
    );
    if (errorState && errorStateMessage) {
      errorState.style.display = "block";
      errorStateMessage.textContent =
        "Page rendering error: Essential UI components are missing. Please contact support or refresh.";
    }
    if (loadingIndicator) loadingIndicator.style.display = "none";
    if (detailsContainer) detailsContainer.style.display = "none";
    return;
  }

  loadingIndicator.style.display = workflowDetailsData.loading
    ? "block"
    : "none";
  errorState.style.display = workflowDetailsData.error ? "block" : "none";
  if (workflowDetailsData.error) {
    errorStateMessage.textContent = workflowDetailsData.error;
  }

  if (workflowDetailsData.loading || workflowDetailsData.error) {
    detailsContainer.style.display = "none";
    minimapFiltersContainer.style.display = "none";
    return;
  }
  detailsContainer.style.display = "block";

  document.getElementById("overallStatus").textContent =
    workflowDetailsData.summary.status || "N/A";
  document.getElementById("currentPhase").textContent =
    workflowDetailsData.summary.current_phase ||
    workflowDetailsData.summary.status ||
    "N/A";
  document.getElementById("overallProgress").textContent =
    workflowDetailsData.summary.progress !== undefined
      ? `${parseFloat(workflowDetailsData.summary.progress).toFixed(1)}%`
      : "N/A";
  document.getElementById("totalChunks").textContent =
    workflowDetailsData.summary.total_chunks !== undefined
      ? workflowDetailsData.summary.total_chunks.toLocaleString()
      : "N/A";
  document.getElementById("completedChunks").textContent =
    workflowDetailsData.summary.completed_chunks !== undefined
      ? workflowDetailsData.summary.completed_chunks.toLocaleString()
      : "N/A";
  document.getElementById("rowsProcessed").textContent =
    workflowDetailsData.summary.rows_processed !== undefined
      ? workflowDetailsData.summary.rows_processed.toLocaleString()
      : "N/A";
  document.getElementById("processingRate").textContent = workflowDetailsData
    .summary.processing_rate
    ? `${parseFloat(workflowDetailsData.summary.processing_rate).toFixed(
        1
      )} rows/sec`
    : "N/A";
  document.getElementById("estimatedCompletion").textContent =
    workflowDetailsData.summary.estimated_completion
      ? new Date(
          workflowDetailsData.summary.estimated_completion
        ).toLocaleString()
      : "N/A";
  document.getElementById("lastErrorDetails").textContent =
    workflowDetailsData.summary.last_error || "None";
  document.getElementById("startTime").textContent = workflowDetailsData.summary
    .start_time
    ? new Date(workflowDetailsData.summary.start_time).toLocaleString()
    : "N/A";
  document.getElementById("updatedTime").textContent = workflowDetailsData
    .summary.updated_at
    ? new Date(workflowDetailsData.summary.updated_at).toLocaleString()
    : "N/A";

  const countsBody = document.getElementById("chunkStatusCountsBody");
  countsBody.innerHTML = "";
  for (const [status, count] of Object.entries(
    workflowDetailsData.statusCounts
  )) {
    const tr = document.createElement("tr");
    const th = document.createElement("th");
    th.scope = "row";
    th.textContent =
      status.charAt(0).toUpperCase() +
      status.slice(1).toLowerCase().replace(/_/g, " ");
    const td = document.createElement("td");
    td.textContent = count.toLocaleString();
    tr.appendChild(th);
    tr.appendChild(td);
    countsBody.appendChild(tr);
  }

  chunkGridContainer.innerHTML = "";
  failedChunksListContainer.innerHTML = "";
  failedChunksInspector.style.display = "none";
  noFailedChunksMessage.style.display = "none";
  chunkGridContainer.style.display = "none";
  chunkMinimapContainer.style.display = "none";
  minimapFiltersContainer.style.display = "none";
  tooManyChunksMessage.style.display = "none";
  if (minimapHoverInfo)
    minimapHoverInfo.textContent = "Hover over minimap for chunk details...";

  const totalChunks = workflowDetailsData.chunks.length;
  totalChunkCountVizEl.textContent = totalChunks.toLocaleString();
  totalChunkCountTextEl.textContent = totalChunks.toLocaleString();

  if (totalChunks > MAX_CHUNKS_FOR_GRID_DISPLAY) {
    tooManyChunksMessage.style.display = "block";
    chunkMinimapContainer.style.display = "block";
    minimapFiltersContainer.style.display = "block";

    updateZoomButtonStates();
    renderChunkMinimap(workflowDetailsData.chunks);

    failedChunksInspector.style.display = "block";
    const failedChunks = workflowDetailsData.chunks.filter(
      (chunk) =>
        chunk.status === "FAILED_RETRYABLE" ||
        chunk.status === "FAILED_PERMANENT"
    );

    if (failedChunks.length > 0) {
      noFailedChunksMessage.style.display = "none";
      failedChunks.forEach((chunk) => {
        const listItem = document.createElement("button");
        listItem.type = "button";
        listItem.className = `list-group-item list-group-item-action chunk-${chunk.status}`;
        listItem.innerHTML = `<strong>Chunk ${
          chunk.chunk_index
        }</strong>: ${chunk.status.replace(
          /_/g,
          " "
        )} <small class="float-end text-muted">Click for details</small>`;
        listItem.addEventListener("click", () => showErrorModal(chunk));
        failedChunksListContainer.appendChild(listItem);
      });
    } else {
      noFailedChunksMessage.style.display = "block";
    }
  } else if (totalChunks > 0) {
    chunkGridContainer.style.display = "grid";
    displayedChunkCountVizEl.textContent = totalChunks.toLocaleString();
    displayedChunkCountTextEl.textContent = totalChunks.toLocaleString();

    workflowDetailsData.chunks.forEach((chunk) => {
      const cell = document.createElement("div");
      cell.className = `chunk-cell chunk-${chunk.status || "UNKNOWN"}`;
      cell.title = `Chunk ${chunk.chunk_index}: ${chunk.status}`;
      if (
        chunk.status === "FAILED_RETRYABLE" ||
        chunk.status === "FAILED_PERMANENT"
      ) {
        cell.style.cursor = "pointer";
        cell.addEventListener("click", () => showErrorModal(chunk));
      }
      chunkGridContainer.appendChild(cell);
    });
    failedChunksInspector.style.display = "none";
  } else {
    displayedChunkCountVizEl.textContent = "0";
    displayedChunkCountTextEl.textContent = "0";
    const noChunksMessage = document.createElement("p");
    noChunksMessage.textContent = "No chunks found for this workflow.";
    noChunksMessage.className = "text-muted mt-3";
    chunkGridContainer.appendChild(noChunksMessage);
    failedChunksInspector.style.display = "none";
  }
}

/**
 * Renders the chunk minimap on the canvas.
 * Applies filters and draws chunks based on their status and current pan/zoom.
 * @param {Array} chunks - The array of chunk objects to render.
 */
function renderChunkMinimap(chunks) {
  if (!minimapCanvas || !minimapCtx) {
    console.warn(
      "[SpatialWorkflowDetailsScript] Minimap canvas or context not available for rendering."
    );
    return;
  }

  const filteredChunks = chunks.filter(
    (chunk) => workflowDetailsData.minimapFilters[chunk.status]
  );
  const numFilteredChunks = filteredChunks.length;

  if (numFilteredChunks === 0) {
    minimapCtx.clearRect(0, 0, minimapCanvas.width, minimapCanvas.height);
    minimapCanvas.width = MINIMAP_MAX_CANVAS_WIDTH;
    minimapCanvas.height = MINIMAP_MIN_CANVAS_HEIGHT;
    if (minimapHoverInfo)
      minimapHoverInfo.textContent = "No chunks match current filters.";

    const displayedChunkCountVizEl = document.getElementById(
      "displayedChunkCountViz"
    );
    if (displayedChunkCountVizEl) displayedChunkCountVizEl.textContent = "0";
    const displayedChunkCountTextEl = document.getElementById(
      "displayedChunkCountText"
    );
    if (displayedChunkCountTextEl) displayedChunkCountTextEl.textContent = "0";

    console.log("[renderChunkMinimap] No filtered chunks to display.");
    return;
  }

  minimapCols = Math.floor(MINIMAP_MAX_CANVAS_WIDTH / minimapSquareSize);
  if (minimapCols <= 0) minimapCols = 1;

  let actualCols = Math.min(minimapCols, numFilteredChunks);
  if (actualCols <= 0) actualCols = 1;

  let actualRows = Math.ceil(numFilteredChunks / actualCols);
  if (actualRows <= 0) actualRows = 1;

  minimapCanvas.width = actualCols * minimapSquareSize;
  minimapCanvas.height = Math.min(
    actualRows * minimapSquareSize,
    MINIMAP_MAX_CANVAS_HEIGHT
  );
  minimapCanvas.height = Math.max(
    minimapCanvas.height,
    MINIMAP_MIN_CANVAS_HEIGHT
  );

  minimapCtx.clearRect(0, 0, minimapCanvas.width, minimapCanvas.height);

  console.log(
    `[renderChunkMinimap] Canvas: ${minimapCanvas.width}x${minimapCanvas.height}, SqSize: ${minimapSquareSize}, Filtered: ${numFilteredChunks}, Cols: ${actualCols}, Rows: ${actualRows}`
  );

  let drawnInViewportCount = 0;

  for (let i = 0; i < numFilteredChunks; i++) {
    const chunk = filteredChunks[i];
    const row = Math.floor(i / actualCols);
    const col = i % actualCols;
    const x = col * minimapSquareSize;
    const y = row * minimapSquareSize;

    if (
      x + minimapSquareSize > 0 &&
      x < minimapCanvas.width &&
      y + minimapSquareSize > 0 &&
      y < minimapCanvas.height
    ) {
      minimapCtx.fillStyle =
        STATUS_COLORS[chunk.status] || STATUS_COLORS["UNKNOWN"];
      minimapCtx.fillRect(x, y, minimapSquareSize, minimapSquareSize);
      drawnInViewportCount++;
    }
  }

  const displayedChunkCountVizEl = document.getElementById(
    "displayedChunkCountViz"
  );
  if (displayedChunkCountVizEl) {
    displayedChunkCountVizEl.textContent = numFilteredChunks.toLocaleString();
  }
  const displayedChunkCountTextEl = document.getElementById(
    "displayedChunkCountText"
  );
  if (displayedChunkCountTextEl) {
    displayedChunkCountTextEl.textContent = numFilteredChunks.toLocaleString();
  }

  minimapCanvas.style.cursor = "default";
}

function handleMinimapMouseMove(event) {
  if (
    !minimapCanvas ||
    !workflowDetailsData.chunks ||
    workflowDetailsData.chunks.length === 0 ||
    !minimapHoverInfo
  ) {
    if (minimapHoverInfo)
      minimapHoverInfo.textContent = "Hover over minimap for chunk details...";
    return;
  }

  const rect = minimapCanvas.getBoundingClientRect();
  const mouseX = event.clientX - rect.left;
  const mouseY = event.clientY - rect.top;

  const logicalX = mouseX;
  const logicalY = mouseY;

  const filteredChunks = workflowDetailsData.chunks.filter(
    (chunk) => workflowDetailsData.minimapFilters[chunk.status]
  );
  if (filteredChunks.length === 0) {
    minimapHoverInfo.textContent = "No chunks match current filters.";
    return;
  }

  let hitTestCols = Math.floor(minimapCanvas.width / minimapSquareSize);
  if (hitTestCols <= 0) hitTestCols = 1;
  hitTestCols = Math.min(hitTestCols, filteredChunks.length);
  if (hitTestCols <= 0) hitTestCols = 1;

  const col = Math.floor(logicalX / minimapSquareSize);
  const row = Math.floor(logicalY / minimapSquareSize);

  const filteredChunkIndex = row * hitTestCols + col;

  if (filteredChunkIndex >= 0 && filteredChunkIndex < filteredChunks.length) {
    const chunk = filteredChunks[filteredChunkIndex];
    const chunkVisualX = col * minimapSquareSize;
    const chunkVisualY = row * minimapSquareSize;

    if (
      logicalX >= chunkVisualX &&
      logicalX < chunkVisualX + minimapSquareSize &&
      logicalY >= chunkVisualY &&
      logicalY < chunkVisualY + minimapSquareSize
    ) {
      minimapHoverInfo.textContent = `Chunk ${chunk.chunk_index}: ${
        chunk.status
      } (Attempt: ${chunk.attempt_count || "N/A"})`;
    } else {
      minimapHoverInfo.textContent = "...";
    }
  } else {
    minimapHoverInfo.textContent = "Hover over minimap for chunk details...";
  }
}

function handleMinimapMouseOut() {
  if (minimapHoverInfo) {
    minimapHoverInfo.textContent = "Hover over minimap for chunk details...";
  }
}

function handleMinimapClick(event) {
  if (
    !minimapCanvas ||
    !workflowDetailsData.chunks ||
    workflowDetailsData.chunks.length === 0
  )
    return;

  const rect = minimapCanvas.getBoundingClientRect();
  const mouseX = event.clientX - rect.left;
  const mouseY = event.clientY - rect.top;

  const logicalX = mouseX;
  const logicalY = mouseY;

  const filteredChunks = workflowDetailsData.chunks.filter(
    (chunk) => workflowDetailsData.minimapFilters[chunk.status]
  );
  if (filteredChunks.length === 0) return;

  let hitTestCols = Math.floor(minimapCanvas.width / minimapSquareSize);
  if (hitTestCols <= 0) hitTestCols = 1;
  hitTestCols = Math.min(hitTestCols, filteredChunks.length);
  if (hitTestCols <= 0) hitTestCols = 1;

  const col = Math.floor(logicalX / minimapSquareSize);
  const row = Math.floor(logicalY / minimapSquareSize);
  const filteredChunkIndex = row * hitTestCols + col;

  if (filteredChunkIndex >= 0 && filteredChunkIndex < filteredChunks.length) {
    const chunk = filteredChunks[filteredChunkIndex];
    const chunkVisualX = col * minimapSquareSize;
    const chunkVisualY = row * minimapSquareSize;

    if (
      logicalX >= chunkVisualX &&
      logicalX < chunkVisualX + minimapSquareSize &&
      logicalY >= chunkVisualY &&
      logicalY < chunkVisualY + minimapSquareSize
    ) {
      if (
        chunk.status === "FAILED_RETRYABLE" ||
        chunk.status === "FAILED_PERMANENT"
      ) {
        showErrorModal(chunk);
      }
    }
  }
}

function handleMinimapWheelZoom(event) {
  event.preventDefault();
  if (
    !minimapCanvas ||
    !workflowDetailsData.chunks ||
    workflowDetailsData.chunks.length === 0
  )
    return;

  if (event.deltaY < 0) {
    if (currentMinimapZoomIndex < MINIMAP_ZOOM_LEVELS.length - 1)
      currentMinimapZoomIndex++;
  } else {
    if (currentMinimapZoomIndex > 0) currentMinimapZoomIndex--;
  }
  minimapSquareSize = MINIMAP_ZOOM_LEVELS[currentMinimapZoomIndex];
  updateZoomButtonStates();

  renderChunkMinimap(workflowDetailsData.chunks);
}

function showErrorModal(chunk) {
  if (!errorModal) {
    console.error(
      "[SpatialWorkflowDetailsScript] Error modal instance not available."
    );
    return;
  }
  document.getElementById("modalChunkIndex").textContent = chunk.chunk_index;
  document.getElementById("modalChunkStatus").textContent = chunk.status;
  document.getElementById("modalAttemptCount").textContent =
    chunk.attempt_count || "N/A";
  document.getElementById("modalErrorType").textContent =
    chunk.error_type || "N/A";
  document.getElementById("modalErrorMessage").textContent =
    chunk.error_message || "No error message provided.";
  errorModal.show();
}

function updateZoomButtonStates() {
  const zoomInButton = document.getElementById("minimapZoomInButton");
  const zoomOutButton = document.getElementById("minimapZoomOutButton");
  if (!zoomInButton || !zoomOutButton) return;

  zoomInButton.disabled =
    currentMinimapZoomIndex >= MINIMAP_ZOOM_LEVELS.length - 1;
  zoomOutButton.disabled = currentMinimapZoomIndex <= 0;
}

function handleMinimapFilterChange(event) {
  const status = event.target.value;
  if (workflowDetailsData.minimapFilters.hasOwnProperty(status)) {
    workflowDetailsData.minimapFilters[status] = event.target.checked;
  }
  if (
    workflowDetailsData.chunks.length > MAX_CHUNKS_FOR_GRID_DISPLAY &&
    minimapCanvas
  ) {
    renderChunkMinimap(workflowDetailsData.chunks);
  }
}
