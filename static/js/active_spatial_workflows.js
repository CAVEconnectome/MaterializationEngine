let activeWorkflowsData = {
  workflows: [],
  loading: true,
  error: null,
};

function getStatusColorClass(status) {
  if (!status) return "status-idle";
  const s = status.toLowerCase();
  if (
    s.includes("processing") ||
    s.includes("running") ||
    s.includes("pending") ||
    s.includes("initializing") ||
    s.includes("resuming") ||
    s.includes("processing_chunks")
  ) {
    return "status-processing";
  }
  if (s.includes("completed")) {
    return "status-completed";
  }
  if (s.includes("failed") || s.includes("error")) {
    return "status-failed";
  }
  return "status-idle";
}

function renderActiveWorkflowsList(workflows) {
  console.log("[ActiveSpatialWorkflows] Rendering workflows list:", workflows);
  const workflowsContainer = document.getElementById("workflowsContainer");
  const loadingIndicator = document.getElementById("loadingIndicator");
  const emptyState = document.getElementById("emptyState");
  const errorState = document.getElementById("errorState");
  const errorStateMessage = document.getElementById("errorStateMessage");
  const workflowCardTemplate = document.getElementById("workflowCardTemplate");

  if (
    !workflowsContainer ||
    !loadingIndicator ||
    !emptyState ||
    !errorState ||
    !errorStateMessage ||
    !workflowCardTemplate
  ) {
    console.error(
      "[ActiveSpatialWorkflows] Critical HTML elements for workflow rendering (including template) are missing!"
    );
    return;
  }

  loadingIndicator.style.display = activeWorkflowsData.loading
    ? "block"
    : "none";
  errorState.style.display = activeWorkflowsData.error ? "block" : "none";
  if (activeWorkflowsData.error) {
    errorStateMessage.textContent = activeWorkflowsData.error;
  }

  if (activeWorkflowsData.loading || activeWorkflowsData.error) {
    workflowsContainer.innerHTML = "";
    emptyState.style.display = "none";
    return;
  }

  if (workflows.length === 0) {
    emptyState.style.display = "block";
    workflowsContainer.innerHTML = "";
  } else {
    emptyState.style.display = "none";
    workflowsContainer.innerHTML = "";

    workflows.forEach((workflow) => {
      const templateContent = workflowCardTemplate.content.cloneNode(true);
      const colDiv = templateContent.querySelector(".col");
      const cardDiv = colDiv.querySelector(".workflow-card");

      cardDiv.id = `workflow-${workflow.table_name}-${workflow.database_name}`;
      cardDiv.className = `card workflow-card h-100 ${getStatusColorClass(
        workflow.status
      )}`;

      cardDiv.querySelector(".workflow-card-header").textContent =
        workflow.table_name;
      cardDiv.querySelector(".workflow-datastack-name").textContent =
        workflow.datastack_name || "N/A";
      cardDiv.querySelector(".workflow-database-name").textContent =
        workflow.database_name || "N/A";
      cardDiv.querySelector(".workflow-status-text").textContent =
        workflow.status || "N/A";

      const progressSection = cardDiv.querySelector(
        ".workflow-progress-section"
      );
      const progressBarContainer = cardDiv.querySelector(
        ".workflow-progress-bar-container"
      );
      const progressBar = cardDiv.querySelector(".workflow-progress-bar");
      const progressText = cardDiv.querySelector(".workflow-progress-text");
      const progressChunks = cardDiv.querySelector(".workflow-progress-chunks");
      const noChunkInfo = cardDiv.querySelector(".workflow-no-chunk-info");

      let progressPercent = 0;
      if (workflow.total_chunks > 0 && workflow.completed_chunks >= 0) {
        progressPercent =
          (workflow.completed_chunks / workflow.total_chunks) * 100;
        progressBarContainer.style.display = "block";
        noChunkInfo.style.display = "none";
        progressBar.style.width = `${progressPercent.toFixed(1)}%`;
        progressBar.setAttribute("aria-valuenow", progressPercent.toFixed(1));
        progressText.textContent = `${progressPercent.toFixed(1)}%`;
        progressChunks.textContent = `Chunks: ${
          workflow.completed_chunks?.toLocaleString() || 0
        } / ${workflow.total_chunks?.toLocaleString()}`;
      } else {
        progressBarContainer.style.display = "none";
        noChunkInfo.style.display = "block";
      }

      const errorContainer = cardDiv.querySelector(
        ".workflow-error-details-container"
      );
      const errorTextEl = cardDiv.querySelector(".workflow-error-details-text");
      if (workflow.last_error) {
        errorTextEl.textContent = workflow.last_error;
        errorContainer.style.display = "block";
      } else {
        errorContainer.style.display = "none";
      }

      cardDiv.querySelector(".workflow-start-time").textContent =
        workflow.start_time
          ? new Date(workflow.start_time).toLocaleString()
          : "N/A";
      cardDiv.querySelector(".workflow-last-updated").textContent =
        workflow.updated_at
          ? new Date(workflow.updated_at).toLocaleString()
          : "N/A";

      const detailsButton = cardDiv.querySelector(".workflow-details-button");
      let detailsUrl = `/materialize/upload/spatial-lookup/details/${workflow.datastack_name}/${workflow.table_name}`;
      const queryParams = [];
      if (workflow.database_name) {
        queryParams.push(
          `database_name=${encodeURIComponent(workflow.database_name)}`
        );
      }
      if (queryParams.length > 0) {
        detailsUrl += `?${queryParams.join("&")}`;
      }
      detailsButton.href = detailsUrl;

      colDiv.appendChild(cardDiv);

      workflowsContainer.appendChild(templateContent);
    });
  }
}

async function fetchActiveWorkflows(showLoading = true) {
  console.log(
    "[ActiveSpatialWorkflows] fetchActiveWorkflows called. showLoading:",
    showLoading
  );
  if (showLoading) activeWorkflowsData.loading = true;
  activeWorkflowsData.error = null;
  renderActiveWorkflowsList(activeWorkflowsData.workflows);

  try {
    const response = await fetch("/materialize/api/v2/all-active-direct");

    if (!response.ok) {
      let errorText = "Failed to fetch active spatial workflows.";

      const responseBodyText = await response.text();
      try {
        const errorData = JSON.parse(responseBodyText);
        errorText = errorData.message || JSON.stringify(errorData);
      } catch (e) {
        errorText = responseBodyText || errorText;
      }
      throw new Error(`Server error (${response.status}): ${errorText}`);
    }

    const data = await response.json();

    if (data.status === "success" && Array.isArray(data.workflows)) {
      activeWorkflowsData.workflows = data.workflows;
    } else if (Array.isArray(data.workflows)) {
      activeWorkflowsData.workflows = data.workflows;
    } else {
      throw new Error(
        data.message ||
          "Failed to load active workflows. Unexpected API response format."
      );
    }
  } catch (error) {
    console.error("[ActiveSpatialWorkflows] Error fetching workflows:", error);
    activeWorkflowsData.error = error.message;
    activeWorkflowsData.workflows = [];
  }
  activeWorkflowsData.loading = false;
  renderActiveWorkflowsList(activeWorkflowsData.workflows);
}

function initActiveSpatialWorkflowsPage() {
  console.log(
    "[ActiveSpatialWorkflows] Initializing page via initActiveSpatialWorkflowsPage()..."
  );
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () =>
      fetchActiveWorkflows(true)
    );
  } else {
    fetchActiveWorkflows(true);
  }

  const refreshButton = document.getElementById("refreshWorkflowsButton");
  if (refreshButton) {
    refreshButton.addEventListener("click", () => fetchActiveWorkflows(true));
  }

  // Set up polling
  setInterval(() => fetchActiveWorkflows(false), 15000); // Poll every 15 seconds
}

initActiveSpatialWorkflowsPage();
