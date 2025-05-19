let runningJobsData = {
  jobs: [],
  loading: true,
  error: null,
  activePollers: {},
  jobToCancel: null,
};

function updateJobDisplay(job) {
  const jobCard = document.getElementById(`job-${job.job_id}`);
  if (!jobCard) {
    renderJobsList(runningJobsData.jobs);
    return;
  }

  jobCard.className = `card job-card h-100 status-${job.status || "idle"}`;

  const filenameEl = jobCard.querySelector(".job-filename");
  if (filenameEl) filenameEl.textContent = job.filename || job.job_id;

  const datastackEl = jobCard.querySelector(".job-datastack-name");
  if (datastackEl) datastackEl.textContent = job.datastack_name || "N/A";

  const statusEl = jobCard.querySelector(".job-status-text");
  if (statusEl) {
    statusEl.textContent = job.status
      ? job.status.charAt(0).toUpperCase() + job.status.slice(1)
      : "N/A";
    statusEl.classList.remove(
      "text-primary",
      "text-info",
      "text-success",
      "text-danger",
      "text-warning"
    );
    if (job.status === "processing") statusEl.classList.add("text-primary");
    else if (job.status === "preparing" || job.status === "pending")
      statusEl.classList.add("text-info");
    else if (job.status === "completed") statusEl.classList.add("text-success");
    else if (job.status === "error") statusEl.classList.add("text-danger");
    else if (job.status === "cancelled") statusEl.classList.add("text-warning");
  }

  const phaseEl = jobCard.querySelector(".job-phase-text");
  if (phaseEl) phaseEl.textContent = job.phase || "N/A";

  const progressBarContainer = jobCard.querySelector(
    ".job-progress-bar-container"
  );
  const progressBarEl = jobCard.querySelector(".job-progress-bar");
  const progressTextEl = jobCard.querySelector(".job-progress-text");
  const progressRowsEl = jobCard.querySelector(".job-progress-rows");

  if (
    progressBarContainer &&
    progressBarEl &&
    progressTextEl &&
    progressRowsEl
  ) {
    if (isJobActive(job.status)) {
      progressBarContainer.style.display = "block";
      progressBarEl.style.width = `${job.progress || 0}%`;
      progressBarEl.setAttribute("aria-valuenow", job.progress || 0);
      progressTextEl.textContent = `${(job.progress || 0).toFixed(1)}%`;

      if (
        job.item_type === "chunks" &&
        job.total_rows !== undefined &&
        job.total_rows !== null
      ) {
        progressRowsEl.innerHTML = `Chunks: <span>${(
          job.processed_rows || 0
        ).toLocaleString()}</span> / <span>${
          typeof job.total_rows === "number"
            ? job.total_rows.toLocaleString()
            : job.total_rows
        }</span>`;
        progressRowsEl.style.display = "block";
      } else if (
        job.item_type === "rows" &&
        job.total_rows &&
        job.total_rows !== "N/A" &&
        job.total_rows !== "Calculating..."
      ) {
        progressRowsEl.innerHTML = `Rows: <span>${(
          job.processed_rows || 0
        ).toLocaleString()}</span> / <span>${
          typeof job.total_rows === "number"
            ? job.total_rows.toLocaleString()
            : job.total_rows
        }</span>`;
        progressRowsEl.style.display = "block";
      } else if (job.item_type === "steps" || job.item_type === "done") {
        progressRowsEl.style.display = "none";
      } else {
        if (
          job.total_rows &&
          job.total_rows !== "N/A" &&
          job.total_rows !== "Calculating..."
        ) {
          progressRowsEl.innerHTML = `Rows: <span>${(
            job.processed_rows || 0
          ).toLocaleString()}</span> / <span>${
            typeof job.total_rows === "number"
              ? job.total_rows.toLocaleString()
              : job.total_rows
          }</span>`;
          progressRowsEl.style.display = "block";
        } else {
          progressRowsEl.style.display = "none";
        }
      }
    } else {
      progressBarContainer.style.display = "none";
      progressRowsEl.style.display = "none";
    }
  }

  const errorDetailsContainer = jobCard.querySelector(
    ".job-error-details-container"
  );
  const errorDetailsText = jobCard.querySelector(".job-error-details-text");
  if (errorDetailsContainer && errorDetailsText) {
    if (job.status === "error" && job.error) {
      errorDetailsText.textContent = job.error;
      errorDetailsContainer.style.display = "block";
    } else {
      errorDetailsContainer.style.display = "none";
    }
  }

  const messageContainer = jobCard.querySelector(".job-message-container");
  const messageText = jobCard.querySelector(".job-message-text");
  if (messageContainer && messageText) {
    if (
      job.message &&
      (job.status === "completed" || job.status === "cancelled")
    ) {
      messageText.textContent = job.message;
      messageContainer.style.display = "block";
    } else {
      messageContainer.style.display = "none";
    }
  }

  const lastUpdatedEl = jobCard.querySelector(".job-last-updated-text");
  if (lastUpdatedEl)
    lastUpdatedEl.textContent = job.last_updated
      ? new Date(job.last_updated).toLocaleString()
      : "N/A";

  const cancelBtn = jobCard.querySelector(".job-cancel-button");
  if (cancelBtn) {
    if (isJobActive(job.status)) {
      cancelBtn.style.display = "inline-block";
      cancelBtn.disabled = job.cancelling || false;
      cancelBtn.innerHTML = job.cancelling
        ? '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Cancelling...'
        : '<i class="fas fa-ban me-1"></i>Cancel';
    } else {
      cancelBtn.style.display = "none";
    }
  }

  let spatialDetailsBtn = jobCard.querySelector(".job-spatial-details-button");
  if (
    job.active_workflow_part === "spatial_lookup" &&
    job.spatial_lookup_config &&
    job.datastack_name
  ) {
    if (!spatialDetailsBtn) {
      spatialDetailsBtn = document.createElement("a");
      spatialDetailsBtn.className =
        "btn btn-sm btn-info job-spatial-details-button ms-2";
      spatialDetailsBtn.target = "_blank";
      spatialDetailsBtn.innerHTML =
        '<i class="fas fa-search-location me-1"></i> View Spatial Details';

      const footerActionsDiv = jobCard.querySelector(
        ".card-footer .d-flex.justify-content-end"
      );
      if (footerActionsDiv) {
        footerActionsDiv.appendChild(spatialDetailsBtn);
      }
    }
    const spatialConfig = job.spatial_lookup_config;
    let detailsUrl = `/materialize/upload/spatial-lookup/details/${job.datastack_name}/${spatialConfig.table_name}`;
    const queryParams = [];
    if (spatialConfig.database_name) {
      queryParams.push(
        `database_name=${encodeURIComponent(spatialConfig.database_name)}`
      );
    }
    if (spatialConfig.use_staging_database !== undefined) {
      queryParams.push(
        `use_staging_database=${spatialConfig.use_staging_database}`
      );
    }
    if (queryParams.length > 0) {
      detailsUrl += `?${queryParams.join("&")}`;
    }
    spatialDetailsBtn.href = detailsUrl;
    spatialDetailsBtn.style.display = "inline-block";
  } else if (spatialDetailsBtn) {
    spatialDetailsBtn.style.display = "none";
  }
}

function renderJobsList(jobs) {
  console.log("[RunningUploads] Rendering jobs list:", jobs);
  const jobsContainer = document.getElementById("jobsContainer");
  const loadingIndicator = document.getElementById("loadingIndicator");
  const emptyState = document.getElementById("emptyState");
  const errorState = document.getElementById("errorState");
  const errorStateMessage = document.getElementById("errorStateMessage");
  const jobCardTemplate = document.getElementById("jobCardTemplate");

  if (
    !jobsContainer ||
    !loadingIndicator ||
    !emptyState ||
    !errorState ||
    !errorStateMessage ||
    !jobCardTemplate
  ) {
    console.error(
      "[RunningUploads] Critical HTML elements for job rendering (including template) are missing!"
    );
    return;
  }

  loadingIndicator.style.display = runningJobsData.loading ? "block" : "none";
  errorState.style.display = runningJobsData.error ? "block" : "none";
  if (runningJobsData.error) {
    errorStateMessage.textContent = runningJobsData.error;
  }

  if (runningJobsData.loading || runningJobsData.error) {
    jobsContainer.innerHTML = "";
    emptyState.style.display = "none";
    return;
  }

  jobsContainer.innerHTML = "";

  if (jobs.length === 0) {
    emptyState.style.display = "block";
  } else {
    emptyState.style.display = "none";

    jobs.forEach((job) => {
      const templateContent = jobCardTemplate.content.cloneNode(true);
      const colDiv = templateContent.querySelector(".col");
      const cardDiv = colDiv.querySelector(".job-card");
      cardDiv.id = `job-${job.job_id}`;

      cardDiv.querySelector(".job-filename").textContent =
        job.filename || job.job_id;
      cardDiv.querySelector(".job-datastack-name").textContent =
        job.datastack_name || "N/A";

      const cancelBtn = cardDiv.querySelector(".job-cancel-button");
      cancelBtn.dataset.jobId = job.job_id;
      cancelBtn.addEventListener("click", function () {
        confirmCancelJob(this.dataset.jobId);
      });

      jobsContainer.appendChild(colDiv);
      updateJobDisplay(job);
    });
  }
}

async function fetchJobs(showLoading = true) {
  console.log("[RunningUploads] fetchJobs called. showLoading:", showLoading);
  if (showLoading) runningJobsData.loading = true;
  runningJobsData.error = null;
  renderJobsList(runningJobsData.jobs);

  try {
    console.log(
      "[RunningUploads] Fetching from /materialize/upload/api/process/user-jobs"
    );
    const response = await fetch("/materialize/upload/api/process/user-jobs");
    if (!response.ok) {
      let errorText = "Failed to fetch jobs.";
      try {
        const errorData = await response.json();
        errorText = errorData.message || JSON.stringify(errorData);
      } catch (e) {
        errorText = (await response.text()) || errorText;
      }
      throw new Error(`Server error (${response.status}): ${errorText}`);
    }
    const data = await response.json();
    if (data.status === "success") {
      const newJobs = data.jobs.map((newJob) => {
        const existingJob = runningJobsData.jobs.find(
          (j) => j.job_id === newJob.job_id
        );
        return {
          ...newJob,
          cancelling: existingJob ? existingJob.cancelling : false,
        };
      });
      runningJobsData.jobs = newJobs;
      updatePollers();
    } else {
      throw new Error(data.message || "Failed to load jobs.");
    }
  } catch (error) {
    console.error("[RunningUploads] Error fetching jobs:", error);
    runningJobsData.error = error.message;
    runningJobsData.jobs = [];
  }
  runningJobsData.loading = false;
  renderJobsList(runningJobsData.jobs);
}

function isJobActive(status) {
  return ["processing", "pending", "preparing"].includes(status?.toLowerCase());
}

function startPollingForJob(jobId) {
  if (runningJobsData.activePollers[jobId]) return;

  console.log(`[RunningUploads] Starting polling for job: ${jobId}`);
  runningJobsData.activePollers[jobId] = setInterval(async () => {
    try {
      const response = await fetch(
        `/materialize/upload/api/process/status/${jobId}`
      );
      if (!response.ok) {
        if (response.status === 404) {
          console.warn(
            `[RunningUploads] Job ${jobId} not found during polling. Stopping poller.`
          );
          updateJobInState(jobId, {
            status: "error",
            error:
              "Job details not found. It might have expired or been removed.",
            phase: "Error",
          });
          stopPollingForJob(jobId);
          return;
        }
        throw new Error(
          `Failed to get status for ${jobId} (${response.status})`
        );
      }
      const data = await response.json();
      updateJobInState(jobId, data);
      if (!isJobActive(data.status)) {
        stopPollingForJob(jobId);
      }
    } catch (error) {
      console.error(`[RunningUploads] Error polling for job ${jobId}:`, error);
      updateJobInState(jobId, {
        status: "error",
        error: "Polling failed. Could not retrieve latest status.",
        phase: "Polling Error",
      });
      stopPollingForJob(jobId);
    }
  }, 5000);
}

function stopPollingForJob(jobId) {
  if (runningJobsData.activePollers[jobId]) {
    clearInterval(runningJobsData.activePollers[jobId]);
    delete runningJobsData.activePollers[jobId];
    console.log(`[RunningUploads] Stopped polling for job: ${jobId}`);
  }
}

function updateJobInState(jobId, newData) {
  const jobIndex = runningJobsData.jobs.findIndex((j) => j.job_id === jobId);
  if (jobIndex !== -1) {
    const existingJob = runningJobsData.jobs[jobIndex];
    runningJobsData.jobs[jobIndex] = {
      ...existingJob,
      ...newData,
      filename: newData.filename || existingJob.filename,
      datastack_name: newData.datastack_name || existingJob.datastack_name,
      user_id: newData.user_id || existingJob.user_id,

      spatial_lookup_config:
        newData.spatial_lookup_config || existingJob.spatial_lookup_config,
      active_workflow_part:
        newData.active_workflow_part || existingJob.active_workflow_part,
      item_type: newData.item_type || existingJob.item_type,
    };
    updateJobDisplay(runningJobsData.jobs[jobIndex]);
  } else {
    console.warn(
      `[RunningUploads] Job ${jobId} (polled) not in local list. Adding.`
    );

    const newJob = { ...newData, job_id: jobId, cancelling: false };
    runningJobsData.jobs.push(newJob);
    renderJobsList(runningJobsData.jobs);
  }
}

function updatePollers() {
  const activeJobIds = new Set();
  runningJobsData.jobs.forEach((job) => {
    if (isJobActive(job.status)) {
      activeJobIds.add(job.job_id);
      if (!runningJobsData.activePollers[job.job_id]) {
        startPollingForJob(job.job_id);
      }
    }
  });

  Object.keys(runningJobsData.activePollers).forEach((jobId) => {
    if (!activeJobIds.has(jobId)) {
      stopPollingForJob(jobId);
    }
  });
}

window.confirmCancelJob = function (jobId) {
  runningJobsData.jobToCancel = jobId;
  if (confirm(`Are you sure you want to cancel job ID: ${jobId}?`)) {
    executeCancelJob();
  } else {
    runningJobsData.jobToCancel = null;
  }
};

async function executeCancelJob() {
  const jobId = runningJobsData.jobToCancel;
  if (!jobId) return;

  const jobIndex = runningJobsData.jobs.findIndex((j) => j.job_id === jobId);
  let jobToUpdate = null;
  if (jobIndex !== -1) {
    jobToUpdate = runningJobsData.jobs[jobIndex];
    jobToUpdate.cancelling = true;
    updateJobDisplay(jobToUpdate);
  }

  try {
    const response = await fetch(
      `/materialize/upload/api/process/cancel/${jobId}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      }
    );
    const data = await response.json();
    if (!response.ok || data.status !== "success") {
      let errorMsg = data.message || "Failed to cancel job.";
      if (data.details && data.details.error) errorMsg = data.details.error;
      throw new Error(errorMsg);
    }
    const cancelledState = {
      status: "cancelled",
      phase: data.details?.phase || "Cancelled by user",
      message: data.message,
      cancelling: false,
    };
    if (jobToUpdate) {
      Object.assign(jobToUpdate, cancelledState);
      updateJobDisplay(jobToUpdate);
    } else {
      updateJobInState(jobId, cancelledState);
    }
    stopPollingForJob(jobId);
  } catch (error) {
    console.error(`[RunningUploads] Error cancelling job ${jobId}:`, error);
    if (jobToUpdate) {
      jobToUpdate.error = error.message;
      jobToUpdate.status = "error";
      jobToUpdate.phase = "Cancellation Error";
      jobToUpdate.cancelling = false;
      updateJobDisplay(jobToUpdate);
    }
  } finally {
    runningJobsData.jobToCancel = null;
  }
}

function initRunningUploadsPage() {
  console.log(
    "[RunningUploads] Initializing page via initRunningUploadsPage()..."
  );
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", fetchJobs);
  } else {
    fetchJobs();
  }

  const refreshButton = document.getElementById("refreshJobsButton");
  if (refreshButton) {
    refreshButton.addEventListener("click", () => fetchJobs(true));
  }
}

initRunningUploadsPage();
