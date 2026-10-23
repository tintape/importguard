// Progressive enhancement only: every page works without JavaScript.
(function () {
  "use strict";

  // Busy state for forms that start slow work (upload, retry).
  document.querySelectorAll("form[data-upload-form]").forEach(function (form) {
    form.addEventListener("submit", function () {
      var button = form.querySelector("[data-submit]");
      if (!button) return;
      button.disabled = true;
      button.classList.add("is-busy");
      var label = button.querySelector("[data-label]");
      if (label) label.textContent = form.querySelector("input[type=file]") ? "Uploading…" : "Re-queuing…";
    });
  });

  // Drag and drop onto the file picker.
  document.querySelectorAll("[data-dropzone]").forEach(function (zone) {
    var input = zone.querySelector("input[type=file]");
    ["dragenter", "dragover"].forEach(function (name) {
      zone.addEventListener(name, function (event) { event.preventDefault(); zone.classList.add("dragover"); });
    });
    ["dragleave", "drop"].forEach(function (name) {
      zone.addEventListener(name, function () { zone.classList.remove("dragover"); });
    });
    zone.addEventListener("drop", function (event) {
      event.preventDefault();
      if (input && event.dataTransfer && event.dataTransfer.files.length) input.files = event.dataTransfer.files;
    });
  });

  // Import history: poll active jobs and reload when one finishes (unless the operator is choosing a file).
  var table = document.querySelector("[data-live-jobs]");
  var ids = table ? table.getAttribute("data-live-jobs").split(",").filter(Boolean) : [];
  if (!ids.length) return;
  var fileInput = document.querySelector("input[type=file]");
  var timer = setInterval(function () {
    Promise.all(ids.map(function (id) {
      return fetch("/imports/" + id + "/status", { headers: { Accept: "application/json" } })
        .then(function (r) { return r.ok ? r.json() : null; })
        .catch(function () { return null; });
    })).then(function (results) {
      var changed = results.some(function (r) { return r && r.status !== "queued" && r.status !== "processing"; });
      var busy = fileInput && fileInput.files && fileInput.files.length;
      if (changed && !busy) { clearInterval(timer); window.location.reload(); }
    });
  }, 2000);
})();
