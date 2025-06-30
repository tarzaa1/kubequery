// -----------------------------------------------------
// Global Variables and Element References
// -----------------------------------------------------
const customSelectFilter = document.getElementById("customSelectFilter");
const customSelectOptions = document.getElementById("customSelectOptions");
const testCheckboxes = customSelectOptions.querySelectorAll(".test-checkbox");
const selectedOutputDropdown = document.getElementById("outputType1");

// Output Divs
const selectedTests = document.getElementById("selectedTests");

// -----------------------------------------------------
// Filter Options Based on the Input in the Filter Field
// -----------------------------------------------------
customSelectFilter.addEventListener("input", () => {
  const filterValue = customSelectFilter.value.toLowerCase();
  testCheckboxes.forEach((checkbox) => {
    const labelText = checkbox.parentElement.textContent.toLowerCase();
    // Adjust based on your HTML structure
    checkbox.parentElement.parentElement.style.display = labelText.includes(
      filterValue
    )
      ? "block"
      : "none";
  });
});
function generateSingleDataTable(dataArray) {
  if (!dataArray || dataArray.length === 0) {
    return "<p>No data available.</p>";
  }

  // Dynamically determine the keys (columns) from the first row
  const keys = Object.keys(dataArray[0]);

  let tableHTML = '<table class="border-collapse table-auto w-full">';

  // Table header
  tableHTML += "<thead><tr>";
  keys.forEach((key) => {
    tableHTML += `<th class="border border-gray-200 px-4 py-2">${key}</th>`;
  });
  tableHTML += "</tr></thead>";

  // Table body
  tableHTML += "<tbody>";
  dataArray.forEach((row) => {
    tableHTML += "<tr>";
    keys.forEach((key) => {
      let val = row[key] ?? ""; // Use nullish coalescing to handle 0/false
      if (key.toLowerCase() === "query names" && Array.isArray(val)) {
        val = `<ul>${val.map((item) => `<li>${item}</li>`).join("")}</ul>`;
      }
      tableHTML += `<td class="border border-gray-200 px-4 py-2">${val}</td>`;
    });
    tableHTML += "</tr>";
  });
  tableHTML += "</tbody></table>";

  return tableHTML;
}

function generateComparisonTable(dataArray, labels) {
  // dataArray: array of arrays, each inner array representing data for one test.
  // Flatten each inner array into a representative object (e.g. the first element)
  const flattenedData = dataArray.map((arr) => (arr.length ? arr[0] : {}));

  // Start with the keys of the first record in order.
  let keys = Object.entries(flattenedData[0]).map(([key, _]) => key);

  // remove "Test ID" from comparison.
  keys = keys.filter((key) => key !== "Test ID");

  // Append keys from subsequent records if they haven't been seen before.
  flattenedData.slice(1).forEach((record) => {
    Object.entries(record).forEach(([key, _]) => {
      if (key !== "Test ID" && keys.indexOf(key) === -1) {
        keys.push(key);
      }
    });
  });

  let html = `<table class="border-collapse table-auto w-full">
        <thead>
          <tr>
            <th class="border border-gray-200 px-4 py-2">Metric</th>`;
  // Create header for each label (each label corresponds to one test).
  labels.forEach((label) => {
    html += `<th class="border border-gray-200 px-4 py-2">${label}</th>`;
  });
  html += `</tr></thead><tbody>`;

  // Loop through each metric key.
  keys.forEach((key) => {
    // Determine highlight condition: for throughput/, bigger is better; for latency, smaller is better.
    const lowerKey = key.toLowerCase();
    let highlightCondition = null;
    if (lowerKey.includes("throughput") || lowerKey.includes("executed")) {
      highlightCondition = "bigger";
    } else if (lowerKey.includes("latency")) {
      highlightCondition = "smaller";
    }

    html += `<tr>
            <td class="border border-gray-200 px-4 py-2 font-semibold">${key}</td>`;

    // Gather values from each flattened data record for this key.
    const values = flattenedData.map((record) => record[key] || "");

    // Attempt to convert values to numbers for comparison.
    const numericValues = values.map((val) => {
      const num = parseFloat(val);
      return isNaN(num) ? null : num;
    });

    // Determine the optimum value if at least one numeric value exists.
    let optimum = null;
    if (numericValues.some((val) => val !== null)) {
      const validNumbers = numericValues.filter((val) => val !== null);
      if (highlightCondition === "bigger") {
        optimum = Math.max(...validNumbers);
      } else if (highlightCondition === "smaller") {
        optimum = Math.min(...validNumbers);
      }
    }

    // Render a cell for each value in this row.
    values.forEach((val, index) => {
      let displayVal = val;
      // If key is "Query Names" and the value is an array, render it as an unordered list.
      if (lowerKey === "query names" && Array.isArray(val)) {
        displayVal = `<ul>${val
          .map((item) => `<li>${item}</li>`)
          .join("")}</ul>`;
      }
      // Apply highlight style if numeric comparison is possible and optimum is met.
      let style = "";
      if (optimum !== null && numericValues[index] !== null) {
        if (
          (highlightCondition === "bigger" &&
            numericValues[index] === optimum) ||
          (highlightCondition === "smaller" && numericValues[index] === optimum)
        ) {
          style = "bg-green-200";
        }
      }
      html += `<td class="border border-gray-200 px-4 py-2 ${style}">${displayVal}</td>`;
    });
    html += `</tr>`;
  });

  html += `</tbody></table>`;
  return html;
}

function updateSelectedTests() {
  // Gather all checked test checkboxes
  const selected = Array.from(testCheckboxes).filter((cb) => cb.checked);

  if (selected.length === 0) {
    selectedTests.innerHTML =
      '<p class="text-gray-600 text-sm">No tests selected.</p>';
  } else {
    const detailsList = selected.map((checkbox) => {
      const description = checkbox.getAttribute("data-desc");
      const dbName = checkbox.getAttribute("data-db");
      return `
          <div class="p-2 border rounded">
            <h2 class="text-sm font-semibold text-gray-800">Test ID: ${checkbox.value}</h2>
            <p class="text-sm"><span class="font-bold">Description:</span> ${description}</p>
            <p class="text-sm"><span class="font-bold">DB Name:</span> ${dbName}</p>
          </div>
        `;
    });
    // Arrange test details in a grid with 2 columns
    selectedTests.innerHTML = `
        <div class="grid grid-cols-2 gap-4">
          ${detailsList.join("")}
        </div>
      `;
  }
  updateLoadDataButtonVisibility();
}

// Attach change listeners to test checkboxes for reactive updates
testCheckboxes.forEach((checkbox) => {
  checkbox.addEventListener("change", () => {
    updateSelectedTests();
    handleSelectedOutput();
  });
});

// -----------------------------------------------------
// Listen for Changes on the Output Dropdown
// -----------------------------------------------------
selectedOutputDropdown.addEventListener("change", () => {
  handleSelectedOutput();
  updateLoadDataButtonVisibility();
});

// -----------------------------------------------------
// Helper Function: updateLoadDataButtonVisibility
// -----------------------------------------------------
function updateLoadDataButtonVisibility() {
  const loadDataBtn = document.getElementById("loadDataBtn");
  const outputType = selectedOutputDropdown.value;
  const selectedTests = Array.from(testCheckboxes).filter((cb) => cb.checked);
  const queryCheckboxes = document.querySelectorAll(".query-checkbox");

  let queriesRequired = [
    "analyse",
    "analyse_bar_plots",
    "analyse_boxplot",
    "analyse_throughput",
  ].includes(outputType);
  let querySelected =
    queryCheckboxes.length === 0
      ? false
      : Array.from(queryCheckboxes).some((cb) => cb.checked);

  // Conditions to show load data button:
  // - Valid output type (not "selectType")
  // - At least one test is selected
  // - If queries are required, then at least one query is selected.
  let canShow = outputType !== "selectType" && selectedTests.length > 0;
  if (queriesRequired) {
    canShow = canShow && querySelected;
  }

  if (canShow) {
    loadDataBtn.classList.remove("hidden");
  } else {
    loadDataBtn.classList.add("hidden");
  }
}

// -----------------------------------------------------
// Helper Function: createDropdown
// -----------------------------------------------------
function createDropdown(containerId, options, placeholder) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`Container #${containerId} not found.`);
    return null;
  }

  // Clear out any previous content
  container.innerHTML = "";

  // Create the <select> element
  const selectEl = document.createElement("select");
  selectEl.className = "border border-gray-200 px-4 py-2 mb-4";

  // Build <option> elements
  let html = `<option value="">${placeholder}</option>`;
  options.forEach((val) => {
    html += `<option value="${val}">${val}</option>`;
  });
  selectEl.innerHTML = html;

  // Append the <select> to the container
  container.appendChild(selectEl);
  container.classList.remove("hidden");

  // Return the <select> so the caller can add event listeners if needed
  // Also update button visibility when the extra field changes
  selectEl.addEventListener("change", updateLoadDataButtonVisibility);
  return selectEl;
}

// -----------------------------------------------------
// Helper Function: displayQueries
// -----------------------------------------------------
function displayQueries(maxQueries, outputType) {
  // Get the container where the query checkboxes will be rendered.
  const queryContainer = document.getElementById("queryContainer");

  // Make sure the container is visible
  queryContainer.classList.remove("hidden");

  // Build a wrapper that includes two buttons and a flexible row for checkboxes.
  queryContainer.innerHTML = `
  <div class="flex items-center justify-between mb-2">
    <button
      id="selectAllQueries"
      class="px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700"
    >
      Select All
    </button>
    <button
      id="clearAllQueries"
      class="px-3 py-1 bg-gray-600 text-white rounded hover:bg-gray-700"
    >
      Clear All
    </button>
  </div>
  <div class="flex flex-wrap gap-4 max-h-64 overflow-y-auto" id="queryContainerInner"></div>
  <!-- Warning message area if user exceeds maxQueries -->
  <p id="queryLimitWarning" class="text-red-600 mt-2 hidden"></p>
`;

  // Grab references to newly created elements
  const queryContainerInner = document.getElementById("queryContainerInner");
  const selectAllBtn = document.getElementById("selectAllQueries");
  const clearAllBtn = document.getElementById("clearAllQueries");
  const queryLimitWarning = document.getElementById("queryLimitWarning");

  // Render each query as a checkbox + label
  globalQueries.forEach((query) => {
    // Create a container for each checkbox so we can keep them inline
    const checkItem = document.createElement("div");
    checkItem.classList.add("flex", "items-center");

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.classList.add("query-checkbox", "mr-2");
    checkbox.value = query;

    // Enforce maxQueries if needed
    checkbox.addEventListener("change", function () {
      if (maxQueries !== 0) {
        const checkedQueries = document.querySelectorAll(
          ".query-checkbox:checked"
        );
        if (checkedQueries.length > maxQueries) {
          // Uncheck the current checkbox if limit exceeded
          this.checked = false;
          // Show or update a warning message
          queryLimitWarning.textContent = `Maximum number of queries is "${maxQueries}" for "${outputType}" with the selected tests.`;
          queryLimitWarning.classList.remove("hidden");
        } else {
          // Hide warning if user is now within limit
          queryLimitWarning.classList.add("hidden");
        }
      }
      updateLoadDataButtonVisibility();
    });

    const label = document.createElement("label");
    label.textContent = query;

    // Append the checkbox and label into the item, then into the container
    checkItem.appendChild(checkbox);
    checkItem.appendChild(label);
    queryContainerInner.appendChild(checkItem);
  });

  // "Select All" button logic
  selectAllBtn.addEventListener("click", () => {
    const checkboxes = queryContainerInner.querySelectorAll(".query-checkbox");
    // If there's a limit (maxQueries), only check up to that limit
    let checkedCount = 0;
    checkboxes.forEach((cb) => {
      if (maxQueries !== 0 && checkedCount >= maxQueries) {
        return;
      }
      cb.checked = true;
      checkedCount++;
    });
    if (maxQueries !== 0 && checkedCount < checkboxes.length) {
      queryLimitWarning.textContent = `Maximum number of queries is "${maxQueries}" for "${outputType}" with the selected tests.`;
      queryLimitWarning.classList.remove("hidden");
    } else {
      queryLimitWarning.classList.add("hidden");
    }
    updateLoadDataButtonVisibility();
  });

  // "Clear All" button logic
  clearAllBtn.addEventListener("click", () => {
    const checkboxes = queryContainerInner.querySelectorAll(".query-checkbox");
    checkboxes.forEach((cb) => (cb.checked = false));
    queryLimitWarning.classList.add("hidden");
    updateLoadDataButtonVisibility();
  });
}

// -----------------------------------------------------
// Main Function: handleSelectedOutput
// -----------------------------------------------------
function handleSelectedOutput() {
  // Clear query container and extras when output type changes
  const queryContainer = document.getElementById("queryContainer");
  queryContainer.innerHTML = "";
  queryContainer.classList.add("hidden");

  const selectPercentile = document.getElementById("selectPercentile");
  selectPercentile.innerHTML = "";
  selectPercentile.classList.add("hidden");

  const selectedHops = document.getElementById("selectedHops");
  selectedHops.innerHTML = "";
  selectedHops.classList.add("hidden");

  const sortByHops = document.getElementById("sortByHops");
  sortByHops.innerHTML = "";
  sortByHops.classList.add("hidden");

  // Get the selected output type
  const outputType = selectedOutputDropdown.value;
  if (outputType === "selectType") {
    updateLoadDataButtonVisibility();
    return;
  }

  // Recalculate selected tests
  const selected = Array.from(testCheckboxes).filter((cb) => cb.checked);
  const numTests = selected.length;

  let maxQueries = 0;
  let options = [];

  // Logic based on outputType
  if (outputType === "analyse") {
    maxQueries = numTests === 1 ? 0 : 1;
    displayQueries(maxQueries, outputType);
    createDropdown(
      "selectedHops",
      [1, 2, 3, 4, 5],
      "Sort by Num Hops (All by default)"
    );
  } else if (outputType === "analyse_bar_plots") {
    displayQueries(0, outputType);
    options = ["Min", "Max", "Mean", "P50", "P75", "P95", "P99"];
    createDropdown("selectPercentile", options, "Select Percentile");
    createDropdown(
      "sortByHops",
      [1, 2, 3, 4, 5],
      "Sort by Num Hops (Optional)"
    );
  } else if (outputType == "analyse_bar_and_hops") {
    displayQueries(0, outputType);
    options = ["Min", "Max", "Mean", "P50", "P75", "P95", "P99"];
    createDropdown("selectPercentile", options, "Select Percentile");
    createDropdown(
      "selectedHops",
      [1, 2, 3, 4, 5],
      "Select Num Hops (All by default)"
    );
  } else if (outputType === "analyse_boxplot") {
    displayQueries(0, outputType);
  } else if (outputType === "analyse_throughput") {
    createDropdown(
      "selectedHops",
      [1, 2, 3, 4, 5],
      "Sort by Num Hops (All by default)"
    );

    displayQueries(0, outputType);
  } else if (outputType === "cg_plots") {
    // For cg_plots, queries are not needed.
  }
  updateLoadDataButtonVisibility();
}

// -----------------------------------------------------
// Load Data Button and fetch function using Query Params
// -----------------------------------------------------
const loadDataBtn = document.getElementById("loadDataBtn");
loadDataBtn.addEventListener("click", fetchDataWithQueryParams);

function fetchDataWithQueryParams() {
  const outputType = selectedOutputDropdown.value;

  // Gather selected test IDs (each appended separately)
  const selectedTestsArr = Array.from(testCheckboxes)
    .filter((cb) => cb.checked)
    .map((cb) => cb.value);

  // Gather selected queries (each appended separately)
  const queryCheckboxes = document.querySelectorAll(".query-checkbox");
  const selectedQueriesArr = Array.from(queryCheckboxes)
    .filter((cb) => cb.checked)
    .map((cb) => cb.value);

  // Get separate selects for percentile and hops
  const selectPercentile = document.querySelector("#selectPercentile select");
  const selectedHops = document.querySelector("#selectedHops select");
  const sortByHops = document.querySelector("#sortByHops select");
  let percentile = "";
  let numHops = "";

  if (
    outputType === "analyse" ||
    outputType === "analyse_bar_and_hops" ||
    outputType === "analyse_throughput"
  ) {
    // Just make it so if left blank, it chooses all
    numHops = selectedHops.value;
  }

  // If the output type requires a percentile (analyse_bar_plots or analyse_bar_and_hops), validate the percentile select.
  if (
    outputType === "analyse_bar_plots" ||
    outputType === "analyse_bar_and_hops"
  ) {
    if (!selectPercentile || selectPercentile.value === "") {
      const percentileContainer = document.getElementById("selectPercentile");
      percentileContainer.innerHTML += `<p class="text-red-600">Please select a percentile value.</p>`;
      return; // Stop execution until a valid percentile is selected.
    }
    percentile = selectPercentile.value;
  }
  if (outputType === "analyse_bar_plots") {
    numHops = sortByHops.value;
  }

  // Build URLSearchParams, appending list elements individually.
  const params = new URLSearchParams();
  params.append("outputType", outputType);
  selectedTestsArr.forEach((testID) => {
    params.append("testIDs", testID);
  });
  selectedQueriesArr.forEach((query) => {
    params.append("queries", query);
  });
  params.append("percentile", percentile);
  params.append("hops", numHops);

  // Debug output
  console.log("URL params:", params.toString());

  fetch(`/fetch_test_data?${params.toString()}`)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return response.json();
    })
    .then((data) => {
      console.log("Response data:", data);
      const cacheBuster = `?t=${Date.now()}`;
      const resultsContainer = document.getElementById("resultsContainer");
      const testIdsCombined = selectedTestsArr.join("_");

      // Handle errors from server
      if (data.Error) {
        console.log("Data Error:", data.Error);
        resultsContainer.innerHTML = `<p>${data.Error}</p>`;
        return;
      } else if (data.cg_plots) {
        const cpuImgUrl = `/static/images/Compare-${testIdsCombined}-cpu.png${cacheBuster}`;
        const memImgUrl = `/static/images/Compare-${testIdsCombined}-mem.png${cacheBuster}`;
        const kafkaImgUrl = `/static/images/kafka_duration_${testIdsCombined}.png${cacheBuster}`;
        const cgDurationImgUrl = `/static/images/cg_duration_${testIdsCombined}.png${cacheBuster}`;

        resultsContainer.innerHTML = `
        <div class="flex flex-col items-center">
          <h3 class="text-lg font-semibold">Memory and CPU Statistics Graphed</h3>
          <div class="flex flex-col items-center space-y-4">
            <img src="${cpuImgUrl}" alt="CPU Plot" class="max-w-full" />
            <img src="${memImgUrl}" alt="Memory Plot" class="max-w-full" />
            
            <!-- Side by side container for Kafka and CG durations -->
            <div class="flex flex-row space-x-4">
              <img src="${cgDurationImgUrl}" alt="Create Graph Duration" class="max-w-full" />
              <img src="${kafkaImgUrl}" alt="Kafka Duration" class="max-w-full" />
            </div>
          </div>
        </div>
      `;
      } else if (data.analyse_bar_plots) {
        const barPlotImgUrl = `/static/images/barplot_queries_by_${percentile}_latency_${testIdsCombined}.png${cacheBuster}`;
        resultsContainer.innerHTML = `
          <div class="flex flex-col items-center">
            <h3 class="text-lg font-semibold">
              Bar Plot Analysis for ${percentile} values
            </h3>
            <div class="flex flex-col items-center space-y-4">
              <img src="${barPlotImgUrl}" alt="Bar Plot"
                   class="max-w-full border border-gray-200" />
            </div>
          </div>
        `;
      } else if (data.analyse_bar_and_hops) {
        const barPlotImgUrl = `/static/images/barplot_k_hop_by_${percentile}_latency.png${cacheBuster}`;
        resultsContainer.innerHTML = `
          <div class="flex flex-col items-center">
            <h3 class="text-lg font-semibold">
              Bar Plot Analysis for ${percentile} values
            </h3>
            <div class="flex flex-col items-center space-y-4">
              <img src="${barPlotImgUrl}" alt="Bar Plot"
                   class="max-w-full border border-gray-200" />
            </div>
          </div>
        `;
      } else if (data.analyse_throughput) {
        const throughputUrl = `/static/images/barplot_queries_by_throughput_${testIdsCombined}.png${cacheBuster}`;
        resultsContainer.innerHTML = `
        <div class="flex flex-col items-center">
            <h3 class="text-lg font-semibold">Throughput of queries</h3>
            <div class="flex flex-col items-center space-y-4">
                 <img src="${throughputUrl}" alt="Memory Plot" class="max-w-full" />
            </div>
        </div>
      `;
      } else if (data.analyse_boxplot) {
        const boxplotUrl = `/static/images/queries_boxplot_${testIdsCombined}.png${cacheBuster}`;

        resultsContainer.innerHTML = `
          <div class="flex flex-col items-center">
            <h3 class="text-lg font-semibold">Boxplot Comparison</h3>
            <div class="p-2 border rounded">
              <img src="${boxplotUrl}" alt="Boxplot for ${testIdsCombined}" class="max-w-full" />
              <p class="text-sm mt-1">${testIdsCombined}</p>
            </div>
          </div>
        `;
      } else if (data.analyse) {
        // Use our new helper to build the table HTML
        console.log(data.analyse);
        console.log(selectedTestsArr.length);

        if (selectedTestsArr.length === 1) {
          data = data.analyse;
          const tableHTML = generateSingleDataTable(data[0]);
          resultsContainer.innerHTML = tableHTML;
        } else {
          const tableHTML = generateComparisonTable(
            data.analyse,
            selectedTestsArr
          );
          resultsContainer.innerHTML = tableHTML;
        }
      }
    })
    .catch((error) => {
      console.error("Error fetching data:", error);
      const resultsContainer = document.getElementById("resultsContainer");
      resultsContainer.innerHTML = `<p class="text-red-600">An error occurred while fetching data. Check console for details.</p>`;
    });
}
