// Global configuration variables (from window.config)
const { globalClusters, globalNodes, globalPods, globalQueries, globalDb } =
  window.config;

// Toggle all custom query checkboxes.
document
  .getElementById("selectAllQueries")
  .addEventListener("change", function () {
    const allChecked = this.checked;
    document
      .querySelectorAll("input.queryCheckbox")
      .forEach(function (checkbox) {
        checkbox.checked = allChecked;
      });
  });

// For a single preset test.
function startBenchmark() {
  const selectElement = document.getElementById("testSelect");
  const dbSelectElement = document.getElementById("dbSelect");

  const selectedOption = selectElement.options[selectElement.selectedIndex];
  const dbSelectedOption =
    dbSelectElement.options[dbSelectElement.selectedIndex];

  const desc = selectedOption.value;
  const clustersStr = selectedOption.getAttribute("data-clusters");
  const nodesStr = selectedOption.getAttribute("data-nodes");
  const podsStr = selectedOption.getAttribute("data-pods");

  // Convert nodes and pods strings into arrays.
  const clustersArr = clustersStr.split(",").map((item) => item.trim());
  const nodesArr = nodesStr.split(",").map((item) => item.trim());
  const podsArr = podsStr.split(",").map((item) => item.trim());

  let dbVal = dbSelectedOption.value;
  const dbArr = dbVal === "All" ? globalDb : [dbVal];

  // Use the global queries array directly.
  const queriesArr = globalQueries;

  runSSE(desc, clustersArr, nodesArr, podsArr, queriesArr, dbArr);
}

// For running all tests using the full configuration.
function startAllBenchmarks() {
  const desc = "all-tests";
  // Assume globalNodes, globalPods, globalQueries, and globalDb are arrays.
  const clustersArr = globalClusters;
  const nodesArr = globalNodes;
  const podsArr = globalPods;
  const queriesArr = globalQueries;
  const dbArr = globalDb;

  document.getElementById("status").innerText = "Running all benchmarks...";
  runSSE(desc, clustersArr, nodesArr, podsArr, queriesArr, dbArr);
}

// For custom tests.
function startCustomBenchmark() {
  const customClusterStr = document.getElementById("customClusters").value;
  const customNodesStr = document.getElementById("customNodes").value;
  const customPodsStr = document.getElementById("customPods").value;
  const customDbSelectElement = document.getElementById("customDbSelect");
  const customDbSelectedOption =
    customDbSelectElement.options[customDbSelectElement.selectedIndex];

  // split inputs to arrays.
  const clustersArr = customClusterStr.split(",").map((item) => item.trim());
  const nodesArr = customNodesStr.split(",").map((item) => item.trim());
  const podsArr = customPodsStr.split(",").map((item) => item.trim());

  let dbVal = customDbSelectedOption.value;
  const dbArr = dbVal === "All" ? globalDb : [dbVal];

  // Collect selected queries from checkboxes.
  const checkboxes = document.querySelectorAll(".queryCheckbox");
  const selectedQueriesArr = [];
  checkboxes.forEach((cb) => {
    if (cb.checked) {
      selectedQueriesArr.push(cb.value);
    }
  });

  const desc = `${customNodesStr}-nodes-with-${customPodsStr}-pods`;
  runSSE(desc, clustersArr, nodesArr, podsArr, selectedQueriesArr, dbArr);
}

var cy = cytoscape({
  container: document.getElementById("diagramData"),
  elements: [], // Start empty; will update from SSE.
  style: [
    {
      selector: "node",
      style: {
        content: "data(label)",
        width: 75,
        height: 75,
        "background-color": "#0074D9",
        color: "#fff",
        "font-size": "20px",
        "text-valign": "center",
        "text-halign": "center",
        "text-outline-color": "#0074D9",
        "text-outline-width": 2,
      },
    },
    {
      selector: "edge",
      style: {
        "line-color": "#000", // Black line
        "target-arrow-color": "#000", // Black arrow
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
        content: "data(label)",
        "font-size": "18px",
        "text-rotation": "autorotate",
        color: "#000", // Black text
        "text-outline-width": 0.1, // No text outline
        // Background fill for text
        "text-background-color": "#fff", // White background for text
        "text-background-opacity": 1,
        "text-background-shape": "roundrectangle",
        "text-background-padding": 4,
      },
    },
  ],
  layout: {
    name: "circle",
    directed: true,
    spacingFactor: 1.5,
    padding: 5,
  },
});

function updateGraphStats(query_name) {
  // Retrieve the counts from the Cytoscape instance.
  const nodeCount = cy.nodes().length;
  const edgeCount = cy.edges().length;

  // Display these counts in the stats element.
  const statsElement = document.getElementById("graph-stats");
  statsElement.textContent = `Nodes: ${nodeCount} | Relationships: ${edgeCount}`;

  const queryNameElement = document.getElementById("query-name");
  queryNameElement.textContent = `${query_name}`;
}

function drawGraph(msgTuple) {
  msgData = msgTuple[0];
  query_name = msgTuple[1];

  try {
    const newElements = [];

    // Process nodes, assigning ID if missing.
    if (msgData.nodes) {
      msgData.nodes.forEach((node, index) => {
        if (!node.id) {
          node.id = "node-" + index;
        }
        newElements.push({ data: node });
      });
    }

    // Process edges, assigning ID if missing.
    if (msgData.edges) {
      msgData.edges.forEach((edge, index) => {
        if (!edge.id) {
          edge.id = "edge-" + index;
        }
        newElements.push({ data: edge });
      });
    }

    if (window.cy) {
      console.log("Instance found");
      window.cy.json({ elements: newElements });

      // Run the circle layout with consistent parameters.
      window.cy
        .layout({
          name: "circle",
          directed: true,
          spacingFactor: 1.5,
          padding: 10,
        })
        .run();

      // Update the stats after the graph is updated.
      updateGraphStats(query_name);
    } else {
      console.error("Cytoscape instance not found");
    }
  } catch (err) {
    console.error("Error processing diagram event:", err);
  }
}

function toggleDivVisibility(div) {
  if (!div) {
    console.error("No div element provided.");
    return;
  }

  // Get the current computed display style of the div.
  const currentDisplay = window.getComputedStyle(div).display;

  // Toggle display style.
  if (currentDisplay === "none") {
    div.style.display = "block";
  } else {
    div.style.display = "none";
  }
}

function runSSE(desc, clusters, nodes, pods, queries, db) {
  let url = `/run_benchmark?desc=${encodeURIComponent(desc)}`;

  clusters.forEach((c) => {
    url += `&num_clusters=${encodeURIComponent(c)}`;
  });

  nodes.forEach((n) => {
    url += `&num_nodes=${encodeURIComponent(n)}`;
  });

  pods.forEach((p) => {
    url += `&num_pods=${encodeURIComponent(p)}`;
  });

  queries.forEach((q) => {
    url += `&queries=${encodeURIComponent(q)}`;
  });

  db.forEach((d) => {
    url += `&db=${encodeURIComponent(d)}`;
  });

  const evtSource = new EventSource(url);

  // Listen for regular messages (status updates).
  evtSource.onmessage = function (e) {
    // Attempt to parse the message. If it fails, treat it as plain text.
    let handled = false;
    let data = e.data;
    try {
      const msg = JSON.parse(data);
      if (msg.type === "toggleDiv") {
        const rawDiv = msg.div;

        if (rawDiv === "all") {
          const idsToHide = ["diagramData", "dash", "deployment-progress"];
          idsToHide.forEach((id) => {
            const element = document.getElementById(id);
            if (element) {
              element.style.display = "none";
            }
          });
        }
        const div = document.getElementById(rawDiv);

        toggleDivVisibility(div);
        //
      } else if (msg.type === "diagram") {
        msgData = msg.data;
        drawGraph(msgData);
      } else if (msg.type === "scaleDeployments") {
        const progress = msg.data.progress;
        const current = msg.data.current;
        const total = msg.data.total;
        const node = msg.data.node;

        // Update the progress bar fill.
        const progressFill = document.getElementById("progress-fill");
        progressFill.style.width = progress + "%";
        progressFill.textContent = progress + "%";

        // Update additional progress text.
        const progressText = document.getElementById("progress-text");
        progressText.textContent = `Deploying ${node} (${current} of ${total} nodes)`;
      }
      // If it has a type property that's not 'diagram', do nothing here.
    } catch (err) {
      // Not JSON, so update status.
      document.getElementById("status").innerText = data;
      handled = true;
    }
    if (e.data.includes("All benchmarks completed.")) {
      evtSource.close();
    }
  };

  evtSource.onerror = function (e) {
    document.getElementById("status").innerText = "Error receiving updates.";
    evtSource.close();
  };
}

// Expose functions to the global scope if needed.
window.startBenchmark = startBenchmark;
window.startAllBenchmarks = startAllBenchmarks;
window.startCustomBenchmark = startCustomBenchmark;

// Hide divs initially
document.addEventListener("DOMContentLoaded", function () {
  const idsToHide = ["diagramData", "dash", "deployment-progress"];
  idsToHide.forEach((id) => {
    const element = document.getElementById(id);
    if (element) {
      element.style.display = "none";
    }
  });
});
