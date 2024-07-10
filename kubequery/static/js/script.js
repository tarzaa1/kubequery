window.addEventListener('load', () => {
    fetch('/static/data/subgraph.json')
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            const nodes = data.nodes;
            const edges = data.edges;

            const container = document.getElementById("graph");

            const orb = new Orb.Orb(container);
        
            // Initialize nodes and edges
            orb.data.setup({ nodes, edges });

            // loop over nodes, check type, get id, getNodeById, style
            for (i in nodes) {
                const node = orb.data.getNodeById(nodes[i].id);
                node.properties.fontSize = 3
                if (nodes[i].type == "K8sNode") {
                    const node = orb.data.getNodeById(nodes[i].id);
                    node.properties.color = "#FF0000";
                } else if (nodes[i].type == "Pod") {
                    const node = orb.data.getNodeById(nodes[i].id);
                    node.properties.color = "#00FF00";
                } else if (nodes[i].type == "Container") {
                    const node = orb.data.getNodeById(nodes[i].id);
                    node.properties.color = "#0000FF";
                } else if (nodes[i].type == "Image") {
                    const node = orb.data.getNodeById(nodes[i].id);
                    node.properties.color = "#03f0fc";
                }
            }

            for (i in edges) {
                const edge = orb.data.getEdgeById(edges[i].id);
                edge.properties.fontSize = 2
            }
            
            // Render and recenter the view
            orb.view.render(() => {
            orb.view.recenter();
            });
            
            // console.log("Nodes:", nodes);
            // console.log("Edges:", edges);
        })
        .catch(error => {
            console.error("Failed to load JSON file:", error);
        });
});
