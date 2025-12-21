
import networkx as nx
import json
import os

# Path to topology (relative to app root or absolute)
# In Docker, app is in /app. Local is different. We try strict then relative.
TOPO_PATH = os.getenv("TOPO_PATH", "data/plant_topology.json")

class PlantGraph:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes_map = {} # physical -> logical ID
        self.load_graph()

    def load_graph(self):
        try:
            # Check absolute first, then relative
            path = TOPO_PATH
            if not os.path.exists(path):
                # Try relative to this file
                path = os.path.join(os.path.dirname(__file__), "../../../data/plant_topology.json")
            
            with open(path, "r") as f:
                data = json.load(f)
                
            for node in data['nodes']:
                self.graph.add_node(node['id'], **node)
                self.nodes_map[node['physical_id']] = node['id']
                
            for edge in data['edges']:
                self.graph.add_edge(edge[0], edge[1])
                
            print(f"✅ Plant Graph Loaded: {self.graph.number_of_nodes()} nodes, {self.graph.number_of_edges()} edges")
            
        except Exception as e:
            print(f"⚠️ Failed to load Plant Graph: {e}")
            self.graph = nx.DiGraph() # Empty fallback

    def get_upstream_dependencies(self, physical_id):
        """Returns list of upstream nodes (logical & physical) that feed into this machine."""
        if physical_id not in self.nodes_map:
            return []
            
        logical_id = self.nodes_map[physical_id]
        ancestors = list(nx.ancestors(self.graph, logical_id))
        
        # Get details
        results = []
        for aid in ancestors:
            node = self.graph.nodes[aid]
            results.append({
                "logical_id": aid,
                "label": node.get('label'),
                "physical_id": node.get('physical_id'),
                "criticality": node.get('criticality')
            })
            
        # Sort by distance? For now just list.
        return results

    def get_context(self, physical_id):
        """Returns node metadata including Criticality."""
        if physical_id not in self.nodes_map:
            return {"criticality": "C", "label": "Unknown"}
            
        lid = self.nodes_map[physical_id]
        return self.graph.nodes[lid]

# Singleton instance
plant_graph = PlantGraph()
