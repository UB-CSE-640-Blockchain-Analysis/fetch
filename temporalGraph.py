import networkx as nx
d = {0: {1: {"weight": 1}}, 1: {0: {"weight": 1}}}
G = nx.DiGraph()
G.add_edges_from(d)

print(list(G.edges))
print(list(G.nodes))