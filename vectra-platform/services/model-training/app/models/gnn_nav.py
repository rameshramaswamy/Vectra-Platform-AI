import torch
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv

class RoadGraphSAGE(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels):
        super().__init__()
        # GraphSAGE is excellent for inductive learning on large graphs
        self.conv1 = SAGEConv(in_channels, hidden_channels)
        self.conv2 = SAGEConv(hidden_channels, hidden_channels)
        self.classifier = torch.nn.Linear(hidden_channels, out_channels)

    def forward(self, x, edge_index):
        # x: Node features [Speed, Lanes, Historical_Park_Count]
        # edge_index: Graph connectivity
        
        h = self.conv1(x, edge_index)
        h = F.relu(h)
        h = F.dropout(h, p=0.2, training=self.training)
        
        h = self.conv2(h, edge_index)
        h = F.relu(h)
        
        # Output: Probability score for each node (road segment) being a "Parking Spot"
        out = self.classifier(h)
        return torch.sigmoid(out)