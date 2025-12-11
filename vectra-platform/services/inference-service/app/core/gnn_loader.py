import torch
import mlflow.pytorch

class GNNInferenceEngine:
    def __init__(self):
        # Load JIT model
        self.model = mlflow.pytorch.load_model("models:/gnn_nav_jit/Production")
        self.model.eval()

    def predict(self, features, edge_index):
        # No_grad context is faster
        with torch.no_grad():
            # Running optimized TorchScript
            # tensors should be prepared beforehand
            x = torch.tensor(features, dtype=torch.float32)
            edge = torch.tensor(edge_index, dtype=torch.long)
            
            probability = self.model(x, edge)
            return probability.item()