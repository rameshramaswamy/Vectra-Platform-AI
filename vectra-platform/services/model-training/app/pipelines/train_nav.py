import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.loader import NeighborLoader
import mlflow
import mlflow.pytorch
import structlog

from app.models.gnn_nav import RoadGraphSAGE
from app.core.config import settings

logger = structlog.get_logger()

def quantize_model(model: torch.nn.Module) -> torch.nn.Module:
    """
    Optimization: Converts Float32 weights to Int8.
    Reduces model size and speeds up CPU inference by ~2x.
    """
    model.eval()
    # 'qconfig_spec' specifies which layers to quantize (Linear layers in GraphSAGE)
    quantized_model = torch.quantization.quantize_dynamic(
        model, 
        {torch.nn.Linear}, 
        dtype=torch.qint8
    )
    return quantized_model

def train_gnn_scalable(graph_data: Data, geohash_region: str, epochs: int = 10):
    """
    Scalable Training Pipeline for large Road Networks.
    
    Args:
        graph_data: A PyG Data object containing the WHOLE city/region graph.
        geohash_region: Identifier for the region being trained.
        epochs: Number of training cycles.
    """
    logger.info("Starting GNN Training", region=geohash_region, nodes=graph_data.num_nodes)

    # 1. Scalability: Neighbor Sampling
    # Instead of processing the whole graph at once, we sample subgraphs.
    # - Batch Size: 1024 target nodes per step
    # - Neighbors: 10 from hop 1, 10 from hop 2
    train_loader = NeighborLoader(
        graph_data,
        num_neighbors=[10, 10], 
        batch_size=1024,
        shuffle=True,
        input_nodes=graph_data.train_mask # Only train on nodes with ground truth labels
    )

    # Setup Device (GPU if available, else CPU)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    # Initialize Model
    # input_channels = features (speed, lanes, historical_density)
    model = RoadGraphSAGE(
        in_channels=graph_data.num_features, 
        hidden_channels=64, 
        out_channels=1
    ).to(device)
    
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
    # 2. Training Loop
    model.train()
    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
    
    with mlflow.start_run(run_name=f"gnn_nav_{geohash_region}"):
        for epoch in range(epochs):
            total_loss = 0
            total_graphs = 0
            
            # Iterate over Mini-Batches
            for batch in train_loader:
                batch = batch.to(device)
                optimizer.zero_grad()
                
                # Forward pass on Sub-Graph
                out = model(batch.x, batch.edge_index)
                
                # Loss calculation (Binary Classification: Is Parking / Is Not)
                # We only calculate loss for the 'batch_size' center nodes, not the sampled neighbors
                loss = F.binary_cross_entropy(
                    out[:batch.batch_size], 
                    batch.y[:batch.batch_size].float().unsqueeze(1)
                )
                
                loss.backward()
                optimizer.step()
                
                total_loss += float(loss) * batch.batch_size
                total_graphs += batch.batch_size

            avg_loss = total_loss / total_graphs
            logger.info("Epoch Completed", epoch=epoch+1, loss=avg_loss)
            mlflow.log_metric("loss", avg_loss, step=epoch)

        # 3. Optimization: Quantization (Int8)
        # Move back to CPU for quantization and inference export
        model.cpu()
        quantized_model = quantize_model(model)
        
        # 4. Optimization: TorchScript (JIT) Export
        # Create dummy input to trace the graph execution path
        # Shape: [1 node, num_features]
        dummy_x = torch.randn(1, graph_data.num_features)
        # Shape: [2, 1 edge]
        dummy_edge = torch.tensor([[0], [0]], dtype=torch.long)
        
        # Trace the model to create a C++ compatible ScriptModule
        # We use the CPU model for tracing to ensure compatibility with CPU inference nodes
        scripted_model = torch.jit.trace(model, (dummy_x, dummy_edge))
        
        # 5. Logging Artifacts
        # Log the JIT model as the primary 'Production' artifact
        mlflow.pytorch.log_model(
            scripted_model, 
            artifact_path="model_jit",
            registered_model_name=f"gnn_nav_{geohash_region}"
        )
        
        # Log the quantized model for reference
        mlflow.pytorch.log_model(quantized_model, "model_quantized")

        logger.info("Training Complete. Model Exported to MLflow.")

    return scripted_model