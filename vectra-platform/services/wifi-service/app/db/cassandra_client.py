from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra.query import dict_factory
from app.core.config import settings
import structlog

logger = structlog.get_logger()

class CassandraManager:
    _session = None

    @classmethod
    def get_session(cls):
        if cls._session is None:
            # Enterprise Load Balancing Policy
            # TokenAware: Sends query directly to the node holding the data
            # DCAware: Prioritizes local datacenter nodes
            lb_policy = TokenAwarePolicy(DCAwareRoundRobinPolicy())
            
            profile = ExecutionProfile(
                load_balancing_policy=lb_policy,
                request_timeout=2.0, # Fast fail
            )

            cluster = Cluster(
                contact_points=settings.CASSANDRA_HOSTS,
                port=settings.CASSANDRA_PORT,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                protocol_version=4 # Efficient binary protocol
            )
            
            cls._session = cluster.connect(settings.CASSANDRA_KEYSPACE)
            cls._session.row_factory = dict_factory
            logger.info("Connected to ScyllaDB (Enterprise Profile)")
            
        return cls._session