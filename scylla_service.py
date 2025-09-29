from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
import sys
import logging
from typing import List, Dict, Any, Optional

class ScyllaDBService:
    def __init__(self, contact_points: List[str], username: str, password: str, keyspace: str):
        """
        Initializes the ScyllaDB service, connecting to the cluster and setting a session.
        """
        self.contact_points = contact_points
        self.username = username
        self.password = password
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self._connect()

    def _connect(self) -> None:
        """
        Internal method to establish a connection to the ScyllaDB cluster.
        """
        try:
            auth_provider = PlainTextAuthProvider(self.username, self.password)
            self.cluster = Cluster(self.contact_points, auth_provider=auth_provider)
            self.session = self.cluster.connect(self.keyspace)
            
            if not self.session:
                raise ConnectionError("Failed to establish session with ScyllaDB")
                
            logging.info(f"Connection to ScyllaDB keyspace '{self.keyspace}' successful.")
        except Exception as e:
            logging.error(f"Error connecting to ScyllaDB: {e}")
            raise

    def close(self) -> None:
        if self.cluster:
            self.cluster.shutdown()
            logging.info("ScyllaDB connection closed.")

    def read_data_batch(self, table_name: str, columns: List[str], batch_size: int, last_token: str = None) -> Optional[List[Dict[str, Any]]]:
        """Read data in batches with token-based pagination for large tables."""
        try:
            if last_token is None:
                select_query = f"SELECT {', '.join(columns)} FROM {table_name} LIMIT {batch_size}"
            else:
                select_query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE token(*) > {last_token} LIMIT {batch_size}"
            
            rows = self.session.execute(select_query)
            result = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                result.append(row_dict)
            return result
        except Exception as e:
            logging.error(f"Error reading batch from table '{table_name}': {e}")
            return None
    
    def get_last_token(self, batch_data: List[Dict[str, Any]]) -> str:
        """Extract the last token from batch data for pagination."""
        if not batch_data:
            return None
        
        try:
            last_row = batch_data[-1]
            return str(hash(str(last_row)))
        except Exception as e:
            logging.error(f"Error getting last token: {e}")
            return None
    
    def get_table_count(self, table_name: str) -> int:
        """Get total count of records in table efficiently.
        
        WARNING: This can timeout on very large tables (millions+ records).
        For streaming operations, consider not using this method.
        """
        try:
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            result = self.session.execute(count_query)
            return result.one()[0]
        except Exception as e:
            logging.error(f"Error getting count for table '{table_name}': {e}")
            logging.warning(f"Count query timed out for {table_name} - this is expected for very large tables")
            return 0
    
    def get_table_schema(self, table_name: str) -> Optional[List[str]]:
        """Get column names from table schema."""
        try:
            sample_query = f"SELECT * FROM {table_name} LIMIT 1"
            result = self.session.execute(sample_query)
            sample_row = result.one()
            
            if sample_row:
                return list(sample_row._fields)
            return None
        except Exception as e:
            logging.error(f"Error getting schema for table '{table_name}': {e}")
            return None

    def write_data(self, table_name: str, rows: List[Dict[str, Any]], columns: List[str], batch_size: int = 50) -> None:
        if not rows:
            logging.warning(f"No data to write to table '{table_name}'.")
            return

        if not columns:
            logging.warning(f"No columns to write to table '{table_name}'.")
            return
        
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        prepared_stmt = self.session.prepare(insert_query)
        
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        
        try:
            for i, row in enumerate(rows):
                # Map the dictionary values to the list of columns for correct ordering
                values = [row[col] for col in columns]
                batch.add(prepared_stmt, values)
                
                if (i + 1) % batch_size == 0:
                    self.session.execute(batch)
                    batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
                    logging.info(f"  > Inserted {i + 1} rows into '{table_name}'...")
            
            # Execute the final batch if it has statements
            if len(batch) > 0:
                self.session.execute(batch)
                logging.info(f"  > Inserted all {len(rows)} rows into '{table_name}'.")
        except Exception as e:
            logging.error(f"Error writing to table '{table_name}': {e}")
