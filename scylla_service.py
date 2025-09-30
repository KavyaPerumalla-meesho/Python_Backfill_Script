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

    
    def read_data_streaming(self, table_name: str, columns: List[str], max_records: int, offset: int = 0) -> List[Dict[str, Any]]:
        """Read data in true streaming fashion for large tables - only reads what's needed."""
        try:
            # Calculate how many records we need to skip and read
            records_to_skip = offset
            records_to_read = max_records
            
            # Use a cursor-based approach for true streaming
            # This is a simplified version - in production you might want to use proper cursors
            # Note: ScyllaDB doesn't support ORDER BY without partition key restriction
            # So we'll fetch all data and apply offset/limit in memory for consistency
            select_query = f"SELECT {', '.join(columns)} FROM {table_name}"
            
            rows = self.session.execute(select_query)
            result = []
            current_record = 0
            
            for row in rows:
                # Skip records until we reach the offset
                if current_record < records_to_skip:
                    current_record += 1
                    continue
                
                # Stop if we've read enough records
                if len(result) >= records_to_read:
                    break
                
                # Convert row to dictionary
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                result.append(row_dict)
                logging.debug(f"Streaming record {current_record}: sscat_id={row_dict.get('sscat_id', 'N/A')}")
                current_record += 1
            
            return result
        except Exception as e:
            logging.error(f"Error streaming data from table '{table_name}': {e}")
            return []
    
    def read_missing_records(self, table_name: str, columns: List[str], existing_ids: List[int]) -> List[Dict[str, Any]]:
        """Read records that are missing from the target table."""
        try:
            if not existing_ids:
                return []
            
            # Create IN clause for missing IDs
            id_list = ', '.join(map(str, existing_ids))
            select_query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE sscat_id IN ({id_list})"
            
            rows = self.session.execute(select_query)
            result = []
            
            for row in rows:
                # Convert row to dictionary
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                result.append(row_dict)
                logging.debug(f"Found missing record: sscat_id={row_dict.get('sscat_id', 'N/A')}")
            
            return result
        except Exception as e:
            logging.error(f"Error reading missing records from table '{table_name}': {e}")
            return []
    
    def read_records_batch(self, table_name: str, columns: List[str], batch_size: int, last_id: int = None) -> List[Dict[str, Any]]:
        """Read records in batches using token-based pagination for consistent ordering."""
        try:
            if last_id is None:
                # First batch - get records using token-based pagination
                select_query = f"SELECT {', '.join(columns)} FROM {table_name} LIMIT {batch_size}"
            else:
                # Subsequent batches - use token-based pagination
                # Since sscat_id is partition key, we need to use token() function
                select_query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE token(sscat_id) > token({last_id}) LIMIT {batch_size}"
            
            rows = self.session.execute(select_query)
            result = []
            
            for row in rows:
                # Convert row to dictionary
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                result.append(row_dict)
            
            return result
        except Exception as e:
            logging.error(f"Error reading batch from table '{table_name}': {e}")
            return []
    
    def get_existing_ids(self, table_name: str) -> set:
        """Get all existing IDs from target table efficiently."""
        try:
            select_query = f"SELECT sscat_id FROM {table_name}"
            rows = self.session.execute(select_query)
            return {row[0] for row in rows}
        except Exception as e:
            logging.error(f"Error getting existing IDs from table '{table_name}': {e}")
            return set()
    
    def write_batch_atomic(self, table_name: str, records: List[Dict[str, Any]], columns: List[str]) -> bool:
        """Write a batch of records atomically using batch statement."""
        if not records:
            return True
            
        try:
            from cassandra.query import BatchStatement
            from cassandra import ConsistencyLevel
            
            placeholders = ', '.join(['?' for _ in columns])
            column_names = ', '.join(columns)
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            logging.debug(f"Generated query: {insert_query}")
            logging.debug(f"Records to insert: {records}")
            
            prepared_stmt = self.session.prepare(insert_query)
            
            batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            
            for record in records:
                values = [record[col] for col in columns]
                logging.debug(f"Values for record: {values}")
                batch.add(prepared_stmt, values)
            
            self.session.execute(batch)
            logging.info(f"Successfully wrote {len(records)} records atomically to {table_name}")
            return True
            
        except Exception as e:
            logging.error(f"Error writing batch to table '{table_name}': {e}")
            return False
    
    
    def get_table_count(self, table_name: str) -> int:
        """Get total count of records in table efficiently.
        
        WARNING: This can timeout on very large tables (millions+ records).
        For streaming operations, consider not using this method.
        
        NOTE: This method counts ALL records in the table, not just the ones
        being processed. Use sample-based validation for large tables instead.
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

