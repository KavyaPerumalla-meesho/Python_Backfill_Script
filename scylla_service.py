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
        
        Args:
            contact_points: A list of IP addresses for the cluster nodes.
            username: The username for plain-text authentication.
            password: The password for plain-text authentication.
            keyspace: The keyspace to connect to.
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
        """
        Closes the cluster connection.
        """
        if self.cluster:
            self.cluster.shutdown()
            logging.info("ScyllaDB connection closed.")

    def read_data(self, table_name: str, columns: List[str]) -> Optional[List[Dict[str, Any]]]:
        """
        Reads all data from a specified table.
        
        Args:
            table_name: The name of the table to read from.
            columns: A list of columns to select.
            
        Returns:
            A list of rows (dictionaries) from the table, or None on error.
        """
        select_query = f"SELECT {', '.join(columns)} FROM {table_name}"
        try:
            # The driver automatically handles pagination for large results
            rows = self.session.execute(select_query)
            return [dict(row) for row in rows]
        except Exception as e:
            logging.error(f"Error reading from table '{table_name}': {e}")
            return None

    def write_data(self, table_name: str, rows: List[Dict[str, Any]], columns: List[str], batch_size: int = 50) -> None:
        """
        Writes data to a specified table using batched inserts.
        
        Args:
            table_name: The name of the table to write to.
            rows: A list of rows (dictionaries) to insert.
            columns: A list of columns corresponding to the row data.
            batch_size: The number of inserts per batch.
        """
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
