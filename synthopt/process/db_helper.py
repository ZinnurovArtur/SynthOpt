from functools import lru_cache
import pandas as pd
from typing import List, Tuple, Any
from synthopt.process.db_adapter import TrinoDBAdapter

class DBHelper:
    def __init__(self, adapter: TrinoDBAdapter):
        self.adapter = adapter

    @lru_cache(maxsize=128)
    def get_table_rowcount(self, table_name: str) -> int:
        """
        Get number of rows in a table
        
        """
        cursor = self.adapter.get_cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    @lru_cache(maxsize=128)
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names from a table
        
        """
        cursor = self.adapter.get_cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            return [desc[0] for desc in cursor.description]
        finally:
            cursor.close()

    def get_table_sample(self, table_name: str, limit: int, offset: int = 0) -> List[Tuple]:
        """
        Get sample data from a table.
        
        """
        cursor = self.adapter.get_cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name} OFFSET {offset} LIMIT {limit}")
            return cursor.fetchall()
        finally:
            cursor.close()

    @lru_cache(maxsize=128)
    def get_table_schema_sql(self, table_name: str) -> str:
        """
        Get CREATE TABLE schema SQL for a table
        
        """
        cursor = self.adapter.get_cursor()
        try:
            cursor.execute(f"SHOW CREATE TABLE {table_name}")
            create_stmt = cursor.fetchone()[0]
            # Extract column definitions between parentheses
            start = create_stmt.find("(") + 1
            end = create_stmt.rfind(")")
            return create_stmt[start:end]
        finally:
            cursor.close()

    def get_random_synthetic_alfs(self, 
                                synthetic_alfs_table: str,
                                num_alfs: int, 
                                allow_duplicates: bool = True) -> List[int]:
        """
        Get synthetic ALF_E values from a synthetic ALFs table.
        
        """
        cursor = self.adapter.get_cursor()
        try:
            if allow_duplicates:
                cursor.execute(f"""
                    SELECT alf_e 
                    FROM {synthetic_alfs_table} 
                    ORDER BY RAND() 
                    LIMIT {num_alfs}
                """)
            else:
                cursor.execute(f"""
                    SELECT DISTINCT alf_e 
                    FROM {synthetic_alfs_table} 
                    ORDER BY RAND() 
                    LIMIT {num_alfs}
                """)
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    @lru_cache(maxsize=128)
    def needs_duplicate_alfs(self, table_name: str) -> bool:
        """
        Determine if a table needs duplicate ALFs by comparing row count to distinct ALF count.
        """
        cursor = self.adapter.get_cursor()
        try:
            cursor.execute(f"""
                SELECT COUNT(*) as total, COUNT(DISTINCT alf_e) as distinct_alfs 
                FROM {table_name}
            """)
            total, distinct_alfs = cursor.fetchone()
            return total > distinct_alfs
        finally:
            cursor.close()

    def clear_cache(self):
        """Clear all cached results"""
        self.get_table_rowcount.cache_clear()
        self.get_table_columns.cache_clear()
        self.get_table_schema_sql.cache_clear()
        self.needs_duplicate_alfs.cache_clear()
