from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import sqlalchemy

class Server(ABC):
    
    server  : str
    database: str
    driver  : str 
    
    @abstractmethod
    def create_engine():
        pass

@dataclass
class MSSQL(Server):

    server  : str
    database: str
    driver  : str = "ODBC Driver 17 for SQL Server"

    def create_engine(self):
        return sqlalchemy.create_engine(
            f"mssql+pyodbc://{self.server}/{self.database}?driver={self.driver}"
            , fast_executemany=True)
