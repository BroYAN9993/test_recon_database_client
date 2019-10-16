from sqlalchemy import create_engine


class Database:
    def __init__(self,
                 database="postgres",
                 user="postgres",
                 password="postgres",
                 host="localhost",
                 port=5432):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = create_engine(self.connection_string)

    @property
    def connection_string(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
