from .base import GraphDB
from neo4j import GraphDatabase


class Neo4j(GraphDB):
    def __init__(self, URI, AUTH) -> None:
        self.driver = self.create_connection(URI, AUTH)
        self.driver.verify_connectivity()

    def create_connection(self, URI, AUTH):
        return GraphDatabase.driver(URI, auth=AUTH)

    def get_session(self):
        return self.driver.session()

    def close_session(self, session):
        session.close()

    def execute_read(self, tx: callable, *args, **kwargs):
        session = self.get_session()
        try:
            result = session.execute_read(tx, *args, **kwargs)
            return result
        finally:
            self.close_session(session)

    def execute_write(self, tx: callable, *args, **kwargs):
        session = self.get_session()
        try:
            session.execute_write(tx, *args, **kwargs)
        finally:
            self.close_session(session)

    def close(self):
        self.driver.close()

    def __del__(self):
        self.close()
