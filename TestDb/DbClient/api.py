from typing import List

from sqlalchemy import MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import and_

from TestDb.Database import Database
from TestDb.DbClient.utils import stmt_exec


class Client:
    def __init__(self,
                 database="test_db",
                 host="192.168.1.185",
                 port=54321):
        self.db = Database(database=database, host=host, port=port)
        self.engine = self.db.engine
        self.meta = MetaData()
        self.meta.reflect(bind=self.engine)
        self.session = sessionmaker(bind=self.engine)
        self.records_table = self.meta.tables["records"]
        self.tags_table = self.meta.tables["tags"]
        self.record_tags_table = self.meta.tables["record_tags"]

    def query_path_from_id(self, id: int) -> str or None:
        select_stmt = select([self.records_table.c.path]).where(self.records_table.c.id == id)
        select_result = stmt_exec(self.engine, select_stmt)
        value = select_result.first()
        return value[0] if value else value

    def query_tags_from_id(self, id: int) -> List[str]:
        select_stmt = select([self.tags_table.c.tag])\
            .where(self.tags_table.c.id.in_(select([self.record_tags_table.c.tag_id])\
                   .where(self.record_tags_table.c.record_id == id)))
        select_result = stmt_exec(self.engine, select_stmt)
        return [x[0] for x in select_result]

    def query_ids_from_tags(self, tags: List[str]) -> List[int]:
        ids = set()
        for tag in tags:
            select_stmt = select([self.record_tags_table.c.record_id])\
                .where(self.record_tags_table.c.tag_id.in_(select([self.tags_table.c.id])\
                        .where(self.tags_table.c.tag == tag)))
            select_result = stmt_exec(self.engine, select_stmt)
            if ids:
                ids = ids & set([x[0] for x in select_result])
            else:
                ids = set([x[0] for x in select_result])
        return list(ids)

    def query_tags(self) -> List[str]:
        select_stmt = select([self.tags_table.c.tag])
        select_result = stmt_exec(self.engine, select_stmt)
        return [x[0] for x in select_result]

    def query_all(self):
        record_select_stmt = select([self.records_table])
        record_select_result = stmt_exec(self.engine, record_select_stmt)
        ans = [list((x[0], x[1], x[2], self.query_tags_from_id(x[0]))) for x in record_select_result]
        return ans

    def insert_record(self, path: str, tags: List[str]) -> int:
        connection = self.engine.connect()
        trans = connection.begin()
        try:
            tag_ids = []
            for tag in tags:
                tag_ids.append(self._insert_tag(tag))
            record_id = self._insert_record(path)
            for tag_id in tag_ids:
                self._insert_record_tag(record_id, tag_id)
            return record_id
        except Exception as ex:
            trans.rollback()
            raise ex

    def update_record(self, id: int, tags: List[str]):
        connection = self.engine.connect()
        trans = connection.begin()
        try:
            delete_stmt = self.record_tags_table.delete()\
                .where(self.record_tags_table.c.record_id == id)
            self.engine.connect().execute(delete_stmt)
            tag_ids = []
            for tag in tags:
                tag_ids.append(self._insert_tag(tag))
            for tag_id in tag_ids:
                self._insert_record_tag(id, tag_id)
            print("Update Done")
        except Exception as ex:
            trans.rollback()
            raise ex

    def remove_record_by_id(self, id: int):
        connection = self.engine.connect()
        trans = connection.begin()
        try:
            delete_record_tag_stmt = self.record_tags_table.delete()\
                .where(self.record_tags_table.c.record_id == id)
            self.engine.execute(delete_record_tag_stmt)
            delete_record_stmt = self.records_table.delete() \
                .where(self.records_table.c.id == id)
            self.engine.execute(delete_record_stmt)
            print("Delete Done")
        except Exception as ex:
            trans.rollback()
            raise ex

    def _insert_tag(self, tag: str) -> int:
        select_stmt = select([self.tags_table.c.id]).where(self.tags_table.c.tag == tag)
        select_result = self.engine.connect().execute(select_stmt)
        value = select_result.first()
        if value:
            return value[0]
        insert_stmt = self.tags_table.insert().values(tag=tag).returning(self.tags_table.c.id)
        insert_result = self.engine.connect().execute(insert_stmt)
        return insert_result.first()[0]

    def _insert_record(self, path: str) -> int:
        insert_stmt = self.records_table.insert().values(path=path).returning(self.records_table.c.id)
        insert_result = self.engine.connect().execute(insert_stmt)
        return insert_result.first()[0]

    def _insert_record_tag(self, record_id: int, tag_id: int):
        select_stmt = select([self.record_tags_table])\
            .where(and_(self.record_tags_table.c.tag_id == tag_id,
                   self.record_tags_table.c.record_id == record_id))
        select_result = self.engine.connect().execute(select_stmt)
        value = select_result.first()
        if value:
            return
        insert_stmt = self.record_tags_table.insert().values(record_id=record_id, tag_id=tag_id)
        self.engine.connect().execute(insert_stmt)
