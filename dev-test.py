from TestDb.DbClient import Client

# db = Database(host="localhost", port=54321)
# meta = MetaData()
# meta.reflect(bind=db.engine)
# conn = db.engine.connect()
# table = meta.tables["tags"]
# s = select([table]).where(table.c.id == 2)
# result = conn.execute(s)
# print(result.first())
c = Client()
#
# print(c.insert_record('123', ['1', '2']))
# print(c.query_tags())
# print(c.query_ids_from_tags(['1', '2']))
# print(c.query_ids_from_tags(['2', '1']))
# print(c.query_tags_from_id(1))
print(c.query_path_from_id(1))
# print(c.query_ids_from_tags(['W']))
# print(c.update_record(1, []))
# pprint(c.query_all())
# // remove
# // update as clear
# // quary and filter
# // order tags
c.remove_record_by_id(1)