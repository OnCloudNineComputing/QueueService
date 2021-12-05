import pymysql
import json
import logging

from database_services.BaseDataResource import BaseDataResource
import middleware.context as context

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class RDBService(BaseDataResource):

    def __init__(self):
        super().__init__()

    @classmethod
    def get_db_connection(cls):

        db_connect_info = context.get_db_info()

        logger.info("RDBService.get_db_connection:")
        logger.info("\t HOST = " + db_connect_info['host'])

        db_info = context.get_db_info()

        db_connection = pymysql.connect(
            **db_info,
            autocommit=True
        )
        return db_connection

    @classmethod
    def run_sql(cls, sql_statement, args, fetch=False):

        conn = RDBService.get_db_connection()

        try:
            cur = conn.cursor()
            res = cur.execute(sql_statement, args=args)
            if fetch:
                res = cur.fetchall()
        except Exception as e:
            conn.close()
            raise e

        return res

    @classmethod
    def get_by_prefix(cls, db_schema, table_name, column_name, value_prefix):

        conn = RDBService.get_db_connection()
        cur = conn.cursor()

        sql = "SELECT * FROM " + db_schema + "." + table_name + " WHERE " + \
              column_name + " LIKE " + "'" + value_prefix + "%'"
        print("SQL Statement = " + cur.mogrify(sql, None))

        res = cur.execute(sql)
        res = cur.fetchall()

        conn.close()

        return res

    @classmethod
    def get_where_clause_args(cls, template):

        terms = []
        args = []
        clause = None

        if template is None or template == {}:
            clause = ""
            args = None
        else:
            for k, v in template.items():
                terms.append(k + "=%s")
                args.append(v)

            clause = " WHERE " + " AND ".join(terms)

        return clause, args

    @classmethod
    def find_by_template(cls, db_schema, table_name, template, order_by=None, limit=None, offset=None, field_list=None):

        wc, args = RDBService.get_where_clause_args(template)

        conn = RDBService.get_db_connection()
        cur = conn.cursor()

        if field_list is None:
            sql = "SELECT * FROM " + db_schema + "." + table_name + " " + wc
        else:
            sql = "SELECT " + field_list + " FROM " + db_schema + "." + \
                  table_name + " " + wc

        if order_by is not None:
            sql += " ORDER BY " + order_by
        if limit is not None:
            sql += " LIMIT " + str(limit)
        if offset is not None:
            sql += " OFFSET " + str(offset)

        res = cur.execute(sql, args=args)
        res = cur.fetchall()

        conn.close()

        return res

    @classmethod
    def delete_by_template(cls, db_schema, table_name, template):

        wc, args = RDBService.get_where_clause_args(template)

        conn = RDBService.get_db_connection()
        cur = conn.cursor()

        sql = "DELETE FROM " + db_schema + "." + table_name + " " + wc
        res = cur.execute(sql, args=args)
        res = cur.fetchall()

        sql = "ALTER TABLE " + db_schema + "." + table_name + " AUTO_INCREMENT = 1"
        cur.execute(sql)

        conn.close()

        return res

    @classmethod
    def update_by_template(cls, db_schema, table_name, data_template, where_template):

        wc, where_args = RDBService.get_where_clause_args(where_template)

        terms = []
        data_args = []
        clause = None

        if data_template is None or data_template == {}:
            clause = ""
            args = None
        else:
            for k, v in data_template.items():
                terms.append(k + "=%s")
                data_args.append(v)

        args = data_args + where_args

        conn = RDBService.get_db_connection()
        cur = conn.cursor()

        sql = "UPDATE " + db_schema + "." + table_name + " SET " + ", ".join(terms) + wc
        res = cur.execute(sql, args=args)
        res = cur.fetchall()

        conn.close()

        return res

    @classmethod
    def create(cls, db_schema, table_name, create_data):
        cols, vals, args = [], [], []

        for k, v in create_data.items():
            cols.append(k)
            vals.append('%s')
            args.append(v)

        cols_clause = "(" + ",".join(cols) + ")"
        vals_clause = "VALUES (" + ",".join(vals) + ")"

        sql_stmt = "INSERT INTO " + db_schema + "." + table_name + " " + cols_clause + \
                   " " + vals_clause

        res = RDBService.run_sql(sql_stmt, args)

        return res

