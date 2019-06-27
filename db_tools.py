# -*- coding: utf-8 -*-
# @Time    : 2019-06-27 15:52
# @Author  : Henson
# @Email   : henson_wu@foxmail.com
# @File    : db_tools.py
import pymysql
from gevent.pool import Pool
from gevent import monkey

monkey.patchall()


class Field(object):
    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type

    def __str__(self):
        return "<%s:%s>" % (self.name, self.column_type)


class ModelMetaClass(type):
    def __new__(cls, name, bases, attrs):
        if name == "Model":
            return type.__new__(cls, name, bases, attrs)

        print('Found model: %s' % name)
        mapping = dict()  # 空字典
        for k, v in attrs.items():
            print('key:%s,value:%s' % (k, v))
            if isinstance(v, Field):
                mapping[k] = v

        for k in mapping.keys():
            attrs.pop(k)
        attrs["__mapping__"] = mapping
        attrs["__table__"] = name
        return type.__new__(cls, name, bases, attrs)


class DBConnect(dict, metaclass=ModelMetaClass):
    def __init__(self, **kwargs):
        self.host = kwargs.get('host', 'localhost'),
        self.user = kwargs.get('user', 'user'),
        self.password = kwargs.get('password', ''),
        self.database = kwargs.get('db', ''),
        self.connections = kwargs.get('maxconnections', 20)
        self.pool = Pool(self.connections)

    def pre_conect(self, **kwargs):
        self.db = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.db.cursor()
        super().__init__(**kwargs)

    def connect(self):
        self.pool.map(self.pre_conect, {
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'database': self.database
        })

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def __del__(self):
        '''
        回收内存
        '''
        self.db.commit()
        self.cursor.close()
        self.db.close()

    def save(self):
        fields = []
        args = []
        for k, v in self.__mapping__.items():
            fields.append(v.name)
            args.append(getattr(self, k, None))
        sql = "insert into %s(%s) values (%s)" % (
            self.__table__,
            ",".join(fields),
            ",".join([repr(str(i)) for i in args]
                     ))  # sql拼接
        return self.cursor.execute(sql)

    def filter(self):
        fields = []
        args = []
        query_string = ""
        for k, v in self.__mapping__.items():
            fields.append(v.name)
            args.append(getattr(self, k, None))
            query_string += " %s=%s "
        query_string = query_string[:-3]

        sql = "select %s from  %s where  (%s)" % (
            ",".join(fields),
            self.__table__,
            query_string)
        return self.cursor.execute(sql)
