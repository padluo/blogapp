#! python3
# -*- coding: utf-8 -*-

import asyncio, logging

import aiomysql

import sys

logging.basicConfig(level=logging.INFO)


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# 创建全局数据库连接池，使每个http请求都能从连接池中直接获取数据库连接
# 避免频繁地打开或关闭数据库连接
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    # 调用子协程创建全局连接池，create_pool返回一个pool实例对象
    # dict.get(key, default)
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),  # 数据库服务器的位置
        port=kw.get('port', 3306),  # mysql的端口
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],  # 当前数据库名
        charset=kw.get('charset', 'utf8'),  # 设置连接使用的编码格式为utf-8
        autocommit=kw.get('autocommit', True),  # 自动提交模式，默认是False
        # 以下三项为可选项
        # 最大连接池大小，默认是10
        maxsize=kw.get('maxsize', 10),
        # 最小连接池大小，默认是10，设为1，保证了任何时候都有一个数据库连接
        minsize=kw.get('minsize', 1),
        # 设置消息循环，何用？？？
        loop=loop
    )


async def destroy_pool():
    global __pool
    if __pool is not None:
        __pool.close()  # close()不是一个协程，所以不用yield from
        await __pool.wait_closed()  # http://aiomysql.readthedocs.io/en/latest/pool.html


async def select(sql, args, size=None):
    """
    将数据库的select操作封装在select函数中
    :param sql: sql语句
    :param args: 填入sql的选项值
    :param size: 指定最大的查询数量，不指定将返回所有查询结果
    :return:
    """
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        # 打开一个DictCursor，它与普通游标的不同在于，以dict形式返回结果
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # sql语句的占位符为"?"，mysql的占位符为"%s"，因此需要进行替换
            # 若没有指定args，将使用默认的select语句(在Metaclass内定义的)进行查询
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                # 若指定size，则打印相应数量的查询信息
                rs = await cur.fetchmany(size)
            else:
                # 未指定size，打印全部的查询信息
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs


async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        # 若数据库的事务为非自动提交的，则调用协程启动连接
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                # 增删改影响的行数
                affected = cur.rowcount
            if not autocommit:
                # 事务非自动提交型的，手动调用协程提交增删改事务
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                # 出错，回滚事务到增删改之前
                await conn.rollback()
            raise
        return affected


def create_args_string(num):
    """
    构造占位符
    :param num:
    :return:
    """
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        """
        域的初始化，包括属性(列)名，属性的类型，是否主键。
        default参数允许orm自己填入缺省值，因此具体的使用请看具体的类怎么使用
        :param name:
        :param column_type:
        :param primary_key:
        :param default:
        """
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    # 用于打印信息，依次为类名(域)名，属性类型，属性名
    def __str__(self):
        return '<%s, %s:%s>' % (
            self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    # ddl，用于定义数据类型
    def __init__(self, name=None, primary_key=False, default=None,
                 ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


"""

这是一个元类，它定义了如何来构造一个类，任何定义了__metaclass__属性或指定
了metaclass的都会通过元类定义的构造方法构造类。
任何继承自Model的类，都会自动通过ModelMetaclass扫描映射关系，并存储到自身
的类属性。
"""
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        """
        # 当前准备创建的类对象，相当于self
        :param name: 类名，如User继承自Model，当使用该元类创建User类时，name=User
        :param bases: 父类的元组
        :param attrs: 属性(方法)的字典，比如User有__table__，id等，就作为attrs的keys
        :return:
        """
        # 排除Model类本身，因为Model类就是用来被继承的，其不存在与数据库表的映射。
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 以下是针对Model的子类的处理，将被用于子类的创建，metaclass将隐式地被继承
        # 获取表名，若没有定义__table__属性，将类名作为表名，此处注意or的用法
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()  # 用字典来储存类属性与数据库表的映射关系
        fields = []  # 用于保存除主键外的属性
        primaryKey = None  # 用于保存主键
        # 遍历类的属性，找出定义的域(如StringField，字符串域)内的值，建立映射关系
        # k是属性名，v其实是定义域！请看name=StringField(ddl="varchar(100)")
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v  # 建立映射关系
                if v.primary_key:
                    # 找到主键，若主键已存在，又找到一个主键，将报错
                    if primaryKey:
                        raise StandardError(
                            'Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        # 若没有找到主键也报错，因为每张表有且仅有一个主键
        if not primaryKey:
            raise StandardError('Primary key not found.')
        # 从类属性中删除已加入映射字典的键，避免重名
        for k in mappings.keys():
            attrs.pop(k)
        # 将非主键的属性变形，放入escaped_fields中，方便增删改查语句的书写
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName  # 保存表名
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 构造默认的select, insert, update, delete语句，使用?作为占位符
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (
            primaryKey, ', '.join(escaped_fields), tableName)
        # 此处利用create_args_string生成的若干个?占位
        # 插入数据时，要指定属性名，并对应的填入属性值
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
            tableName, ', '.join(escaped_fields), primaryKey,
            create_args_string(len(escaped_fields) + 1))
        # 通过主键查找到记录并更新
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName,
                                                                   ', '.join(
                                                                       map(
                                                                           lambda
                                                                               f: '`%s`=?' % (
                                                                               mappings.get(
                                                                                   f).name or f),
                                                                           fields)),
                                                                   primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (
            tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


# ORM映射基类，继承自dict，通过ModelMetaclass元类来构造类
class Model(dict, metaclass=ModelMetaclass):
    # 初始化函数，调用父类(dict)的方法
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # 增加__getattr__方法，使获取属性更方法，即可通过"a.b"的形式
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    # 增加__setattr__方法，使设置属性更方便，可通过"a.b=c"的形式
    def __setattr__(self, key, value):
        self[key] = value

    # 通过键取值，若值不存在，返回None
    def getValue(self, key):
        return getattr(self, key, None)

    # 通过键取值，若值不存在，返回默认值
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]  # field是一个定义域！比如FloatField
            # default这个属性发挥作用
            if field.default is not None:
                # 看例子你就懂了
                # id的StringField.default=next_id,因此调用该函数生成独立id
                # FloatFiled.default=time.time数,因此调用time.time函数返回当前时间
                # 普通属性的StringField默认为None,因此还是返回None
                value = field.default() if callable(
                    field.default) else field.default
                logging.debug(
                    'using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # classmethod装饰器将方法定义为类方法
    # 对于查询相关的操作，我们都定义为类方法，就可以方便查询，而不必先创建实例再查询
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        # 我们指定的默认的select语句是通过主键查询的，并不包括where子句
        # 因此若指定有where，需要在select中追加关键字
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            # 如果limit为一个两个值的tuple，则前一个值代表索引，后一个值代表从这个索引开始要取的结果数
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        # 我们之前已将数据库的select操作封装在了select函数中，以下selet的参数依次是sql, args, size
        rs = await select(
            '%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        # 我们在定义__insert__时，将主键放在了末尾，因为属性与值要一一对应，
        # 因此通过append的方式将主键加在最后
        # 通过getValutOrDefault方法，可以调用time.time这样的函数来获取值
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        # 插入一条记录，结果影响的条数不等于1，肯定出错了
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        # 像time.time, next_id之类的函数在插入的时候已经调用过了，没有其他需要实时更新的值，因此调用getValue
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning(
                'failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning(
                'failed to remove by primary key: affected rows: %s' % rows)


if __name__ == '__main__':
    class User(Model):
        id = IntegerField('id', primary_key=True)
        username = StringField('username')
        email = StringField('email')
        password = StringField('password')


    # 创建实例
    async def test():
        await create_pool(loop=loop, host='localhost', port=3306,
                          user='root', password='Aa123456', db='test')
        user = User(id=8, username='sly', email='slysly759@gmail.com',
                    password='fuckblog')
        await user.save()
        r = await User.find(8)
        logging.info('r = %s' % r)
        await destroy_pool()


    # 创建异步事件的句柄
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
    loop.close()
    if loop.is_closed():
        sys.exit(0)
