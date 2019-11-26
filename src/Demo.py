import pymongo


# TODO 1. variable, var-kid, ref-type, dec-type, compr中，variable是多出来的吗？
#      2. Compr的设定方法：根据（ref-type，dec-type）确定compr，如果ref-type是数组 -> compr[...]，
#           ...根据var-kind（field）给值，不同的declaration之间必须匹配（提前确定，or生成并保存在map里？）（或许可以用来实现对数组内容的分析）
#      3. 实现ref-type到java type的转换（rep-type should be one of boolean, int, hashcode, double,
#           or java.lang.String; or an array of one of those (indicated by a [..] suffix)）
#      4. def-type是啥？

# {name: "xxx", fields: {"name1": type1, "name2": type2}}
# 多个collections
def get_collections(db):
    collections = []
    cols = db.list_collection_names()
    for col in cols:
        temp = {'Name: ': col}
        cursor = db[col].find()
        docs = list(cursor)  # get all documents from cursor
        field_name_set = set()
        types = {}
        # 获取所有field
        for document in docs:  # 这里的document就是一个collection里的所有数据，document.keys()包含了col里的所有key
            for key in document.keys():
                field_name_set.add(key)
        # 获取field的对应type
        for doc in docs:
            for key in field_name_set:
                try:
                    var_type = type(doc[key]).__name__
                    types[key] = var_type  # 获取type
                except KeyError:
                    continue
        temp['fields'] = types
        collections.append(temp)
    return collections


def write_decls(collections, path):
    with open(path + '.decls', 'w') as out:
        out.write('decl-version 2.0\ninput-language MongoDB\n\n')
        for col in collections:
            out.write('ppt ' + 'test.' + col['name'] + ':::POINT\n')
            out.write('  ppt-type point\n')
            for field in col['fields']:
                out.write('  variable ' + field + '\n')
                out.write('    var-kind variable ' + '\n')  # todo: array的情况 - 层级关系
                out.write('    rep-type string ' + '\n')
                out.write('    dec-type string ' + '\n')
                out.write('    flags ' + 'non_null' + '\n')
                out.write('    comparability 1' + '\n')
            out.write('\n')


if __name__ == '__main__':
    # 连接mongodb
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["SoftwareAnalysis"]  # 数据库名
    # todo：把一个collection拆成多个？
    mycol = mydb["book"]  # collection名

    collections = get_collections(mydb)
    print(collections)

    # x = mycol.find()
    # for i in x:
    #     print(i)
    # for i in x.keys():
    #     print(type(x[i]))
    # print(x)
