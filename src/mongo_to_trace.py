import sys
import gzip
import pymongo

def parse_str(mystr):
  mystr.replace(' ', '_')
  mystr.replace('\n', '')
  return mystr

 
def write_decls(collections, path):
  with open(path+'.decls', 'w') as out:
    out.write('decl-version 2.0\ninput-language MongoDB\n\n')
    for col in collections:
      out.write('ppt ' + 'test.' + col['name'] + ':::POINT\n')
      out.write('  ppt-type point\n')
      for field in col['fields']:
        out.write('  variable ' + field + '\n')
        out.write('    var-kind variable ' + '\n')
        out.write('    rep-type string ' + '\n')
        out.write('    dec-type string ' + '\n')
        out.write('    flags ' + 'non_null' + '\n')
        out.write('    comparability 1' + '\n')
      out.write('\n')
  
def write(db, path, level_orig, level_new):
  # collections
  collections = []
  colls = db.list_collection_names()
  for c in colls:
      temp = { 'name':c }
      cursor = db[c].find()
      field_set = set()
      for document in cursor:
        for key in document.keys():
          if key not in field_set:
            field_set.add(key)
      temp['fields'] = list(field_set)
      collections.append(temp)

  # decls
  with open(path + '.decls', 'w') as decls:
    decls.write('decl-version 2.0\ninput-language MongoDB\n\n')
    for coll in collections:
      decls.write('ppt ' + db.name + '.' + coll['name'] + ':::POINT\n')
      decls.write('  ppt-type point\n')
      for field in coll['fields']:
        decls.write('  variable ' + field + '\n')
        decls.write('    var-kind variable ' + '\n')
        decls.write('    rep-type string ' + '\n')
        decls.write('    dec-type string ' + '\n')
        decls.write('    comparability 1' + '\n')
      decls.write('\n')

  # dtrace
  with open(path + '.dtrace', 'w', encoding="utf-8") as dtrace:
    dtrace.write('decl-version 2.0\n')
    for coll in collections:
      coll_ppt = '\n{0}.{1}:::POINT\n'.format(db.name, coll['name'].replace(' ', '_')) # escaped
      cursor = db[coll['name']].find()
      
      for document in cursor:
        dtrace.write(coll_ppt)
        for field in coll['fields']:
          dtrace.write(field + '\n')
          if field in document.keys():
            if type(document[field]).__name__ == 'list':
              dtrace.write('\"' + str(document[field]).replace('\n','') + '\"\n')
            else:
              dtrace.write('\"' + str(document[field]).replace('\n','') + '\"\n')
          else:
            dtrace.write('\"' + "default" + '\"\n')            
          dtrace.write("1")
          dtrace.write("\n")
          
def get_collections(db):
  collections = []
  cols = db.list_collection_names()
  for c in cols:
      temp = { 'name':c }
      cursor = db[c].find()
      for document in cursor:
        temp['fields'] = document.keys()
      collections.append(temp)
  return collections

def write_dtrace(db, collections, path):
  out = gzip.GzipFile(path+'.dtrace.gz', 'wb', 3)
  with out:
    out.write(b'decl-version 2.0\n')    
    for col in collections:
      col_point = bytes('\n{0}.{1}:::POINT\n'.format('test', col['name'].replace(' ', '_')), 'utf-8') # escaped
      cursor = db[col['name']].find()
      for document in cursor:
        out.write(col_point)
        for field in document.keys():
          out.write(bytes(field, 'utf-8'))
          out.write(b'\n')
          out.write(bytes(str("\"" + str(document[field])) + "\"", 'utf-8'))
          out.write(b'\n')
          out.write(b'1')
          out.write(b'\n')
        

if __name__ == '__main__':
  path, host, port, database, level_orig, level_new = sys.argv[1:]
  # client = pymongo.MongoClient("localhost", 27017, maxPoolSize=50)
  client = pymongo.MongoClient(host, int(port), maxPoolSize=50)  
  db = client[database]
  # collections = get_collections(db)
  # write_decls(collections, path)
  # write_dtrace(db, collections, path)
  write(db, path, level_orig, level_new)

  # row = db['books'].find_one()
  # structure = []
  # for k, v in row.items():
  #   structure.append(get_structure(v, k, 1, int(level_orig)))
  # print(structure)
  
  # st1 = [{'name': '_id', 'type': 'int', 'level': 1}, {'name': 'title', 'type': 'str', 'level': 1}, {'name': 'isbn', 'type': 'str', 'level': 1}, {'name': 'pageCount', 'type': 'int', 'level': 1}, {'name': 'publishedDate', 'type': 'datetime', 'level': 1}, {'name': 'thumbnailUrl', 'type': 'str', 'level': 1}, {'name': 'shortDescription', 'type': 'str', 'level': 1}, {'name': 'longDescription', 'type': 'str', 'level': 1}, {'name': 'status', 'type': 'str', 'level': 1}, {'name': 'authors', 'type': 'list', 'level': 1, 'content': [{'name': 'authors[0]', 'type': 'str', 'level': 2}, {'name': 'authors[1]', 'type': 'str', 'level': 2}, {'name': 'authors[2]', 'type': 'str', 'level': 2}]}, {'name': 'categories', 'type': 'list', 'level': 1, 'content': [{'name': 'categories[0]', 'type': 'str', 'level': 2}, {'name': 'categories[1]', 'type': 'str', 'level': 2}]}]
  # st2 = [{'name': 'title', 'type': 'str', 'level': 1}, {'name': 'isbn', 'type': 'str', 'level': 1}, {'name': 'pageCount', 'type': 'int', 'level': 1}, {'name': 'publishedDate', 'type': 'datetime', 'level': 1}, {'name': 'thumbnailUrl', 'type': 'str', 'level': 1}, {'name': 'shortDescription', 'type': 'str', 'level': 1}, {'name': 'longDescription', 'type': 'str', 'level': 1}, {'name': 'status', 'type': 'str', 'level': 1}, {'name': 'authors', 'type': 'list', 'level': 1, 'content': [{'name': 'authors[0]', 'type': 'str', 'level': 2}, {'name': 'authors[1]', 'type': 'str', 'level': 2}, {'name': 'authors[2]', 'type': 'str', 'level': 2}]}, {'name': 'categories', 'type': 'list', 'level': 1, 'content': [{'name': 'categories[0]', 'type': 'str', 'level': 2}, {'name': 'categories[1]', 'type': 'str', 'level': 3}]}]
  # print([item for item in st1 if item not in st2])