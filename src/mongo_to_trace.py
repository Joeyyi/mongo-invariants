import sys
import gzip
import pymongo

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
  path, host, port, database, level_orig, level_new = sys.argv
  # client = pymongo.MongoClient("localhost", 27017, maxPoolSize=50)
  client = pymongo.MongoClient(host, int(port), maxPoolSize=50)  
  db = client[database]
  collections = get_collections(db)
  write_decls(collections, path)
  write_dtrace(db, collections, path)
  