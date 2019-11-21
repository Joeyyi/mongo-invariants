# import mongoengine as mo

# mo.connect('test')

# class Books(mo.Document):
#   title = mo.StringField(required=True)
#   isbn = mo.StringField(max_length=50)
#   pageCount = mo.IntField()
#   publishedDate = mo.DateField()
#   thumbnailUrl = mo.StringField()
#   shortDescription = mo.StringField()
#   longDescription = mo.StringField()
#   status = mo.StringField()
#   authors = mo.ListField(mo.StringField())
#   categories = mo.ListField(mo.StringField())
  
  
  
# for book in Books.objects:
#     print(book.title)
import sys
import gzip
import pymongo

def write_decls(collections, path):
  with open(path+'.decls', 'w') as out:
    out.write('decl-version 2.0\ninput-language MongoDB\n\n')
    for col in collections:
      out.write('ppt ' + 'test.' + col['name'] + ':::POINT\n')
      out.write('  ppt-type point\n')
      # out.write('DECLARE\n')
      # out.write(col['name'] + ':::POINT\n')

      #out.write('ppt ' + 'test.' + col['name'] + ':::POINT\n')
      #out.write('ppt-type point\n')
      for field in col['fields']:
        out.write('  variable ' + field + '\n')
        out.write('    var-kind variable ' + '\n')
        out.write('    rep-type string ' + '\n')
        out.write('    dec-type string ' + '\n')
        out.write('    flags ' + 'non_null' + '\n')
        out.write('    comparability 1' + '\n')
        
        #out.write('\n  variable ' + field + '\n')
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
  args = sys.argv[1:]
  path = args[0]
  client = pymongo.MongoClient("localhost", 27017, maxPoolSize=50)
  db = client['test']
  collections = get_collections(db)
  print(collections)
  write_decls(collections, path)
  write_dtrace(db, collections, path)


    
# collection = mon_db[col].find()

# keylist = []
# for item in collection:
#     for key in item.keys():
#         if key not in keylist:
#             keylist.append(key)
#         if isinstance(item[key], dict):
#             for subkey in item[key]:
#                 subkey_annotated = key + "." + subkey
#                 if subkey_annotated not in keylist:
#                     keylist.append(subkey_annotated)
#                     if isinstance(item[key][subkey], dict):
#                         for subkey2 in item[subkey]:
#                             subkey2_annotated = subkey_annotated + "." + subkey2
#                             if subkey2_annotated not in keylist:
#                                 keylist.append(subkey2_annotated)
#         if isinstance(item[key], list):
#             for l in item[key]:
#                 if isinstance(l, dict):
#                     for lkey in l.keys():
#                         lkey_annotated = key + ".[" + lkey + "]"
#                         if lkey_annotated not in keylist:
#                             keylist.append(lkey_annotated)
# keylist.sort()
# for key in keylist:
#     keycnt = mon_db[col].find({key:{'$exists':1}}).count()
#     print "%-5d\t%s" % (keycnt, key)