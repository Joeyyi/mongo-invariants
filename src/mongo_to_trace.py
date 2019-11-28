import sys
import gzip
import pymongo

from datatype_map import to_rep_type

# string preprocessing for dtrace
def parse_str(mystr, escaped=False, single_line=False, quoted=False):
  if type(mystr) is not str:
    mystr = str(mystr)
  if escaped:
    mystr = mystr.replace(' ', '_')
  if single_line:
    mystr = mystr.replace('\n', '')
  if quoted:
    mystr = '\"' + mystr + '\"'
  return mystr
  

# merge field structures
def merge_structure(s1, s2):
  s3 = s1
  for i in range(len(s2)):
    f = False
    key_i = s2[i]
    for j in range(len(s3)):
      key_j = s3[j]
      if key_i['name'] == key_j['name']:
        f = True
        if 'length' in key_i and 'length' in key_j:
          s3[j]['length'] = max(key_i['length'], key_j['length'])
        elif 'content' in key_i and 'content' in key_j:
            s3[j]['content'] = merge_structure(key_i['content'], key_j['content'])
    if not f:
      s3.append(key_i)
  return s3

# generate field sturcture from field
def get_structure(field, name_field, level, target_level):
  if type(field) is list:
    res = {'name': name_field, 'type': 'list', 'level': level}
    if level < target_level:
      res['content'] = []
      for i in range(len(field)):
        sub_field = field[i]
        res['content'].append(get_structure(sub_field, '{0}[{1}]'.format(name_field, i), level + 1, target_level))
    else:
      res['length'] = len(field)
    return res
  if type(field) is dict:
    res = {'name': name_field, 'type': 'dict', 'level': level}
    if level < target_level:
      res['content'] = []
      for sub_field_k, sub_field_v in field.items():
        res['content'].append(get_structure(sub_field_v, '{0}<{1}>'.format(name_field, sub_field_k), level + 1, target_level))
    else: 
        res['length'] = len(field.keys())
    return res    
  else:
    return {'name': name_field, 'type': type(field).__name__, 'level': level}

  
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
  
def write1(db, path, level_orig, level_new):
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
          dtrace.write("1") # TODO mod
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
  
def get_collections3(db, level_orig):
  collections = []
  colls = db.list_collection_names()
  for c in colls:
    structure = []
    cursor = db[c].find()
    for doc in cursor:
      st = []
      for k, v in doc.items():
        st.append(get_structure(v, k, 1, level_orig))
      structure = merge_structure(structure, st)
    collections.append({'name': c, 'fields': structure})
  return collections

def write_decls3(collections, path):
  with open(path + '.decls', 'w') as decls:
    def write_var(writer, name, kind, rep, dec, comp):
        writer.write(' variable {0}\n'.format(name))
        writer.write('  var-kind {0}\n'.format(kind))
        writer.write('  rep-type {0}\n'.format(rep))
        writer.write('  dec-type {0}\n'.format(dec))
        writer.write('  comparability {0}\n'.format(comp))
    decls.write('decl-version 2.0\ninput-language MongoDB\n\n')
    for coll in collections:
      decls.write('ppt {0}.{1}:::POINT\n'.format(db.name, coll['name']))
      decls.write('  ppt-type point\n')
      for field in coll['fields']:
        if field['type'] == 'list':
          field['name'] = 'num_' + field['name']
        write_var(decls, field['name'], 'variable', to_rep_type(field['type']), field['type'], '1') # DOTO levels
        if 'content' in field:
          for subf in field['content']:
            write_var(decls, subf['name'], 'variable', to_rep_type(subf['type']), subf['type'], '2')
      decls.write('\n')

def write_dtrace3(db, collections, path):
  with open(path + '.dtrace', 'w', encoding="utf-8") as dtrace:
    def write_t(writer, k, v, mod):
      writer.write('{0}\n'.format(parse_str(k, escaped=True)))
      writer.write('{0}\n'.format(parse_str(v, escaped=True, single_line=True, quoted=True)))
      writer.write('{0}\n'.format(mod))
    dtrace.write('decl-version 2.0\n')
    for coll in collections:
      coll_ppt = '\n{0}.{1}:::POINT\n'.format(parse_str(db.name, escaped=True), parse_str(coll['name'], escaped=True))
      cursor = db[coll['name']].find()
      for document in cursor:
        dtrace.write(coll_ppt)
        for field in coll['fields']:
          if field['type'] == 'list':
            var_name = field['name']
            if field['name'].split('_')[1] in document.keys():
              write_t(dtrace, var_name, len(document[field['name'].split('_')[1]]), '1')
              if 'content' in field:
                for i in range(len(field['content'])):
                  subf = field['content'][i]
                  if i < len(document[field['name'].split('_')[1]]):
                    write_t(dtrace, subf['name'], document[field['name'].split('_')[1]][i], '1')
                  else:
                    write_t(dtrace, subf['name'], 'NoField', '1')
          elif field['type'] == 'dict': # DOTO dict
            pass
          else:
            if field['name'] in document:
              write_t(dtrace, field['name'], document[field['name']], '1')
            else:
              write_t(dtrace, field['name'], 'NoField', '1')   

if __name__ == '__main__':
  path, host, port, database, level_orig, level_new = sys.argv[1:]
  level_orig = int(level_orig)
  port = int(port)
  client = pymongo.MongoClient(host, port, maxPoolSize=50)  # client = pymongo.MongoClient("localhost", 27017, maxPoolSize=50)
  db = client[database]
  # v1
  # collections = get_collections(db)
  # write_decls(collections, path)
  # write_dtrace(db, collections, path)

  # v2
  # write1(db, path, level_orig, level_new)

  # v3
  collections = get_collections3(db, level_orig)
  write_decls3(collections, path)
  write_dtrace3(db, collections, path)
  
