import sys
import pymongo
from copy import deepcopy

from datatype_map import to_rep_type, to_dtrace_val, to_default

# string preprocessing for dtrace
def parse_str(mystr, escaped=True, single_line=False, quoted=False):
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
        # res['content'].append(get_structure(sub_field_v, '{0}_val'.format(name_field), level + 1, target_level))
    else: 
        res['length'] = len(field.keys())
    return res
  else:
    return {'name': name_field, 'type': type(field).__name__, 'level': level}
  
def get_collections(db, level_orig):
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

def write_decls(collections, path):
  with open(path + '.decls', 'w') as decls:
    def write_var(writer, name, kind, rep, dec, comp):
        writer.write(' variable {0}\n'.format(name))
        writer.write('  var-kind {0}\n'.format(kind))
        writer.write('  rep-type {0}\n'.format(rep))
        writer.write('  dec-type {0}\n'.format(dec))
        writer.write('  comparability {0}\n'.format(int(comp)))
    def write_structure(writer, field, level):
      if field['type'] == 'list' or field['type'] == 'dict':
        # variable:num_arr type:int
        write_var(writer, 'num_' + field['name'], 'variable', to_rep_type('int'), 'int', field['level'])        
        if 'content' in field:
          # variable: each item in arr
          for subf in field['content']:
              write_structure(writer, subf, level + 1)
      elif field['name'] == '_id':
        write_var(writer, '_id', 'variable', to_rep_type('ObjectId'), 'ObjectId', field['level'])
      else:
        # basic variable types
        write_var(writer, field['name'].replace(' ', '_'), 'variable', to_rep_type(field['type']), field['type'], field['level'])
        
    decls.write('decl-version 2.0\ninput-language MongoDB\n\n')
    for coll in collections:
      decls.write('ppt {0}.{1}:::POINT\n'.format(db.name, coll['name']))
      decls.write('  ppt-type point\n')
      for field in coll['fields']:
        write_structure(decls, field, 1)
      decls.write('\n')


'''
write_dtrace
1. get default trace structure from default model structure
2. overwrite trace structure with current document
3. write dtrace structure to dtrace file

trace structure:
{
  'var': 'aaa',
  'val': 'bbb',
  'mod': 1,
  'content': [] # optional
}

'''
def write_dtrace(db, collections, path):
  
  with open(path + '.dtrace', 'w', encoding="utf-8") as dtrace:
    def write_trace(item):
      dtrace.write('{0}\n{1}\n{2}\n'.format(item['var'].replace(' ', '_'), item['val'], item['mod']))
      if 'content' in item:
        for sub in item['content']:
          write_trace(sub)

    def get_default_trace(field):
      res = {}
      if field['type'] == 'list' or field['type'] == 'dict':
        res = {'var': 'num_' + field['name'], 'val': to_default('int'), 'mod': '1'}
        if 'content' in field:
          content = []
          for sub in field['content']:
            content.append(get_default_trace(sub))
          res = {'var': 'num_' + field['name'], 'val': to_default('int'), 'mod': '1', 'content': content}
      else:
        res = {'var': field['name'], 'val': to_default(field['type']), 'mod': '1'}
      return res
    
    
    
    def get_trace(d_trace, key, val):
      new_trace = []

      if type(val).__name__ == 'list':
        for i in range(len(d_trace)):
          default = d_trace[i]
          if default['var'] == 'num_' + key:
            default['val'] = len(val)
            if len(val) > 0 and 'content' in default:
              for j in range(len(val)):
                default['content'] = get_trace(default['content'], key + '[' + str(j) + ']', val[j])        
          new_trace.append(default)
        
      elif type(val).__name__ == 'dict':
        for i in range(len(d_trace)):
          default = d_trace[i]
          if default['var'] == 'num_' + key:
            default['val'] = len(val.keys())
            if len(val.keys()) > 0 and 'content' in default:
              for k, v in val.items():
                default['content'] = get_trace(default['content'], key + '<' + str(k) + '>', v)            
          new_trace.append(default)
      
      else:
        for i in range(len(d_trace)):
          default = d_trace[i]
          if default['var'] == key:
            default['val'] = to_dtrace_val(val)
          new_trace.append(default)
      return new_trace
      
      
    for coll in collections:
      
      default_trace = []
      for field in coll['fields']:
        default_trace.append(get_default_trace(field))

      dtrace.write('decl-version 2.0\n')
      coll_ppt = '\n{0}.{1}:::POINT\n'.format(parse_str(db.name, escaped=True), parse_str(coll['name'], escaped=True))
      cursor = db[coll['name']].find()
      # print(default_trace)
      
      for document in cursor:
        dtrace.write(coll_ppt)
        default = deepcopy(default_trace)
        for k, v in document.items():
          trace = get_trace(default, k, v)
        for item in trace:
          write_trace(item)



if __name__ == '__main__':
  # path, host, port, database, level_orig, level_new = sys.argv[1:]
  # level_orig = int(level_orig)
  # port = int(port)
  # client = pymongo.MongoClient(host, port, maxPoolSize=50)  # client = pymongo.MongoClient("localhost", 27017, maxPoolSize=50)
  # db = client[database]


  path = '/Users/Renol/Desktop/UVA/2019Fall/SA/Project/mongo-invariants/src/test'
  level_orig = 2
  myclient = pymongo.MongoClient("mongodb://localhost:27017/")
  db = myclient["SA"]  # 数据库名
  collections = get_collections(db, level_orig)
  # print(collections)
  write_decls(collections, path)
  write_dtrace(db, collections, path)