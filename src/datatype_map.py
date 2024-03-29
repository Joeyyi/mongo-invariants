import datetime
import bson

'''
m_type
str, int, list, dict, datetime..?

rep_type <daikon-type>
The representation type should be one of boolean, int, hashcode, double, or
java.lang.String; or an array of one of those (indicated by a [..] suffix).
'''


def to_rep_type(m_type):
  rep_type = m_type
  if m_type == 'int' or m_type == 'double' or m_type == 'float':
    rep_type = 'double'
  elif m_type == 'bool':
    rep_type = 'boolean'
  else:
    rep_type = 'java.lang.String'
  return rep_type


def to_dtrace_val(val):
  if type(val) is str:
    val = '\"' + val + '\"'
    val = val.replace('\n', '')
  if type(val) is datetime.datetime:
    val = '\"' + str(val) + '\"'
  if type(val) is bson.objectid.ObjectId:
    val = '\"' + str(val) + '\"'
  if type(val) is bool:
    val = str(val).lower()
  return val

def to_default(field_type):
  if field_type == 'str':
    return '\"NoField\"'
  if field_type == 'int':
    return 0 
  if field_type == 'float' or field_type == 'int' or field_type == 'double':
    return 0.00
  if field_type == 'bool':
    return
  else:
    return '\"NoField\"'