import datetime

'''
m_type
str, int, list, dict, datetime..?

rep_type <daikon-type>
The representation type should be one of boolean, int, hashcode, double, or
java.lang.String; or an array of one of those (indicated by a [..] suffix).
'''


def to_rep_type(m_type):
  rep_type = m_type
  if m_type == 'str':
    rep_type = 'java.lang.String'
  if m_type == 'list':
    rep_type = 'java.lang.String'
  if m_type == 'dict':
    rep_type = 'java.lang.String'
  if m_type == 'datetime':
    rep_type = 'java.lang.String'
  if m_type == 'int':
    rep_type = 'int'
  
  return rep_type


def to_dtrace_val(val):
  if type(val) is str:
    val = '\"' + val + '\"'
    val = val.replace('\n', '')
  if type(val) is datetime.datetime:
    val = '\"' + str(val) + '\"'

  return val

def to_default(field_type):
  if field_type == 'str':
    return '\"NoField\"'
  if field_type == 'int':
    return 0 
  if field_type == 'float':
    return 0.0
  else:
    return '\"NoField\"'