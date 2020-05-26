from dal.biostudies import db
from dal.common_mysql import execute_select, execute_insert

__author__ = 'Ahmed G. Ali'


def get_root_section_by_accession(acc):
    sql = """ SELECT * FROM SectionAttribute where 
    section_id in
    (select rootSection_id from Submission where accNo = '{acc}')""".format(acc=acc)
    return execute_select(sql, db=db)


def retrieve_gxa_studies():
    sql = """SELECT sub.accNo,  
l.id as link_id,l.url,l.section_id, 
la.id as link_att_id, la.name, la.value
 from Submission sub join Section sec on sub.rootSection_id = sec.id 
join Link l on l.section_id = sec.id join LinkAttribute la on la.link_id = l.id
where  sub.accNo like 'E-%' and la.name='Type'"""
    return execute_select(sql, db=db)


def insert_gxa_link(acc, section_id, link_type, dscr):
    sql = """INSERT INTO Link (local, tableIndex, url, section_id) 
    values(0, -1, '{acc}', {sec})""".format(acc=acc, sec=section_id)
    # print(sql)
    execute_insert(sql, db)

    sql = """SELECT max(id) as link_id from Link 
    where local = 0 and tableIndex=-1 and url='{acc}' and section_id={sec}""".format(acc=acc, sec=section_id)
    print(sql)
    res = execute_select(sql, db)
    print (res)
    link_id = res[0]['link_id']
    insrt_sql = """INSERT INTO LinkAttribute (name, value, link_id,numValue, reference ) values 
    ('Type', '{link_type}', {link_id}, 0, 0), ('Description', '{dscr}', {link_id}, 0,0)""".format(
        link_type=link_type, dscr=dscr, link_id=link_id)
    print(insrt_sql)
    execute_insert(insrt_sql, db)
    # execute_insert("""""", db)


def get_ae_submissions():

    sql = """SELECT s.* FROM SubmissionAttribute sa join Submission s on s.id = sa.submission_id 
    where name = 'AttachTo' and value = 'ArrayExpress';"""

    return execute_select(sql, db)

