import psy


def deduplicate_forbidden():
    sql = '''
        DELETE FROM forbidden_eating WHERE id IN (
            SELECT DISTINCT(a.id)
                FROM forbidden_eating a JOIN forbidden_eating b
                ON a.a_id = b.a_id AND
                   a.b_id = b.b_id AND
                   a.id != b.id
                WHERE a.id > b.id
        );
    '''
    with psy.query() as cursor:
        cursor.execute(sql)
