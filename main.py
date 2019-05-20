import pymysql.cursors

connection = pymysql.connect(host='s.snu.ac.kr', port=3306, user='DB2014_15395', password='DB2014_15395', database='DB2014_15395')

try:
    with connection.cursor() as cursor:
        sql= "SHOW TABLES"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(result)
    connection.commit()
finally:
    connection.close()
