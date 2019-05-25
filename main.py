import pymysql.cursors
#import warnings
import ast

#warnings.filterwarnings("ignore")

db_student_id = 'DB2014_15395'
def connect_server():
    return pymysql.connect(host='s.snu.ac.kr', port=3306, user=db_student_id, password=db_student_id, database=db_student_id)

# Table Names
T_Buildings = 'Buildings'
T_Performances = 'Performances'
T_Audiences = 'Audiences'
T_Assign = 'Assign'
T_Book = 'Book'

def print_menu():
    print("============================================================")
    print("1. print all buildings")
    print("2. print all performances")
    print("3. print all audiences")
    print("4. insert a new building")
    print("5. remove a building")
    print("6. insert a new performance")
    print("7. remove a performance")
    print("8. insert a new audience")
    print("9. remove an audience")
    print("10. assign a performance to a building")
    print("11. book a performance")
    print("12. print all performances which assigned at a building")
    print("13. print all audiences who booked for a performance")
    print("14. print ticket booking status of a performance")
    print("15. exit")
    print("16. reset database")
    print("============================================================")

# Setup Database Schema
def drop_table(tableName):
    with connection.cursor() as cursor:
        sql = "DROP TABLE IF EXISTS {var}".format(var=tableName)
        result = cursor.execute(sql)
    connection.commit()
def create_buildings():
    with connection.cursor() as cursor:
        sql= """CREATE TABLE IF NOT EXISTS {buildings} (
                `id` INT NOT NULL AUTO_INCREMENT,
                `name` VARCHAR(200) NOT NULL,
                `location` VARCHAR(200) NOT NULL,
                `capacity` INT NOT NULL,
                PRIMARY KEY (`id`))""".format(buildings=T_Buildings)
        cursor.execute(sql)
    connection.commit()
def create_performances():
    with connection.cursor() as cursor:
        sql= """CREATE TABLE IF NOT EXISTS {performances} (
                `id` INT NOT NULL AUTO_INCREMENT,
                `name` VARCHAR(200) NOT NULL,
                `type` VARCHAR(200) NOT NULL,
                `price` INT UNSIGNED NOT NULL,
                PRIMARY KEY (`id`))""".format(performances=T_Performances)
        cursor.execute(sql)
    connection.commit()
def create_audiences():
    with connection.cursor() as cursor:
        sql= """CREATE TABLE IF NOT EXISTS {audiences} (
                `id` INT NOT NULL AUTO_INCREMENT,
                `name` VARCHAR(200) NOT NULL,
                `gender` ENUM('M', 'F') NOT NULL,
                `age` INT UNSIGNED NOT NULL,
                PRIMARY KEY (`id`))""".format(audiences=T_Audiences)
        cursor.execute(sql)
    connection.commit()
def create_assign():
    with connection.cursor() as cursor:
        sql= """CREATE TABLE IF NOT EXISTS {assign} (
                `p_id` INT NOT NULL,
                `b_id` INT NOT NULL,
                PRIMARY KEY (`p_id`),
                CONSTRAINT `assign_performance`
                    FOREIGN KEY (`p_id`)
                    REFERENCES {performances} (`id`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                CONSTRAINT `assign_building`
                    FOREIGN KEY (`b_id`)
                    REFERENCES {buildings} (`id`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE)""".format(assign=T_Assign, performances=T_Performances, buildings=T_Buildings)
        cursor.execute(sql)
    connection.commit()
def create_book():
    with connection.cursor() as cursor:
        sql= """CREATE TABLE IF NOT EXISTS {book} (
                `p_id` INT NOT NULL,
                `seat` INT NOT NULL,
                `a_id` INT NOT NULL,
                PRIMARY KEY (`p_id`, `seat`),
                CONSTRAINT `book_performance`
                    FOREIGN KEY (`p_id`)
                    REFERENCES {assign} (`p_id`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                CONSTRAINT `book_audience`
                    FOREIGN KEY (`a_id`)
                    REFERENCES {audiences} (`id`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE)""".format(book=T_Book, assign=T_Assign, audiences=T_Audiences)
        cursor.execute(sql)
    connection.commit()

def data_to_str(data):
    if data is None:
        return ""
    else:
        return str(data)

def print_line(data, lengthList):
    data = list(map(data_to_str, data))
    length = len(data)
    formatStr = ""
    for i in range(0, length):
        formatStr = formatStr + "{" + "{num}:<{len}s".format(num=i, len=lengthList[i]) + "}"
        if i != length-1:
            formatStr = formatStr + "    "
    print(formatStr.format(*data))
    
def print_align(titleList, records):
    colNum = len(titleList)
    maxLen = [0] * colNum
    
    for i in range(0, colNum):
        maxLen[i] = len(titleList[i])

    for rec in records:
        for i in range(0, colNum):
            maxLen[i] = max(maxLen[i], len(str(rec[i])))
    
    print("-----------------------------------------------------------------------------------------")
    print_line(titleList, maxLen)
    print("-----------------------------------------------------------------------------------------")
    
    for i in records:
        print_line(i, maxLen)

def id_exists(tableName, id):
    with connection.cursor() as cursor:

        sql= """SELECT id
                FROM {table}
                WHERE id=%s
                """.format(table=tableName)

        cursor.execute(sql, id)
        result = cursor.fetchall()

        if len(result) != 1:
            return False
        return True

# Functions for book
def already_booked(p_id, seat):
    with connection.cursor() as cursor:

        sql= """SELECT p_id
                FROM {book}
                WHERE p_id=%s AND seat=%s
                """.format(book=T_Book)

        cursor.execute(sql, (p_id, seat))
        result = cursor.fetchall()

        if len(result) != 1:
            return False
        return True
def get_performance_info(p_id): # Return capacity on success / 0: not assigned
    with connection.cursor() as cursor:

        sql= """SELECT capacity, price
                FROM {performances} as P, {assign}, {buildings} as B
                WHERE P.id=%s and P.id=p_id and b_id=B.id
                """.format(performances=T_Performances, assign=T_Assign, buildings=T_Buildings)
        
        cursor.execute(sql, p_id)
        result = cursor.fetchone()
        
        if result is None:
            result = (0, 0)

        return result
def get_audience_age(a_id):
    with connection.cursor() as cursor:

        sql= """SELECT age
                FROM {audiences}
                WHERE id=%s
                """.format(audiences=T_Audiences)
        
        cursor.execute(sql, a_id)
        result = cursor.fetchone()
        return result[0]
def price_calculator(age, price, number):
    if age <= 7:
        multiply = 0
    elif age <= 12:
        multiply = 0.5
    elif age <= 18:
        multiply = 0.8
    else:
        multiply = 1
    return int(price * number * multiply)

# 1
def print_buildings():
    with connection.cursor() as cursor:

        sql= """SELECT id, name, location, capacity, COUNT(p_id)
                FROM {buildings} LEFT OUTER JOIN {assign} ON id=b_id
                GROUP BY id
                """.format(buildings=T_Buildings, assign=T_Assign)

        cursor.execute(sql)
        result = cursor.fetchall()
    titleList = ('id', 'name', 'location', 'capacity', 'assigned')
    print_align(titleList, result)
# 2
def print_performances():
    with connection.cursor() as cursor:

        sql= """SELECT id, name, type, price, COUNT(DISTINCT a_id)
                FROM {performances} LEFT OUTER JOIN {book} ON id=p_id
                GROUP BY id
                """.format(performances=T_Performances, book=T_Book)

        cursor.execute(sql)
        result = cursor.fetchall()
    titleList = ('id', 'name', 'type', 'price', 'booked')
    print_align(titleList, result)
# 3
def print_audiences():
    with connection.cursor() as cursor:

        sql= """SELECT *
                FROM {audiences}
                """.format(audiences=T_Audiences)

        cursor.execute(sql)
        result = cursor.fetchall()
    titleList = ('id', 'name', 'gender', 'age')
    print_align(titleList, result)
# 4
def insert_building():
    with connection.cursor() as cursor:

        bldg_n = input("Building name: ")
        bldg_l = input("Building location: ")
        bldg_c = input("Building capacity: ")
        if int(bldg_c) <= 0:
            print("Capacity should be positive integer")
            return

        bldg_n = bldg_n[:200]
        bldg_l = bldg_l[:200]

        sql= """INSERT INTO {buildings}
                (`name`, `location`, `capacity`)
                VALUES (%s, %s, %s)
                """.format(buildings=T_Buildings)

        cursor.execute(sql, (bldg_n, bldg_l, bldg_c))
        print("A building is successfully inserted")
    connection.commit()
# 5
def remove_building():
    with connection.cursor() as cursor:

        bldg_id = input("Enter building id to remove: ")
        if not id_exists(T_Buildings, bldg_id):
            print("There is no building with id " + bldg_id)
            return

        sql= """DELETE FROM {buildings}
                WHERE id=%s""".format(buildings=T_Buildings)

        cursor.execute(sql, bldg_id)
        print("The building " + bldg_id + " is successfully deleted")
    connection.commit()
# 6
def insert_performance():
    with connection.cursor() as cursor:

        pf_n = input("Performance name: ")
        pf_t = input("Performance type: ")
        pf_p = input("Performance price: ")
        if int(pf_p) < 0:
            print("Price should be non-negative integer")
            return
        
        pf_n = pf_n[:200]
        pf_t = pf_t[:200]

        sql= """INSERT INTO {performances}
                (`name`, `type`, `price`)
                VALUES (%s, %s, %s)
                """.format(performances=T_Performances)

        cursor.execute(sql, (pf_n, pf_t, pf_p))
        print("A performance is successfully inserted")
    connection.commit()
# 7
def remove_performance():
    with connection.cursor() as cursor:

        pf_id = input("Enter performance id to remove: ")
        if not id_exists(T_Performances, pf_id):
            print("There is no performance with id " + pf_id)
            return

        sql= """DELETE FROM {performances}
                WHERE id=%s""".format(performances=T_Performances)

        cursor.execute(sql, pf_id)
        print("The performance " + pf_id + " is successfully deleted")
    connection.commit()
# 8
def insert_audience():
    with connection.cursor() as cursor:

        aud_n = input("Audience name: ")

        aud_n = aud_n[:200]

        aud_g = input("Audience gender: ")
        if (aud_g != 'M' and aud_g != 'F'):
            print("Gender should be 'M' or 'F'")
            return

        aud_a = input("Audience age: ")
        if int(aud_a) <= 0:
            print("Price should be positive integer")
            return

        sql= """INSERT INTO {audiences}
                (`name`, `gender`, `age`)
                VALUES (%s, %s, %s)
                """.format(audiences=T_Audiences)

        cursor.execute(sql, (aud_n, aud_g, aud_a))
        print("An audience is successfully inserted")
    connection.commit()
# 9
def remove_audience():
    with connection.cursor() as cursor:

        aud_id = input("Enter audience id to remove: ")
        if not id_exists(T_Audiences, aud_id):
            print("There is no audience with id " + aud_id)
            return

        sql= """DELETE FROM {audiences}
                WHERE id=%s""".format(audiences=T_Audiences)

        cursor.execute(sql, aud_id)
        print("The audience " + aud_id + " is successfully deleted")
    connection.commit()
# 10
def assign_performance():
    with connection.cursor() as cursor:
        bldg_id = input("Building ID: ")
        if not id_exists(T_Buildings, bldg_id):
            print("There is no building with id " + bldg_id)
            return

        pf_id = input("Performance ID: ")
        if not id_exists(T_Performances, pf_id):
            print("There is no performance with id " + pf_id)
            return

        # Check if the performance is already assigned to other building
        sql= """SELECT p_id, b_id
                FROM {assign}
                WHERE p_id=%s
                """.format(assign=T_Assign)
        cursor.execute(sql, pf_id)
        check = cursor.fetchone()
        if check is not None:
            print("Error: Performance " + pf_id + " is already assigned to Building " + str(check[1]))
            return

        sql= """INSERT INTO {assign}
                (`p_id`, `b_id`)
                VALUES (%s, %s)
                """.format(assign=T_Assign)

        cursor.execute(sql, (pf_id, bldg_id))
        print("Successfully assign a performance")
    connection.commit()
# 11
def book_performance():
    with connection.cursor() as cursor:
        pf_id = input("Performance ID: ")
        if not id_exists(T_Performances, pf_id):
            print("There is no performance with id " + pf_id)
            return

        pf_info = get_performance_info(pf_id)
        capacity = pf_info[0]
        price = pf_info[1]
        if capacity == 0:
            print("Performance " + pf_id + " is not assigned to building now")
            return

        aud_id = input("Audience ID: ")
        if not id_exists(T_Audiences, aud_id):
            print("There is no audience with id " + aud_id)
            return

        seat_raw = input("Seat number: ")

        seat_list = ast.literal_eval(seat_raw)

        if isinstance(seat_list, int):
            seat_list = [seat_list]

        # Check seats
        for seat in seat_list:
            if seat <= 0:
                print("Invalid seat(" + seat + "): Seat number should be positive integer")
                return
            if capacity < seat:
                print("Invalid seat(" + seat + "): Assigned building has only " + capacity + " seats")
                return
            if already_booked(pf_id, seat):
                print("The seat is already taken")
                return

        for seat in seat_list:
            sql= """INSERT INTO {book}
                    (`p_id`, `seat`, `a_id`)
                    VALUES
                    (%s, %s, %s)
                    """.format(book=T_Book)
            cursor.execute(sql, (pf_id, seat, aud_id))

        print("Successfully book a performance")

        age = get_audience_age(aud_id)
        total = price_calculator(age, price, len(seat_list))
        print("Total ticket price is {:,}".format(total))
    connection.commit()
# 12
def print_assigned_performances():
    with connection.cursor() as cursor:

        bldg_id = input("Building ID: ")
        if not id_exists(T_Buildings, bldg_id):
            print("There is no building with id " + bldg_id)
            return


        sql= """SELECT id, name, type, price, COUNT(seat)
                FROM ({performances} as P LEFT OUTER JOIN {book} ON id=p_id) JOIN {assign} as A ON P.id=A.p_id
                WHERE b_id=%s
                GROUP BY id
                """.format(performances=T_Performances, book=T_Book, assign=T_Assign)

        cursor.execute(sql, bldg_id)
        result = cursor.fetchall()
    titleList = ('id', 'name', 'type', 'price', 'booked')
    print_align(titleList, result)
# 13
def print_booked_audiences():
    with connection.cursor() as cursor:

        pf_id = input("Performance ID: ")
        if not id_exists(T_Performances, pf_id):
            print("There is no performance with id " + pf_id)
            return

        sql= """SELECT DISTINCT id, name, gender, age
                FROM {audiences} JOIN {book} ON a_id=id
                WHERE p_id=%s
                """.format(audiences=T_Audiences, book=T_Book)

        cursor.execute(sql, pf_id)
        result = cursor.fetchall()
    titleList = ('id', 'name', 'gender', 'age')
    print_align(titleList, result)
# 14
def print_booking_status():
    with connection.cursor() as cursor:
        
        pf_id = input("Performance ID: ")
        if not id_exists(T_Performances, pf_id):
            print("There is no performance with id " + pf_id)
            return

        pf_info = get_performance_info(pf_id)
        capacity = pf_info[0]
        if capacity == 0:
            print("Performance " + pf_id + " is not assigned to building now")
            return

        sql= """SELECT seat, a_id
                FROM {book}
                WHERE p_id=%s
                """.format(book=T_Book)

        cursor.execute(sql, pf_id)
        status = cursor.fetchall()
        result = [None] * (capacity + 1)

        for bk in status:
            result[bk[0]] = bk[1]

        del result[0]

    result = list(zip(range(1, capacity+1), result))
    titleList = ('seat_number', 'audience_id')
    print_align(titleList, result)
# 16
def reset_database():
    while True:
        userinput = input('Reset Database: All data will be deleted. Would you proceed? (y/n) ')
        if userinput.lower() == 'n':
            print("Reset Cancelled.")
            return
        if userinput.lower() == 'y':
            break
    drop_table(T_Book)
    drop_table(T_Assign)
    drop_table(T_Audiences)
    drop_table(T_Performances)
    drop_table(T_Buildings)
    create_buildings()
    create_performances()
    create_audiences()
    create_assign()
    create_book()
    print("Database has been reset successfully.")

# Function Dictionary
execute = {
    1: print_buildings,
    2: print_performances,
    3: print_audiences,
    4: insert_building,
    5: remove_building,
    6: insert_performance,
    7: remove_performance,
    8: insert_audience,
    9: remove_audience,
    10: assign_performance,
    11: book_performance,
    12: print_assigned_performances,
    13: print_booked_audiences,
    14: print_booking_status,
    16: reset_database
}

# Main Routine
connection = connect_server()
print_menu()
while True:
    if connection is None:
        connection = connect_server()
    else:
        raw = input('Select your action: ')
        if raw.strip() == "":
            continue

        option = int(raw)

        if option == 15:
            break

        elif option < 1 or 16 < option:
            print("Invalid Operation")
            print()
            continue

        else:
            execute[option]()
            print()

connection.close()
