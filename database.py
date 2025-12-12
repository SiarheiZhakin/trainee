import psycopg2

class Database:
    def __init__(self, config: dict):
        try:
            self.conn = psycopg2.connect(**config)
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            print("Connection is OK!")
        except psycopg2.OperationalError as e:
            print(f"Connection error: {e}")

    def insert_into_db_rooms(self, rooms)->None:
        """Insert into table rooms if not exists"""
        sql_query = "INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;"
        with self.conn.cursor() as cursor:
            cursor.executemany(sql_query, [(i['id'], i['name']) for i in rooms])
            self.conn.commit()

    def insert_into_db_students(self, students)->None:
        """Insert into table students if not exists"""
        sql_query = """INSERT INTO students(birthday, id, name, room, sex) VALUES(%s, %s, %s, %s, %s)
                     ON CONFLICT (id) DO NOTHING;"""
        with self.conn.cursor() as cursor:
            cursor.executemany(sql_query, [(i['birthday'], i['id'], i['name'], i['room'], i['sex']) for i in students])
            self.conn.commit()

    def number_and_students(self)->list:
        """List of rooms and the number of students in each of them"""
        sql_query = """SELECT r.name, COUNT(s.room) FROM rooms r
                     LEFT JOIN students s ON r.id = s.room
                     GROUP BY r.id
                     ORDER BY r.id"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_query)
            result_query = cursor.fetchall()
            return result_query

    def smallest_avg_age(self)->list:
        """5 rooms with the smallest average age of students"""
        sql_query = """SELECT r.name, ROUND(AVG(DATE_PART('year', age(s.birthday)))) AS average FROM rooms r
        JOIN students s ON r.id = s.room
        GROUP BY r.name
        ORDER BY average
        LIMIT 5;"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_query)
            result_query = cursor.fetchall()
            return result_query

    def largest_difference(self)->list:
        """5 rooms with the largest difference in the age of students"""
        sql_query = """SELECT r.name,    MAX(DATE_PART('year', AGE(s.birthday))) 
                                        -MIN(DATE_PART('year', AGE(s.birthday))) AS difference FROM rooms r
                       JOIN students s ON r.id = s.room
                       GROUP BY r.name
                       ORDER BY difference DESC
                       LIMIT 5"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_query)
            result_query = cursor.fetchall()
            return result_query

    def different_sex_room(self)->list:
        """List of rooms where different-sex students live"""
        # before optimize -------------- Ex time = 19.5ms
        # sql_query = """SELECT r.name FROM rooms r
        #                JOIN students s ON r.id = s.room
        #                GROUP BY r.name,r.id
        #                HAVING COUNT(s.sex) FILTER(WHERE sex='F')>0
        #                        AND
        #                       COUNT(s.sex) FILTER(WHERE sex='M')>0
        #                ORDER BY r.id"""
        # after optimize -------------- Ex time = 11.3ms
        sql_query = """SELECT r.name
                        FROM rooms r
                        WHERE EXISTS (SELECT 1 FROM students s WHERE s.room = r.id AND s.sex = 'F')
                        AND EXISTS (SELECT 1 FROM students s WHERE s.room = r.id AND s.sex = 'M')
                        ORDER BY r.id;"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql_query)
            result_query = cursor.fetchall()
            return result_query