import time
import mysql.connector
from mysql.connector import Error
from faker import Faker
from randomtimestamp import randomtimestamp

Faker.seed(33422)
fake = Faker()

create_table_sql = """
CREATE TABLE `users_details` (
  `user_id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `registered_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`user_id`),
  UNIQUE KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""

try:
    conn = mysql.connector.connect(host='localhost', database='bootcamp',
                                   user='bootcamp', password='bootcamp')

    if conn.is_connected():
        cursor = conn.cursor()

        try:
            cursor.execute(create_table_sql)
            print("Table created")
        except Exception as e:
            print("Error creating table", e)
        row = {}
        n = 0

        while n < 1000:
            n += 1
            registered_at = time.strftime(
                str(randomtimestamp(start_year=2018, end_year=2019, pattern="%Y-%m-%d %H:%M:%S")))
            row = [n, str(fake.first_name() + "_" + fake.last_name()).replace(" ", "_").lower(), registered_at]

            if n not in [100, 200, 300, 400, 500, 600, 700, 800, 900]:
                try:
                    cursor.execute('INSERT INTO `users_details` (user_id, username, registered_at) VALUES ("%s", "%s", '
                                   '"%s");' % (row[0], row[1], row[2]))
                    conn.commit()
                except Exception as e:
                    print(f"Error: {e}")

            if n % 100 == 0:
                print("iteration %s" % n)
                time.sleep(0.5)

except Error as e:
    print("error", e)
    pass
except Exception as e:
    print("Unknown error %s", e)
finally:
    # closing database connection.
    if conn and conn.is_connected():
        conn.commit()
        cursor.close()
        conn.close()
