import h5py
import PIL
import numpy
import MySQLdb
import os
import time
"""
Author = Daniel Redder
UGA Computer Science PhD student

The intent of this test is to determine the 
1) storage
2) time
requirements to store / update / backup images using a variety of storage methods

Replace test_type with which test to use
possible values: 
blob  : a mySQL blob test
path  : a traditional mySQL file path storage
h5py  : storing a reference to the part of a hdf5 file to decompress & fetch
zipped path: no SQL database just accessing zipped files ordered by category
"""
test_type = "blob"
batch_size = 10


"""
The classes of images we are saving are these sizes
cars: 949MB
codd: 400kb
elon 1.05 MB
"""


"""
place your selected testing dataset into a directory with 
the dir name set here with subdirectories holding categorized images
"""
dataset_usage = "imageNet"


run_insert = False

"""
Please place the following information for your mySQL database implementation
"""

from dotenv import load_dotenv

load_dotenv()


hostname = os.getenv("SQL_HOSTNAME")
username = os.getenv("SQL_USERNAME")
password = os.getenv("SQL_PASSWORD")

#Also please create a database called the following / change this string to be a different database name
database = "data"

conn = MySQLdb.connect(host = hostname, user = username, passwd = password, db = database)
curse = conn.cursor()

assert os.path.exists(dataset_usage), "directory not found"


#We choose medium blob here because it is fitting for our test data
if test_type == "blob":

    if run_insert:

        emp_table_create = """
        CREATE TABLE IF NOT EXISTS employee(
        emp_id int  PRIMARY KEY,
        emp_name VARCHAR(20),
        emp_loc_id int);
        """

        im_table_create = """
         CREATE TABLE IF NOT EXISTS image_test(
            im_id BIGINT PRIMARY KEY auto_increment,
            emp_id int NOT NULL,
            image_class VARCHAR(15) NOT NULL,
            image MEDIUMBLOB NOT NULL,
            FOREIGN KEY (emp_id) REFERENCES employee(emp_id));
        """
        curse.execute(emp_table_create)
        curse.execute(im_table_create)
        conn.commit()

        curse.execute("INSERT INTO employee(emp_id, emp_name,emp_loc_id) VALUES(0,'billy',0);")

        clock_start_insert = time.time()

        for category in os.listdir(dataset_usage):
            img_list = os.listdir(f"{dataset_usage}/{category}")
            for img in img_list:

                with open(f"{dataset_usage}/{category}/{img}","rb") as f:
                    img_file = f.read()

                curse.execute("INSERT INTO image_test(image_class, image, emp_id) VALUES( %s, %s, 0)",(category,img_file))
            conn.commit()
        clock_stop_insert = time.time()

        print(f"Insertion into blob complete time taken {clock_stop_insert-clock_start_insert}")

    #-------------- insertion complete -------------------------------------------------------------------
    #begin space calculation

    curse.execute("SHOW TABLE STATUS")
    print("Total Data Length (gb) (medium blob): ")
    print(int(curse.fetchall()[1][6])/(pow(10,9)))

    #----------- Space calculation complete -----------------
    #begin query batch



    clock_start_query = time.time()
    curse.execute(f"SELECT image FROM image_test WHERE image_class='cars' LIMIT {batch_size}")
    x= curse.fetchall()
    clock_stop_query = time.time()

    print("Query time for blob batch:  ",clock_stop_query - clock_start_query)

    #------------- Time Query batch complete ---------------------------------------------------------
    #begin backup
    #
    # print("what")
    # clock_start_backup = time.time()
    # dump = f"mysqldump -h {hostname} -u {username} -p {password} > {os.getcwd()}/back.sql"
    # os.system(dump)
    # clock_stop_backup = time.time()
    #
    # print("Backup time for blob: ",clock_stop_backup-clock_start_backup)

elif test_type == "h5py":
    pass




#Out of time :(
elif test_type == "path":
    pass










else:
    print("please select a test_type from the available listed in test.py")










