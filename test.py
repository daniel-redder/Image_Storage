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
test_type = ""


"""
place your selected testing dataset into a directory with 
the dir name set here with subdirectories holding categorized images
"""
dataset_usage = "imageNet"


"""
Please place the following information for your mySQL database implementation
"""
hostname = "localhost"
username = "username"
password = "password"
database = "dbname"

conn = MySQLdb.connect(host = hostname, user = username, passwd = password, db = database)
curse = conn.cursor()

assert os.path.exists(dataset_usage), "directory not found"


if test_type == "blob":


    table_create = """
    CREATE TABLE image_test(
        im_id BIGINT PRIMARY KEY AUTOINCREMENT,
        class VARCHAR(15) NOT NULL,
        image BLOB NOT NULL
    """
    curse.execute(table_create)
    conn.commit()

    clock_start_insert = time.time()

    for category in os.listdir(dataset_usage):
        img_list = os.listdir(f"{dataset_usage}/{category}")
        for img in img_list:

            with open(f"{dataset_usage}/{category}/{img}","rb") as f:
                img_file = f.read()

            curse.execute("INSERT INTO image_test(class, image) VALUES( (?), (?))",(category,img_file))
        conn.commit()
    clock_stop_insert = time.time()

    print(f"Insertion into blob complete time taken {clock_stop_insert-clock_start_insert}")

    #-------------- insertion complete -------------------------------------------------------------------
    #begin space calculation

    curse.execute("SHOW TABLE STATUS")
    print(curse.fetchall())

    #---------------- space calc complete ---------------------------------------------------------------
    #begin query time calculation

    #single element query


    #class query

    #batch access query

    #create materialized view

    #query all values in view in batches of size ...
    batch_size = 10000

    #------------- query calc complete ----------------------------------------------------------------------
    #start update calc


    #------------ update calc complete ----------------------------------------------------------------------
    #start backup calc



    #----------- backup calc complete -----------------------------------------------------------------------

elif test_type == "path":
    pass


elif test_type == "h5py":
    pass


elif test_type == "zipped path":
    pass






else:
    print("please select a test_type from the available listed in test.py")










