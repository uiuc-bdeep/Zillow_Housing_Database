# Zillow_Housing_Database Creation

The creation of database involves 3 steps. First, we need to install the database on the VM. Then, we need to create the database and tables. Finally, we need to insert the data into the database.

## Prerequisite
- A working Linux VM connected to the internet with root access
- Zillow Data
    - ZAsmt: Main.txt, Building.txt, BuildingAreas.txt
    - ZTrans: Main.txt, PropertyInfo.txt

## Setup PosgreSQL Engine
Steps:
1. Install the latest version of Postgres server and client (9.5 @ BDEEP):

    ```
    sudo apt-get update
    sudo apt-get install postgresql postgresql-client postgresql-contrib
    ```

2. Create superuser in PostgreSQL

    We need to set new user `postgres` and a password for it, in order to limit administrator privileges. For example, if one wants to manage, create, delete, or insert tables, he/she needs this password.

    In the Linux command line, execute:
    ```
    sudo su postgres -c psql template1
    ```

    We are switched to the PostgreSQL command line. One can check for all available psql commands by `\?`. Here, we want to change the password of the current user `postgres`. Type in:
    ```
    \password
    ```
    and enter the desired password. **Please record your password in case you forget.**

    Finally, leave the PostgreSQL environment by typing `\q`.

3. Create corresponding user in UNIX

    Previously, we create a new superuser and password in the database. To protect configuration files in the Linux system, we also need to set up a password for the corresponding UNIX user `postgres`. Execute the following in Linux command line:
    ```
    sudo passwd -d postgres
    sudo su postgres -c passwd
    ```

4. Set up Environment

    Finally, we will set up our environment such that we can execute native Postgres commands. Type in:
    ```
    sudo vim /etc/bash.bashrc
    ```
    At the end of the file, add the following line (**change <PostgreSQL_version> to your database version**):
    ```
    export PATH=$PATH:/usr/lib/postgresql/<PostgreSQL_version>/bin
    ```
    When done, re-execute the .bashrc file:
    ```
    source /etc/bash.bashrc
    ```

5. Restart the Postgres server

    When all steps above are finished, restart the database server by executing:
    ```
    sudo service postgresql restart
    ```

## Create Database and Table
We perform all operations in the Linux user `postgres`. First, we substitute the current Linux user by
```
sudo su postgres
```

To create an empty database, execute:
```
createdb -O postgres zillow_2017_nov
```
Or use other name for your newly created database. Note that the BDEEPZillow package uses the name above.

To ease the pain of specifying the data type for each Zillow data column, we can create from the provided `zillow.dump`.
```
pg_restore -C -d zillow_2017_nov zillow.dump
```

Finally, we exit the current Linux user by executing `exit`. And we are ready for data insertion!


## Insert Data
[zillow_txt_to_database.py](./zillow_txt_to_database.py) converts Zillow_Housing raw data from txt file to postgresql database hedonics.

The assumed raw file location is `/home/schadri/share/projects/Zillow_Housing/stores/Zillow_2017_Nov/`. One might want to change this line (& the completionfile log file location) on his/her own VM.

For example, if one wants to convert for state AL (state code 01), use command in the current VM:
```
python3 zillow_txt_to_database.py 01
```

Note: this script inserts data in chunk size 1. A faster method is possible for some txt files, given that they can be converted to pandas table. One can use python package [sqlalchemy](https://docs.sqlalchemy.org/en/13/) to insert into the database in larger chunks and achieve better performance. See InfoUSA Database for more details.
