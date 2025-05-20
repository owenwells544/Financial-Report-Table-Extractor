# Financial-Report-Table-Extractor
Extracts tables from financial report pdfs using pdfplumber in python with mysql integration for storing extracted tables seemlessly.

# USAGE
If running with a mysql docker instance:
docker run --name <instance name> -e MYSQL_ROOT_PASSWORD=<root password> -e MYSQL_DATABASE=<database name> -e MYSQL_USER=<user name> -e MYSQL_PASSWORD=<user password> -e MYSQL_ARGS="--default-authentication-plugin=mysql_native_password" -p 3306:3306 -d mysql:5.7

-p 3306:3306 specifies the port numbers used for the mysql docker instance, change as needed

Then or otherwise:
python extract_tables.py --filename example.pdf --intersection_tolerance <default 10> --mysql_host 127.0.0.1 --mysql_user <user name> --mysql_password <user password> --mysql_database <database name>

# OUTPUT
All tables are inserted into the specified mysql instance.

# PROBLEMS/SOLUTIONS
Char Set: certain characters were not being recognized by mysql when data was being input to tables. This was solved by adding parameters to
the mysql connection object that specified charset='utf8mb4' and collation='utf8mb4_unicode_ci'. 

NAN: mysql was throwing several different errors revolving around NAN values, which was corrected by converting all NAN values to None before
inputting the data to mysql.

Docker for MYSQL: since I don't have a mysql instance, I used a mysql docker container for testing this project.

MYSQL authentication: I was having problems because the authentication method I was using for the mysql connection was not the one the docker container expected.
I found a slightly older version of the mysql docker container that accepted the method of authentication I was using, as attempts to update the authentication method
only produced additional issues.
