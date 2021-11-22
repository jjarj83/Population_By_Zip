from uszipcode import SearchEngine
import mysql.connector
import json


config = {
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
    'database': 'zip_populations'
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

get_zips = ("SELECT zip "
            "FROM   zip_stats " )

cursor.execute(get_zips)
existing_zips = []
rows = cursor.fetchall()

search = SearchEngine(simple_zipcode=True)

for row in rows:
    zipcode_stats = json.loads(search.by_zipcode(row[0]).to_json())
    #print(zipcode_stats)
    if zipcode_stats['zipcode']:

        update_zip = ("UPDATE zip_stats  "
                      "SET    state = %s, median_home_value = %s, median_household_income = %s, "
                      "       lat = %s, lng = %s, bounds_north = %s, bounds_east = %s, "
                      "       bounds_south = %s, bounds_west = %s "
                      "WHERE  zip = %s ")

        zipcode_data = (zipcode_stats['state'], zipcode_stats['median_home_value'],
                        zipcode_stats['median_household_income'], zipcode_stats['lat'],
                        zipcode_stats['lng'], zipcode_stats['bounds_north'],
                        zipcode_stats['bounds_east'], zipcode_stats['bounds_south'],
                        zipcode_stats['bounds_west'], int(zipcode_stats['zipcode']))
        #print(zipcode_data)

        cursor.execute(update_zip, zipcode_data)


cnx.commit()
cursor.close()
cnx.close()
