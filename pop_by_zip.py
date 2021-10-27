from flask import Flask
from flask_restful import Resource, Api, reqparse
import mysql.connector
import requests
import json

app = Flask(__name__)
api = Api(app)

config = {
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
    'database': 'zip_populations'
}


class Zips(Resource):
    #main function that gets 2010 pop and 2020 pop of zips within given radius of given zip
    def get(self):
        cnx = mysql.connector.connect(**config)
        zips = {}

        parser = reqparse.RequestParser()
        parser.add_argument('zip', required=True)
        parser.add_argument('radius', required=True)
        args = parser.parse_args()

        radius = ''
        if (args['radius'] == '10'):
            radius = 'neighbors_10'
        elif (args['radius'] == '25'):
            radius = 'neighbors_25'
        elif (args['radius'] == '50'):
            radius = 'neighbors_50'
        else:
            return { 'message': 'Invalid Radius' }, 400

        check_neighbors = (f"SELECT zip, pop_2010, pop_2020, {radius} "
                            "FROM zip_stats "
                            "WHERE zip = %s" )

        cursor = cnx.cursor()
        cursor.execute(check_neighbors, (int(args['zip']),))
        result = cursor.fetchone()
        if result[1] != None and result[2] != None:
            change = ((result[2] - result[1]) / result[1]) * 100
            zips[result[0]] = {2010: result[1], 2020: result[2], 'Change': change}
        else:
            zips[result[0]] = {2010: result[1], 2020: result[2], 'Change': 'N/A'}

        zips['nostats'] = []

        if result == None:
            return { 'Zip Not Found' }, 404

        neighbors = None if result[3] == None else json.loads(result[3])

        #if neighbors not in database uses outside API to get neighbors and then add them to the database
        if neighbors == None:
            endpoint = f"https://www.zipcodeapi.com/rest/KWUrRxQTHwgWU9D6wbwh9vmAmTm1ZI7v5a24bPzWULU5X37AWaxWjeQO3Zds98ca/radius.json/{args['zip']}/{args['radius']}/miles?minimal"
            response = requests.get(endpoint)
            response_json = response.json()
            neighbors = response_json['zip_codes']
            neighbors_json = json.dumps(neighbors)

            if (response.status_code == 200):
                add_neighbor = ("UPDATE zip_stats "
                                f"SET {radius} = %s "
                                f"WHERE zip = %s ")

                cursor.execute(add_neighbor, (neighbors_json, int(args['zip'])))
                cnx.commit()
            else:
                return { 'Result': response.reason }, 429


        for neighbor in neighbors:
            get_pop = ("SELECT pop_2010, pop_2020 "
                        "FROM zip_stats "
                        "WHERE zip = %s")

            cursor.execute(get_pop, (neighbor,))
            result = cursor.fetchone()
            if result == None:
                zips['nostats'].append(neighbor)
            else:
                if result[0] != None and result[1] != None:
                    change = ((result[1] - result[0]) / result[0]) * 100
                    zips[neighbor] = {2010: result[0], 2020: result[1], 'Change': change}
                else:
                    zips[neighbor] = {2010: result[0], 2020: result[1], 'Change': 'N/A'}

        cursor.close()
        cnx.close()
        return { 'Result': zips }, 200


api.add_resource(Zips, '/zips')
app.run()
