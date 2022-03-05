from flask import Flask, request, jsonify #render_template
import sqlite3

app = Flask(__name__)

# Convert tuples to dicts - ready for json
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

@app.route("/")
def index():
    # Write to SQLITE
    con = sqlite3.connect('metaweather.db')
    con.row_factory = dict_factory
    cur = con.cursor()

    # Create table
    cur.execute('''SELECT * FROM MetaWeather''')

    rows = cur.fetchall()
    return jsonify(rows)

@app.route("/avglast3")
def allweather():
    # Write to SQLITE
    con = sqlite3.connect('metaweather.db')
    con.row_factory = dict_factory
    cur = con.cursor()

    # Create table
    cur.execute('''SELECT city,date,avg_temp FROM MetaWeather WHERE date >= (SELECT date
                                                                            FROM MetaWeather
                                                                            GROUP BY date
                                                                            ORDER BY date DESC
                                                                            LIMIT 2 , 1) ''') # Subquery returns 3rd largest date

    rows = cur.fetchall()
    return jsonify(rows)

@app.route("/getn")
def getn():
    print(request.args['top'])

    con = sqlite3.connect('metaweather.db')
    con.row_factory = dict_factory
    cur = con.cursor()
    
    cur.execute(F''' SELECT city,avg_temp FROM (SELECT city, MAX(avg_temp) as avg_temp FROM MetaWeather GROUP BY city) ORDER BY avg_temp DESC LIMIT {request.args['top']}''')
    rows = cur.fetchall()
    cur.execute(F''' SELECT city,humidity FROM (SELECT city, MAX(humidity) as humidity FROM MetaWeather GROUP BY city) ORDER BY humidity DESC LIMIT {request.args['top']}''')
    rows.append(cur.fetchall())
    cur.execute(F''' SELECT city,max_temp FROM (SELECT city, MAX(max_temp) as max_temp FROM MetaWeather GROUP BY city) ORDER BY max_temp DESC LIMIT {request.args['top']}''')
    rows.append(cur.fetchall())
    cur.execute(F''' SELECT city,min_temp FROM (SELECT city, MAX(min_temp) as min_temp FROM MetaWeather GROUP BY city) ORDER BY min_temp DESC LIMIT {request.args['top']}''')
    rows.append(cur.fetchall())
    cur.execute(F''' SELECT city,wind_speed FROM (SELECT city, MAX(wind_speed) as wind_speed FROM MetaWeather GROUP BY city) ORDER BY wind_speed DESC LIMIT {request.args['top']}''')
    rows.append(cur.fetchall())

    return jsonify(rows)

if __name__ == "__main__" :
	app.run(debug=True)
