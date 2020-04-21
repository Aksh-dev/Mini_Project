from flask import Flask, request, jsonify
import json
import requests
import requests_cache
from cassandra.cluster import Cluster

cluster = Cluster(contact_points=['172.17.0.2'],port=9042)
session=cluster.connect()
requests_cache.install_cache('test1_api_cache', backend='sqlite', expire_after=36000)

app = Flask(__name__)

nbaPlayerData = 'https://www.balldontlie.io/api/v1/players'
statistics= 'https://www.balldontlie.io/api/v1/stats'

@app.route('/', methods=['GET'])
def Hello():
    return ('<h1> NBA Games Statistics<h1><p> <a href = "/records"> "Click here"</a> to view the current statistics on the Cloud Database<p> <p> <a href = "/external"> "Click here"</a> to view the details of Basketball Players<p> ')

#Displays the contents of the Cloud database
@app.route('/records', methods=['GET'])
def records():
    rows = session.execute("""Select * From test1.stats""")
    result = []
    for r in rows:
        result.append({"Season":r.season,"HomeTeamScore":r.hometeamscore,"VisitorTeamScore":r.visitorteamscore,
                       "PlayerName":r.playername,"Points":r.points})
       
    return jsonify(result)
   
# The external api link is stored in the nbaPlayerData variable
# and its content which is initially in json format is extracted and displayed for each of basketball players
@app.route('/external')
def external():
    resp = requests.get(nbaPlayerData)
    if resp.ok:
        response = resp.json()
    else:
        print(resp.reason)

    header = """<table style="width:30%">
                <tr>
                    <th align=\"left\">First Name</th>
                    <th align=\"left\">Last Name</th>
                    <th align=\"right\"> City</th>
                    <th align=\"right\"> Team </th>
                    <th align=\"right\"> Height</th>
                    <th align=\"right\"> Weight</th>
                    <th align=\"right\"> Position </th>
                </tr>
     """
    for i in range(0,len(response['data'])):

        Players_data = response['data'][i]
        FirstName = Players_data['first_name']
        LastName = Players_data['last_name']
        Height = Players_data['height_feet']
        Team = Players_data['team']
        City = Team['city']
        TeamName = Team['name']
        Weight = Players_data['weight_pounds']
        Position = Players_data['position']
        header += "<tr>\n"
        header += "<td align=\"left\">" + str(FirstName) + "</td>\n"
        header += "<td align=\"left\">" + str(LastName) + "</td>\n"
        header += "<td align=\"right\">" + str(City) + "</td>\n"
        header += "<td align=\"right\">" + str(TeamName) + "</td>\n"
        header += "<td align=\"right\">" + str(Height) + "</td>\n"
        header += "<td align=\"right\">" + str(Weight) + "</td>\n"
        header += "<td align=\"right\">" + str(Position) + "</td>\n"

        
        header += "</tr>\n"

    header += "</table>"
    return header

#Post,Put and Delete implementation

@app.route('/stats', methods = ['POST'])
def create():
    session.execute("""INSERT INTO test1.stats(season,hometeamscore,visitorteamscore,playername,points) VALUES('{}',{},{},'{}',{})""".format(request.json['season'], int(request.json['hometeamscore']), int(request.json['visitorteamscore']), request.json['playername'], int(request.json['points'])))
    return jsonify({'message':' added: /stats/{}'.format(request.json['season'])}),201


@app.route('/stats',  methods=['PUT']) 
def update():
    session.execute("""UPDATE test1.stats SET hometeamscore= {},visitorteamscore= {}, playername= '{}', points={} WHERE season= '{}'""".format(int(request.json['hometeamscore']),int(request.json['visitorteamscore']),request.json['playername'],int(request.json['points']),(request.json['season'])))
    return jsonify({'message': 'updated: /stats/{}'.format(request.json['season'])}), 200

@app.route('/stats',  methods=['DELETE']) 
def delete():
    session.execute("""DELETE FROM test1.stats WHERE season= '{}'""".format(request.json['season']))
    return jsonify({'message': 'deleted: /stats/{}'.format(request.json['season'])}), 200

if __name__ == "__main__":
  
    app.run(host='0.0.0.0', port=443, ssl_context=('cert.pem','key.pem'))


