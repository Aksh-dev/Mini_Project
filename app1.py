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
############
@app.route('/', methods=['GET'])
def FirstPage():
    rows = session.execute("""Select * From test1.stats""")
    result = []
    
    for r in rows:
        result.append({"Season":r.season,"HomeTeamScore":r.hometeamscore,"VisitorTeamScore":r.visitorteamscore,
                       "PlayerName":r.playername,"Points":r.points})
       
    return jsonify(result)
############
@app.route('/cass-db', methods=['GET','PUT'])
def cassdb():
    resp = requests.get(statistics)
    if resp.ok:
        response = resp.json()
     #Variables are extracted from the json file 

        for i in range(0,len(response['data'])):

            StatsInfo = response['data'][i]
            Game = StatsInfo['game']
            Player = StatsInfo['player']
            Season = Game['season']
            HomeTeamScore = Game['home_team_score']
            VisitorTeamScore = Game['visitor_team_score']
            PlayerName = Player['first_name']
            Points = StatsInfo['pts']
            #Storing it to the database
            cas_db = "UPDATE test1.stats SET HomeTeamScore= {}, VisitorTeamScore= {},PlayerName= {},Points= {} Where Season = '{}'".format(int(HomeTeamScore),int(VisitorTeamScore),PlayerName,int(Points), Season)
            session.execute(cas_db)

    else:
        print(resp.reason)
    return('<h1> Data is stored !<h1>. <p> <a href = "/records"> Click here</a> to view the records<p>')

#######################
@app.route('/records', methods=['GET'])
def summary():
    rows = session.execute("""Select * From test1.stats""")
    result = []
    
    for r in rows:
        result.append({"Season":r.season,"HomeTeamScore":r.hometeamscore,"VisitorTeamScore":r.visitorteamscore,
                       "PlayerName":r.playername,"Points":r.points})
       
    return jsonify(result)
######################
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
     #Variables are extracted from the json file 
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

@app.route('/stats',  methods=['POST']) #REST api POST method
def create():
    session.execute( """INSERT INTO test1.stats(season,hometeamscore,visitorteamscore,playername,points) VALUES('{}',{}, {}, {},{})""".format(request.json['season'],int(request.json['hometeamscore']),int(request.json['visitorteamscore']),request.json['playername'],int(request.json['points'])))
    return jsonify({'message': 'created: /stats/{}'.format(request.json['season'])}), 201

@app.route('/stats',  methods=['PUT']) #REST api PUT method
def update():
    session.execute("""UPDATE test1.stats SET hometeamscore= {},visitorteamscore= {}, playername= {}, points={} WHERE hometeamscore= '{}'""".format(int(request.json['hometeamscore']),int(request.json['visitorteamscore']),request.json['playername'],int(request.json['points']),int(request.json['season'])))
    return jsonify({'message': 'updated: /stats/{}'.format(request.json['season'])}), 200

@app.route('/stats',  methods=['DELETE']) #REST api DELETE method
def delete():
    session.execute("""DELETE FROM test1.stats WHERE season= '{}'""".format(request.json['season']))
    return jsonify({'message': 'deleted: /stats/{}'.format(request.json['hometeamscore'])}), 200

if __name__ == "__main__":
  
    app.run(host='0.0.0.0', port=80)



