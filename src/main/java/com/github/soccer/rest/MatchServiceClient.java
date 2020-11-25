package com.github.soccer.rest;

import com.github.soccer.dto.Match;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.ws.rs.core.UriBuilder;
import java.util.ArrayList;
import java.util.List;

public class MatchServiceClient {

    private final static String API_KEY = "d4e6be6c46924236b1bcbac859d3e329";
    private static Logger logger = LoggerFactory.getLogger(MatchServiceClient.class.getName());
    private static final int SEASON_ID = 638;

    public static List<Match> getMatchesFromApi(){

        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        WebResource service = client.resource(UriBuilder.fromUri("http://api.football-data.org/v2/competitions/SA/matches").build());

        ClientResponse response = service.header("X-Auth-Token", API_KEY).get(ClientResponse.class);
        if(response.getStatus() != 200){
            logger.error("matches API not working");
            return null;
        }

        List<Match> matches = new ArrayList<Match>();

        JSONObject jsonResponse = new JSONObject(response.getEntity(String.class));
        JSONArray matchesArray = jsonResponse.getJSONArray("matches");
        for(int i=0;i< matchesArray.length(); i++){
            JSONObject matchObject = matchesArray.getJSONObject(i);
            JSONObject seasonObject = matchObject.getJSONObject("season");
            if(seasonObject.getInt("id") == SEASON_ID){
                try {
                    int matchId = matchObject.getInt("id");
                    int matchDay = matchObject.getInt("matchday");
                    String homeTeam = matchObject.getJSONObject("homeTeam").getString("name");
                    String awayTeam = matchObject.getJSONObject("awayTeam").getString("name");
                    JSONObject fullTimeScore = matchObject.getJSONObject("score").getJSONObject("fullTime");
                    int homeTeamScore = fullTimeScore.getInt("homeTeam");
                    int awayTeamScore = fullTimeScore.getInt("awayTeam");
                    String score = homeTeamScore + "-" + awayTeamScore;
                    String winner = "NONE";
                    if (homeTeamScore > awayTeamScore) {
                        winner = homeTeam;
                    } else if (awayTeamScore > homeTeamScore) {
                        winner = awayTeam;
                    }
                    Match match = new Match(matchId, matchDay, homeTeam, awayTeam, winner, score);
                    matches.add(match);
                }catch (Exception e){

                }
            }

        }

        return matches;
    }
}
