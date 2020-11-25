package com.github.db;

import com.github.soccer.dto.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DbManager {

    private static Connection connection;
    private static Logger logger = LoggerFactory.getLogger(DbManager.class.getName());

    public static String GET_ALL_MATCHES = "SELECT * FROM MATCH";
    public static String INSERT_MATCH = "insert into match(id,match_day ,home_team ,away_team ,winner ,score ) values (?,?,?,?,?,?)";

    public static Connection getConnection(){
        if(connection == null){
            try {
                connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                logger.error("Errore di connessione al db",e);
            }
        }

        return  connection;
    }

    public static List<Match> getMatches() throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();

        List<Match> matches = new ArrayList<Match>();
        ResultSet resultSet = statement.executeQuery(GET_ALL_MATCHES);
        while(resultSet.next()){
            Match match = new Match();
            match.setId(resultSet.getInt(1));
            match.setMatchDay(resultSet.getInt(2));
            match.setHomeTeam(resultSet.getString(3));
            match.setAwayTeam(resultSet.getString(4));
            match.setWinner(resultSet.getString(5));
            match.setScore(resultSet.getString(6));
            matches.add(match);
        }

        return matches;
    }

    public static void insertMatches(List<Match> matches) throws SQLException {
        List<Match> currentMatches = getMatches();
        List<Match> matchToInsert = matches.stream().filter(m -> !currentMatches.contains(m)).collect(Collectors.toList());

        Connection connection = getConnection();
        for(Match match : matchToInsert){
            PreparedStatement statement = connection.prepareStatement(INSERT_MATCH);
            statement.setInt(1,match.getId());
            statement.setInt(2,match.getMatchDay());
            statement.setString(3,match.getHomeTeam());
            statement.setString(4,match.getAwayTeam());
            statement.setString(5,match.getWinner());
            statement.setString(6,match.getScore());

            statement.executeUpdate();
            statement.close();
        }
    }
}
