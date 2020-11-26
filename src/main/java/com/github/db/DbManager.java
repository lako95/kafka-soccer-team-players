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
    public static String GET_ALL_NO_WINNING_MATCHES = "SELECT * FROM no_winner_match";
    public static String INSERT_MATCH = "insert into match(id,match_day ,home_team ,away_team ,winner ,score ) values (?,?,?,?,?,?)";
    public static String INSERT_NO_WINNING_MATCH = "insert into no_winner_match(id,match_day ,home_team ,away_team ,winner ,score ) values (?,?,?,?,?,?)";


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

    public static List<Match> getAllMatches() throws SQLException {
        return getMatches(GET_ALL_MATCHES);
    }

    public static List<Match> getNoWinnerMatches() throws SQLException {
        return getMatches(GET_ALL_NO_WINNING_MATCHES);
    }


    private static List<Match> getMatches(String query) throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();

        List<Match> matches = new ArrayList<Match>();
        ResultSet resultSet = statement.executeQuery(query);
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

        return  matches;
    }

    public static void insertMatches(List<Match> matches) throws SQLException {
        List<Match> currentMatches = getAllMatches();
        List<Match> matchToInsert = matches.stream().filter(m -> !currentMatches.contains(m)).collect(Collectors.toList());

        insertMatch(matchToInsert, INSERT_MATCH);
    }

    public static void insertNoWinnerMatches(List<Match> matches) throws SQLException {
        List<Match> currentNoWinnerMatches = getNoWinnerMatches();
        List<Match> matchToInsert = matches.stream().filter(m -> !currentNoWinnerMatches.contains(m)).collect(Collectors.toList());

        insertMatch(matchToInsert, INSERT_NO_WINNING_MATCH);
    }

    private static void insertMatch(List<Match> matches, String query) throws SQLException {
        Connection connection = getConnection();
        for(Match match : matches){
            PreparedStatement statement = connection.prepareStatement(query);
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
