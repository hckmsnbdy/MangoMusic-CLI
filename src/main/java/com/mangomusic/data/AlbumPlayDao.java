package com.mangomusic.data;

import com.mangomusic.models.AlbumPlay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class AlbumPlayDao {

    private final DataManager dataManager;

    public AlbumPlayDao(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public List<AlbumPlay> getUserRecentPlays(int userId, int limit) {
        List<AlbumPlay> plays = new ArrayList<>();
        String query = "SELECT ap.play_id, ap.user_id, ap.album_id, ap.played_at, ap.completed, " +
                "       al.title as album_title, ar.name as artist_name " +
                "FROM album_plays ap " +
                "JOIN albums al ON ap.album_id = al.album_id " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE ap.user_id = ? " +
                "ORDER BY ap.played_at DESC " +
                "LIMIT ?";

        try {
            Connection connection = dataManager.getConnection();
            PreparedStatement statement = connection.prepareStatement(query);

            statement.setInt(1, userId);
            statement.setInt(2, limit);

            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    long playId = results.getLong("play_id");
                    int uid = results.getInt("user_id");
                    int albumId = results.getInt("album_id");
                    LocalDateTime playedAt = results.getTimestamp("played_at").toLocalDateTime();
                    boolean completed = results.getBoolean("completed");
                    String albumTitle = results.getString("album_title");
                    String artistName = results.getString("artist_name");

                    plays.add(new AlbumPlay(playId, uid, albumId, playedAt, completed, albumTitle, artistName));
                }
            }

            statement.close();

        } catch (SQLException e) {
            System.err.println("Error getting user plays: " + e.getMessage());
            e.printStackTrace();
        }

        return plays;
    }
}