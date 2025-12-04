package com.mangomusic.data;

import com.mangomusic.models.Album;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class AlbumDao {

    private final DataManager dataManager;

    public AlbumDao(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public List<Album> getAlbumsByArtist(int artistId) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE al.artist_id = ? " +
                "ORDER BY al.release_year DESC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                statement.setInt(1, artistId);

                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        int albumId = results.getInt("album_id");
                        int artId = results.getInt("artist_id");
                        String title = results.getString("title");
                        int releaseYear = results.getInt("release_year");
                        String artistName = results.getString("artist");

                        albums.add(new Album(albumId, artId, title, releaseYear, artistName));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error getting albums for artist: " + e.getMessage());
            e.printStackTrace();
        }

        return albums;
    }

    public List<Album> getAlbumsByGenre(String genre) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE ar.primary_genre = '" + genre + "' " +
                "ORDER BY al.title";

        try {
            Connection connection = dataManager.getConnection();

            try (Statement statement = connection.createStatement();
                 ResultSet results = statement.executeQuery(query)) {

                while (results.next()) {
                    int albumId = results.getInt("album_id");
                    int artistId = results.getInt("artist_id");
                    String title = results.getString("title");
                    int releaseYear = results.getInt("release_year");
                    String artistName = results.getString("artist_name");

                    albums.add(new Album(albumId, artistId, title, releaseYear, artistName));
                }
            }

        } catch (SQLException e) {
            System.err.println("Error getting albums by genre: " + e.getMessage());
            e.printStackTrace();
        }

        return albums;
    }

    public List<Album> searchAlbums(String searchTerm) {
        List<Album> albums = new ArrayList<>();
        String query = "SELECT al.album_id, al.artist_id, al.title, al.release_year, ar.name as artist_name " +
                "FROM albums al " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE al.title LIKE ? " +
                "ORDER BY al.title";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                statement.setString(1, "%" + searchTerm + "%");

                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        int albumId = results.getInt("album_id");
                        String artistIdStr = results.getString("artist_id");
                        int artistId = Integer.parseInt(artistIdStr);
                        String title = results.getString("title");
                        int releaseYear = results.getInt("release_year");
                        String artistName = results.getString("artist_name");

                        albums.add(new Album(albumId, artistId, title, releaseYear, artistName));
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error searching for albums: " + e.getMessage());
            e.printStackTrace();
        }

        return albums;
    }
}