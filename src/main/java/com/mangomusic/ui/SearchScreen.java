package com.mangomusic.ui;

import com.mangomusic.data.*;
import com.mangomusic.models.*;
import com.mangomusic.util.ConsoleColors;
import com.mangomusic.util.InputValidator;

import java.util.List;

/**
 * Screen for interactive search and query functions
 */
public class SearchScreen {

    private final ArtistDao artistDao;
    private final AlbumDao albumDao;
    private final UserDao userDao;
    private final AlbumPlayDao albumPlayDao;

    public SearchScreen(ArtistDao artistDao, AlbumDao albumDao, UserDao userDao, AlbumPlayDao albumPlayDao) {
        this.artistDao = artistDao;
        this.albumDao = albumDao;
        this.userDao = userDao;
        this.albumPlayDao = albumPlayDao;
    }

    /**
     * Displays the search menu
     */
    public void display() {
        boolean running = true;

        while (running) {
            InputValidator.clearScreen();
            displayMenu();

            int choice = InputValidator.getIntInRange("Select an option: ", 0, 6);

            switch (choice) {
                case 1:
                    searchArtists();
                    break;
                case 2:
                    viewArtistAlbums();
                    break;
                case 3:
                    lookupUser();
                    break;
                case 4:
                    viewUserPlays();
                    break;
                case 5:
                    findAlbumsByGenre();
                    break;
                case 6:
                    searchAlbums();
                    break;
                case 0:
                    running = false;
                    break;
            }
        }
    }

    /**
     * Displays the search menu options
     */
    private void displayMenu() {
        ConsoleColors.printHeader("SEARCH & QUERIES");

        System.out.println("\n1. Search for an artist");
        System.out.println("2. View artist's albums");
        System.out.println("3. Look up user by username");
        System.out.println("4. View user's recent plays");
        System.out.println("5. Find albums by genre");
        System.out.println("6. Search albums by title");
        System.out.println("0. Back to main menu");
        System.out.println();
    }

    /**
     * Search for artists by name
     */
    private void searchArtists() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Search Artists");

        String searchTerm = InputValidator.getNonEmptyString("\nEnter artist name to search: ");

        List<Artist> artists = artistDao.searchArtists(searchTerm);

        if (artists.isEmpty()) {
            ConsoleColors.printWarning("No artists found matching: " + searchTerm);
        } else {
            System.out.println("\nFound " + artists.size() + " artist(s):\n");
            System.out.println(Artist.getHeader());
            System.out.println("-".repeat(70));
            for (Artist artist : artists) {
                System.out.println(artist);
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * View albums for a specific artist
     */
    private void viewArtistAlbums() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("View Artist Albums");

        int artistId = InputValidator.getIntInRange("\nEnter artist ID: ", 1, Integer.MAX_VALUE);

        List<Album> albums = albumDao.getAlbumsByArtist(artistId);

        if (albums.isEmpty()) {
            ConsoleColors.printWarning("No albums found for artist ID: " + artistId);
        } else {
            System.out.println("\nFound " + albums.size() + " album(s):\n");
            System.out.println(Album.getHeader());
            System.out.println("-".repeat(130));
            for (Album album : albums) {
                System.out.println(album);
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Look up a user by username
     */
    private void lookupUser() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Look Up User");

        String username = InputValidator.getNonEmptyString("\nEnter username to search: ");

        List<User> users = userDao.searchUsers(username);

        if (users.isEmpty()) {
            ConsoleColors.printWarning("No users found matching: " + username);
        } else {
            System.out.println("\nFound " + users.size() + " user(s):\n");
            System.out.println(User.getHeader());
            System.out.println("-".repeat(90));
            for (User user : users) {
                System.out.println(user);
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * View recent plays for a user
     */
    private void viewUserPlays() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("View User's Recent Plays");

        int userId = InputValidator.getIntInRange("\nEnter user ID: ", 1, Integer.MAX_VALUE);
        int limit = InputValidator.getIntInRange("Number of plays to show (max 50): ", 1, 50);

        List<AlbumPlay> plays = albumPlayDao.getUserRecentPlays(userId, limit);

        if (plays.isEmpty()) {
            ConsoleColors.printWarning("No plays found for user ID: " + userId);
        } else {
            System.out.println("\nShowing " + plays.size() + " most recent play(s):\n");
            System.out.println(AlbumPlay.getHeader());
            System.out.println("-".repeat(120));
            for (AlbumPlay play : plays) {
                System.out.println(play);
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Find albums by genre
     */
    private void findAlbumsByGenre() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Find Albums by Genre");

        // Show available genres
        List<String> genres = artistDao.getAllGenres();
        System.out.println("\nAvailable genres:");
        for (int i = 0; i < genres.size(); i++) {
            System.out.print(genres.get(i));
            if (i < genres.size() - 1) {
                System.out.print(", ");
            }
        }
        System.out.println("\n");

        String genre = InputValidator.getNonEmptyString("Enter genre: ");

        List<Album> albums = albumDao.getAlbumsByGenre(genre);

        if (albums.isEmpty()) {
            ConsoleColors.printWarning("No albums found for genre: " + genre);
        } else {
            System.out.println("\nFound " + albums.size() + " album(s):\n");
            System.out.println(Album.getHeader());
            System.out.println("-".repeat(130));

            // Show first 50 to avoid overwhelming output
            int displayCount = Math.min(albums.size(), 50);
            for (int i = 0; i < displayCount; i++) {
                System.out.println(albums.get(i));
            }

            if (albums.size() > 50) {
                System.out.println("\n... and " + (albums.size() - 50) + " more albums");
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Search albums by title
     */
    private void searchAlbums() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Search Albums");

        String searchTerm = InputValidator.getNonEmptyString("\nEnter album title to search: ");

        List<Album> albums = albumDao.searchAlbums(searchTerm);

        if (albums.isEmpty()) {
            ConsoleColors.printWarning("No albums found matching: " + searchTerm);
        } else {
            System.out.println("\nFound " + albums.size() + " album(s):\n");
            System.out.println(Album.getHeader());
            System.out.println("-".repeat(130));
            for (Album album : albums) {
                System.out.println(album);
            }
        }

        InputValidator.pressEnterToContinue();
    }
}