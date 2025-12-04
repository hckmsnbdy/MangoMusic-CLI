package com.mangomusic.data;

import com.mangomusic.models.ReportResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ReportsDao {

    private final DataManager dataManager;

    public ReportsDao(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public List<ReportResult> getDailyActiveUsersReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    DATE(played_at) as activity_date, " +
                "    COUNT(DISTINCT user_id) as daily_active_users " +
                "FROM album_plays " +
                "WHERE played_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) " +
                "GROUP BY DATE(played_at) " +
                "ORDER BY activity_date DESC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("activity_date", rs.getDate("activity_date").toLocalDate());
                    result.addColumn("daily_active_users", rs.getInt("daily_active_users"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Daily Active Users report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getTopAlbumsThisMonthReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    al.title as album_title, " +
                "    ar.name as artist_name, " +
                "    COUNT(*) as play_count " +
                "FROM album_plays ap " +
                "JOIN albums al ON ap.album_id = al.album_id " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "WHERE YEAR(ap.played_at) = YEAR(CURDATE()) " +
                "  AND MONTH(ap.played_at) = MONTH(CURDATE()) " +
                "GROUP BY al.album_id, al.title, ar.name " +
                "ORDER BY play_count DESC " +
                "LIMIT 10";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("album_title", rs.getString("album_title"));
                    result.addColumn("artist_name", rs.getString("artist_name"));
                    result.addColumn("play_count", rs.getInt("play_count"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Top Albums This Month report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getUserRetentionReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    DATE_FORMAT(u.signup_date, '%Y-%W') as signup_week, " +
                "    COUNT(DISTINCT u.user_id) as total_signups, " +
                "    COUNT(DISTINCT CASE " +
                "        WHEN ap.played_at >= DATE_ADD(u.signup_date, INTERVAL 7 DAY) " +
                "         AND ap.played_at < DATE_ADD(u.signup_date, INTERVAL 14 DAY) " +
                "        THEN u.user_id " +
                "    END) as retained_users, " +
                "    ROUND( " +
                "        COUNT(DISTINCT CASE " +
                "            WHEN ap.played_at >= DATE_ADD(u.signup_date, INTERVAL 7 DAY) " +
                "             AND ap.played_at < DATE_ADD(u.signup_date, INTERVAL 14 DAY) " +
                "            THEN u.user_id " +
                "        END) * 100.0 / COUNT(DISTINCT u.user_id), " +
                "        2 " +
                "    ) as retention_rate_percent " +
                "FROM users u " +
                "LEFT JOIN album_plays ap ON u.user_id = ap.user_id " +
                "WHERE u.signup_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY) " +
                "GROUP BY DATE_FORMAT(u.signup_date, '%Y-%W') " +
                "ORDER BY signup_week DESC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("signup_week", rs.getString("signup_week"));
                    result.addColumn("total_signups", rs.getInt("total_signups"));
                    result.addColumn("retained_users", rs.getInt("retained_users"));
                    result.addColumn("retention_rate_percent", rs.getDouble("retention_rate_percent"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running User Retention report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getMonthlyActiveByCountryReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    u.country, " +
                "    DATE_FORMAT(ap.played_at, '%Y-%m') as activity_month, " +
                "    COUNT(DISTINCT u.user_id) as monthly_active_users " +
                "FROM album_plays ap " +
                "JOIN users u ON ap.user_id = u.user_id " +
                "WHERE ap.played_at >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) " +
                "GROUP BY u.country, DATE_FORMAT(ap.played_at, '%Y-%m') " +
                "HAVING COUNT(DISTINCT u.user_id) >= 1 " +
                "ORDER BY activity_month DESC, monthly_active_users DESC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("country", rs.getString("country"));
                    result.addColumn("activity_month", rs.getString("activity_month"));
                    result.addColumn("monthly_active_users", rs.getInt("monthly_active_users"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Monthly Active by Country report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getArtistRevenueReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    ar.name as artist_name, " +
                "    ar.primary_genre, " +
                "    COUNT(ap.play_id) as total_plays, " +
                "    SUM(CASE WHEN u.subscription_type = 'premium' THEN 1 ELSE 0 END) as premium_plays, " +
                "    SUM(CASE WHEN u.subscription_type = 'free' THEN 1 ELSE 0 END) as free_plays, " +
                "    ROUND( " +
                "        (SUM(CASE WHEN u.subscription_type = 'premium' THEN 0.004 ELSE 0.001 END)), " +
                "        2 " +
                "    ) as estimated_revenue_usd " +
                "FROM album_plays ap " +
                "JOIN albums al ON ap.album_id = al.album_id " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "JOIN users u ON ap.user_id = u.user_id " +
                "GROUP BY ar.artist_id, ar.name, ar.primary_genre " +
                "ORDER BY estimated_revenue_usd DESC " +
                "LIMIT 50";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("artist_name", rs.getString("artist_name"));
                    result.addColumn("primary_genre", rs.getString("primary_genre"));
                    result.addColumn("total_plays", rs.getInt("total_plays"));
                    result.addColumn("premium_plays", rs.getInt("premium_plays"));
                    result.addColumn("free_plays", rs.getInt("free_plays"));
                    result.addColumn("estimated_revenue_usd", rs.getDouble("estimated_revenue_usd"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Artist Revenue report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getChurnRiskReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    u.user_id, " +
                "    u.username, " +
                "    u.email, " +
                "    u.country, " +
                "    DATEDIFF(CURDATE(), MAX(ap.played_at)) as days_since_last_play, " +
                "    COUNT(ap.play_id) as lifetime_plays, " +
                "    CASE " +
                "        WHEN DATEDIFF(CURDATE(), MAX(ap.played_at)) >= 30 THEN 'High Risk' " +
                "        WHEN DATEDIFF(CURDATE(), MAX(ap.played_at)) >= 21 THEN 'Medium Risk' " +
                "        WHEN DATEDIFF(CURDATE(), MAX(ap.played_at)) >= 14 THEN 'Low Risk' " +
                "    END as churn_risk_level " +
                "FROM users u " +
                "LEFT JOIN album_plays ap ON u.user_id = ap.user_id " +
                "WHERE u.subscription_type = 'premium' " +
                "GROUP BY u.user_id, u.username, u.email, u.country " +
                "HAVING DATEDIFF(CURDATE(), MAX(ap.played_at)) >= 14 " +
                "ORDER BY days_since_last_play DESC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("user_id", rs.getInt("user_id"));
                    result.addColumn("username", rs.getString("username"));
                    result.addColumn("email", rs.getString("email"));
                    result.addColumn("country", rs.getString("country"));
                    result.addColumn("days_since_last_play", rs.getInt("days_since_last_play"));
                    result.addColumn("lifetime_plays", rs.getInt("lifetime_plays"));
                    result.addColumn("churn_risk_level", rs.getString("churn_risk_level"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Churn Risk report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getListeningBySubscriptionReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    u.subscription_type, " +
                "    COUNT(DISTINCT u.user_id) as total_users, " +
                "    COUNT(ap.play_id) as total_plays, " +
                "    ROUND(COUNT(ap.play_id) / COUNT(DISTINCT u.user_id), 2) as avg_plays_per_user " +
                "FROM users u " +
                "LEFT JOIN album_plays ap ON u.user_id = ap.user_id " +
                "GROUP BY u.subscription_type " +
                "ORDER BY u.subscription_type";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("subscription_type", rs.getString("subscription_type"));
                    result.addColumn("total_users", rs.getInt("total_users"));
                    result.addColumn("total_plays", rs.getInt("total_plays"));
                    result.addColumn("avg_plays_per_user", rs.getDouble("avg_plays_per_user"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Listening by Subscription report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getMostActiveCountriesReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    u.country, " +
                "    COUNT(DISTINCT u.user_id) as total_users, " +
                "    COUNT(ap.play_id) as total_plays, " +
                "    ROUND(COUNT(ap.play_id) / COUNT(DISTINCT u.user_id), 2) as avg_plays_per_user " +
                "FROM users u " +
                "JOIN album_plays ap ON u.user_id = ap.user_id " +
                "GROUP BY u.country " +
                "ORDER BY total_plays DESC " +
                "LIMIT 15";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("country", rs.getString("country"));
                    result.addColumn("total_users", rs.getInt("total_users"));
                    result.addColumn("total_plays", rs.getInt("total_plays"));
                    result.addColumn("avg_plays_per_user", rs.getDouble("avg_plays_per_user"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Most Active Countries report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getGenrePopularityReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    ar.primary_genre as genre, " +
                "    COUNT(DISTINCT al.album_id) as total_albums, " +
                "    COUNT(ap.play_id) as total_plays, " +
                "    COUNT(DISTINCT ar.artist_id) as total_artists " +
                "FROM album_plays ap " +
                "JOIN albums al ON ap.album_id = al.album_id " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "GROUP BY ar.primary_genre " +
                "ORDER BY total_plays DESC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("genre", rs.getString("genre"));
                    result.addColumn("total_albums", rs.getInt("total_albums"));
                    result.addColumn("total_plays", rs.getInt("total_plays"));
                    result.addColumn("total_artists", rs.getInt("total_artists"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Genre Popularity report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getUserGrowthReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    DATE_FORMAT(signup_date, '%Y-%m') as signup_month, " +
                "    COUNT(user_id) as new_users, " +
                "    SUM(CASE WHEN subscription_type = 'free' THEN 1 ELSE 0 END) as free_signups, " +
                "    SUM(CASE WHEN subscription_type = 'premium' THEN 1 ELSE 0 END) as premium_signups " +
                "FROM users " +
                "WHERE signup_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) " +
                "GROUP BY DATE_FORMAT(signup_date, '%Y-%m') " +
                "ORDER BY signup_month ASC";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("signup_month", rs.getString("signup_month"));
                    result.addColumn("new_users", rs.getInt("new_users"));
                    result.addColumn("free_signups", rs.getInt("free_signups"));
                    result.addColumn("premium_signups", rs.getInt("premium_signups"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running User Growth report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getCompletionRateReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    CASE " +
                "        WHEN completed = TRUE THEN 'Completed' " +
                "        ELSE 'Incomplete' " +
                "    END as play_status, " +
                "    COUNT(play_id) as total_plays, " +
                "    ROUND(COUNT(play_id) * 100.0 / (SELECT COUNT(*) FROM album_plays), 2) as percentage " +
                "FROM album_plays " +
                "GROUP BY completed " +
                "ORDER BY play_status";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("play_status", rs.getString("play_status"));
                    result.addColumn("total_plays", rs.getInt("total_plays"));
                    result.addColumn("percentage", rs.getDouble("percentage"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Completion Rate report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    public List<ReportResult> getTopArtistsReport() {
        List<ReportResult> results = new ArrayList<>();
        String query = "SELECT " +
                "    ar.name as artist_name, " +
                "    ar.primary_genre as genre, " +
                "    COUNT(DISTINCT al.album_id) as total_albums_played, " +
                "    COUNT(ap.play_id) as total_plays, " +
                "    COUNT(DISTINCT ap.user_id) as unique_listeners " +
                "FROM album_plays ap " +
                "JOIN albums al ON ap.album_id = al.album_id " +
                "JOIN artists ar ON al.artist_id = ar.artist_id " +
                "GROUP BY ar.artist_id, ar.name, ar.primary_genre " +
                "ORDER BY total_plays DESC " +
                "LIMIT 20";

        try {
            Connection connection = dataManager.getConnection();

            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    ReportResult result = new ReportResult();
                    result.addColumn("artist_name", rs.getString("artist_name"));
                    result.addColumn("genre", rs.getString("genre"));
                    result.addColumn("total_albums_played", rs.getInt("total_albums_played"));
                    result.addColumn("total_plays", rs.getInt("total_plays"));
                    result.addColumn("unique_listeners", rs.getInt("unique_listeners"));
                    results.add(result);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error running Top Artists report: " + e.getMessage());
            e.printStackTrace();
        }

        return results;
    }

    /**
     * Report: MangoMusic Mapped - Personalized Year in Review
     */
    public ReportResult getMangoMusicMapped(int userId) {
        ReportResult mapped = new ReportResult();

        try {
            Connection connection = dataManager.getConnection();

            // Get user's top artist of the year
            String topArtistQuery = "SELECT ar.name as artist_name, COUNT(*) as play_count " +
                    "FROM album_plays ap " +
                    "JOIN albums al ON ap.album_id = al.album_id " +
                    "JOIN artists ar ON al.artist_id = ar.artist_id " +
                    "WHERE ap.user_id = ? AND YEAR(ap.played_at) = YEAR(CURDATE()) " +
                    "GROUP BY ar.artist_id, ar.name " +
                    "ORDER BY play_count DESC " +
                    "LIMIT 1";

            try (PreparedStatement stmt = connection.prepareStatement(topArtistQuery)) {
                stmt.setInt(1, userId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        mapped.addColumn("top_artist", rs.getString("artist_name"));
                        mapped.addColumn("top_artist_plays", rs.getInt("play_count"));
                    } else {
                        mapped.addColumn("top_artist", "N/A");
                        mapped.addColumn("top_artist_plays", 0);
                    }
                }
            }

            // Get most active month this year
            String topMonthQuery = "SELECT DATE_FORMAT(played_at, '%M') as month_name, " +
                    "       COUNT(*) as play_count " +
                    "FROM album_plays " +
                    "WHERE user_id = ? AND YEAR(played_at) = YEAR(CURDATE()) " +
                    "GROUP BY MONTH(played_at), DATE_FORMAT(played_at, '%M') " +
                    "ORDER BY play_count DESC " +
                    "LIMIT 1";

            try (PreparedStatement stmt = connection.prepareStatement(topMonthQuery)) {
                stmt.setInt(1, userId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        mapped.addColumn("top_month", rs.getString("month_name"));
                        mapped.addColumn("top_month_plays", rs.getInt("play_count"));
                    } else {
                        mapped.addColumn("top_month", "N/A");
                        mapped.addColumn("top_month_plays", 0);
                    }
                }
            }

            // Get total plays this year
            String totalPlaysQuery = "SELECT COUNT(*) as total_plays, " +
                    "       COUNT(DISTINCT al.album_id) as unique_albums, " +
                    "       COUNT(DISTINCT ar.artist_id) as unique_artists, " +
                    "       SUM(CASE WHEN ap.completed = TRUE THEN 1 ELSE 0 END) as completed_plays " +
                    "FROM album_plays ap " +
                    "JOIN albums al ON ap.album_id = al.album_id " +
                    "JOIN artists ar ON al.artist_id = ar.artist_id " +
                    "WHERE ap.user_id = ? AND YEAR(ap.played_at) = YEAR(CURDATE())";

            try (PreparedStatement stmt = connection.prepareStatement(totalPlaysQuery)) {
                stmt.setInt(1, userId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        mapped.addColumn("total_plays", rs.getInt("total_plays"));
                        mapped.addColumn("unique_albums", rs.getInt("unique_albums"));
                        mapped.addColumn("unique_artists", rs.getInt("unique_artists"));
                        mapped.addColumn("completed_plays", rs.getInt("completed_plays"));
                    } else {
                        mapped.addColumn("total_plays", 0);
                        mapped.addColumn("unique_albums", 0);
                        mapped.addColumn("unique_artists", 0);
                        mapped.addColumn("completed_plays", 0);
                    }
                }
            }

            // Get top genre
            String topGenreQuery = "SELECT ar.primary_genre, COUNT(*) as play_count " +
                    "FROM album_plays ap " +
                    "JOIN albums al ON ap.album_id = al.album_id " +
                    "JOIN artists ar ON al.artist_id = ar.artist_id " +
                    "WHERE ap.user_id = ? AND YEAR(ap.played_at) = YEAR(CURDATE()) " +
                    "GROUP BY ar.primary_genre " +
                    "ORDER BY play_count DESC " +
                    "LIMIT 1";

            try (PreparedStatement stmt = connection.prepareStatement(topGenreQuery)) {
                stmt.setInt(1, userId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        mapped.addColumn("top_genre", rs.getString("primary_genre"));
                    } else {
                        mapped.addColumn("top_genre", "N/A");
                    }
                }
            }

            // Calculate listener personality based on stats
            int totalPlays = mapped.getInt("total_plays");
            int uniqueArtists = mapped.getInt("unique_artists");
            int completedPlays = mapped.getInt("completed_plays");

            String personality = determineListenerPersonality(totalPlays, uniqueArtists, completedPlays);
            mapped.addColumn("listener_personality", personality);

            // Get fun streak stat - longest consecutive days of listening
            String streakQuery = "WITH daily_plays AS ( " +
                    "    SELECT DISTINCT DATE(played_at) as play_date " +
                    "    FROM album_plays " +
                    "    WHERE user_id = ? AND YEAR(played_at) = YEAR(CURDATE()) " +
                    "), " +
                    "date_groups AS ( " +
                    "    SELECT " +
                    "        play_date, " +
                    "        DATE_SUB(play_date, INTERVAL ROW_NUMBER() OVER (ORDER BY play_date) DAY) as grp " +
                    "    FROM daily_plays " +
                    ") " +
                    "SELECT COALESCE(MAX(streak_length), 0) as max_streak " +
                    "FROM ( " +
                    "    SELECT COUNT(*) as streak_length " +
                    "    FROM date_groups " +
                    "    GROUP BY grp " +
                    ") streaks";

            try (PreparedStatement stmt = connection.prepareStatement(streakQuery)) {
                stmt.setInt(1, userId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        mapped.addColumn("longest_streak", rs.getInt("max_streak"));
                    } else {
                        mapped.addColumn("longest_streak", 0);
                    }
                }
            }

            // Get current year for display
            String yearQuery = "SELECT YEAR(CURDATE()) as current_year";
            try (PreparedStatement stmt = connection.prepareStatement(yearQuery);
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    mapped.addColumn("year", rs.getInt("current_year"));
                } else {
                    mapped.addColumn("year", 2025);
                }
            }

        } catch (SQLException e) {
            System.err.println("Error generating MangoMusic Mapped: " + e.getMessage());
            e.printStackTrace();
        }

        return mapped;
    }

    /**
     * Determines listener personality based on listening patterns
     */
    private String determineListenerPersonality(int totalPlays, int uniqueArtists, int completedPlays) {
        if (totalPlays == 0) {
            return "New Explorer";
        }

        double completionRate = (double) completedPlays / totalPlays;
        double artistDiversity = (double) totalPlays / Math.max(uniqueArtists, 1);

        if (totalPlays > 500 && completionRate > 0.85) {
            return "Devoted Audiophile";
        } else if (totalPlays > 300 && uniqueArtists > 50) {
            return "Genre Wanderer";
        } else if (artistDiversity > 15 && completionRate > 0.75) {
            return "Loyal Fan";
        } else if (uniqueArtists > 40 && completionRate < 0.70) {
            return "Playlist Skipper";
        } else if (totalPlays > 400) {
            return "Music Enthusiast";
        } else if (uniqueArtists < 10) {
            return "True Believer";
        } else if (completionRate > 0.80) {
            return "Patient Listener";
        } else if (totalPlays > 200) {
            return "Casual Curator";
        } else {
            return "Emerging Fan";
        }
    }
}