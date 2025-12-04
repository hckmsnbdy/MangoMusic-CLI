package com.mangomusic.ui;

import com.mangomusic.data.ReportsDao;
import com.mangomusic.models.ReportResult;
import com.mangomusic.util.ConsoleColors;
import com.mangomusic.util.InputValidator;

import java.util.List;

/**
 * Screen for viewing analytics reports
 * All reports from Week 1 (bugs + features) - now working correctly
 */
public class ReportsScreen {

    private final ReportsDao reportsDao;

    public ReportsScreen(ReportsDao reportsDao) {
        this.reportsDao = reportsDao;
    }

    /**
     * Displays the reports menu
     */
    public void display() {
        boolean running = true;

        while (running) {
            InputValidator.clearScreen();
            displayMenu();

            int choice = InputValidator.getIntInRange("Select an option: ", 0, 11);

            switch (choice) {
                case 1:
                    showDailyActiveUsers();
                    break;
                case 2:
                    showMonthlyActiveByCountry();
                    break;
                case 3:
                    showUserGrowth();
                    break;
                case 4:
                    showTopAlbumsThisMonth();
                    break;
                case 5:
                    showGenrePopularity();
                    break;
                case 6:
                    showTopArtists();
                    break;
                case 7:
                    showCompletionRate();
                    break;
                case 8:
                    showUserRetention();
                    break;
                case 9:
                    showListeningBySubscription();
                    break;
                case 10:
                    showArtistRevenue();
                    break;
                case 11:
                    showChurnRisk();
                    break;
                case 0:
                    running = false;
                    break;
            }
        }
    }

    /**
     * Displays the reports menu options
     */
    private void displayMenu() {
        ConsoleColors.printHeader("ANALYTICS REPORTS");

        System.out.println("\nPLATFORM METRICS:");
        System.out.println("1. Daily Active Users Report");
        System.out.println("2. Monthly Active Users by Country");
        System.out.println("3. User Growth by Month");

        System.out.println("\nCONTENT PERFORMANCE:");
        System.out.println("4. Top Albums This Month");
        System.out.println("5. Genre Popularity Rankings");
        System.out.println("6. Top Artists by Play Count");
        System.out.println("7. Album Completion Rate Summary");

        System.out.println("\nUSER INSIGHTS:");
        System.out.println("8. User Retention (7-Day)");
        System.out.println("9. Listening by Subscription Type");
        System.out.println("10. Artist Revenue Projection");
        System.out.println("11. Churn Risk Users");

        System.out.println("\n0. Back to main menu");
        System.out.println();
    }

    /**
     * Shows Daily Active Users report
     */
    private void showDailyActiveUsers() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Daily Active Users Report");
        System.out.println("Shows unique users who played at least one album per day (past 30 days)\n");

        List<ReportResult> results = reportsDao.getDailyActiveUsersReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-15s %20s%n", "Date", "Daily Active Users");
            System.out.println("-".repeat(40));

            for (ReportResult result : results) {
                System.out.printf("%-15s %20s%n",
                        result.getString("activity_date"),
                        result.getInt("daily_active_users"));
            }

            System.out.println("\nTotal days shown: " + results.size());
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Monthly Active Users by Country report
     */
    private void showMonthlyActiveByCountry() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Monthly Active Users by Country");
        System.out.println("Shows unique active users per month, grouped by country (past 6 months)\n");

        List<ReportResult> results = reportsDao.getMonthlyActiveByCountryReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-10s %-15s %20s%n", "Country", "Month", "Monthly Active Users");
            System.out.println("-".repeat(50));

            int displayCount = Math.min(results.size(), 50);
            for (int i = 0; i < displayCount; i++) {
                ReportResult result = results.get(i);
                System.out.printf("%-10s %-15s %20s%n",
                        result.getString("country"),
                        result.getString("activity_month"),
                        result.getInt("monthly_active_users"));
            }

            if (results.size() > 50) {
                System.out.println("\n... and " + (results.size() - 50) + " more rows");
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows User Growth by Month report
     */
    private void showUserGrowth() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("User Growth by Month");
        System.out.println("Shows new user signups per month (past 12 months)\n");

        List<ReportResult> results = reportsDao.getUserGrowthReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-15s %15s %15s %18s%n",
                    "Month", "New Users", "Free Signups", "Premium Signups");
            System.out.println("-".repeat(70));

            for (ReportResult result : results) {
                System.out.printf("%-15s %15s %15s %18s%n",
                        result.getString("signup_month"),
                        result.getInt("new_users"),
                        result.getInt("free_signups"),
                        result.getInt("premium_signups"));
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Top Albums This Month report
     */
    private void showTopAlbumsThisMonth() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Top 10 Most Played Albums This Month");
        System.out.println("Shows the most popular albums in the current month\n");

        List<ReportResult> results = reportsDao.getTopAlbumsThisMonthReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this month.");
        } else {
            System.out.printf("%-50s %-40s %12s%n", "Album", "Artist", "Play Count");
            System.out.println("-".repeat(105));

            for (ReportResult result : results) {
                System.out.printf("%-50s %-40s %12s%n",
                        truncate(result.getString("album_title"), 50),
                        truncate(result.getString("artist_name"), 40),
                        result.getInt("play_count"));
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Genre Popularity report
     */
    private void showGenrePopularity() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Genre Popularity Rankings");
        System.out.println("Shows all genres ranked by total play count\n");

        List<ReportResult> results = reportsDao.getGenrePopularityReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-20s %15s %15s %15s%n",
                    "Genre", "Total Albums", "Total Plays", "Total Artists");
            System.out.println("-".repeat(70));

            for (ReportResult result : results) {
                System.out.printf("%-20s %15s %15s %15s%n",
                        result.getString("genre"),
                        result.getInt("total_albums"),
                        result.getInt("total_plays"),
                        result.getInt("total_artists"));
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Top Artists report
     */
    private void showTopArtists() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Top 20 Artists by Play Count");
        System.out.println("Shows the most popular artists on the platform\n");

        List<ReportResult> results = reportsDao.getTopArtistsReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-40s %-20s %12s %12s %18s%n",
                    "Artist", "Genre", "Albums", "Plays", "Unique Listeners");
            System.out.println("-".repeat(110));

            for (ReportResult result : results) {
                System.out.printf("%-40s %-20s %12s %12s %18s%n",
                        truncate(result.getString("artist_name"), 40),
                        truncate(result.getString("genre"), 20),
                        result.getInt("total_albums_played"),
                        result.getInt("total_plays"),
                        result.getInt("unique_listeners"));
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Completion Rate report
     */
    private void showCompletionRate() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Album Completion Rate Summary");
        System.out.println("Shows overall platform engagement - completed vs incomplete plays\n");

        List<ReportResult> results = reportsDao.getCompletionRateReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-15s %15s %15s%n", "Status", "Total Plays", "Percentage");
            System.out.println("-".repeat(50));

            for (ReportResult result : results) {
                System.out.printf("%-15s %15s %14.2f%%%n",
                        result.getString("play_status"),
                        result.getInt("total_plays"),
                        result.getDouble("percentage"));
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows User Retention report
     */
    private void showUserRetention() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("User Retention (7-Day) Report");
        System.out.println("Shows what % of new signups return 7 days later (past 90 days)\n");

        List<ReportResult> results = reportsDao.getUserRetentionReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-15s %15s %18s %22s%n",
                    "Signup Week", "Total Signups", "Retained Users", "Retention Rate %");
            System.out.println("-".repeat(75));

            for (ReportResult result : results) {
                System.out.printf("%-15s %15s %18s %21.2f%%%n",
                        result.getString("signup_week"),
                        result.getInt("total_signups"),
                        result.getInt("retained_users"),
                        result.getDouble("retention_rate_percent"));
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Listening by Subscription Type report
     */
    private void showListeningBySubscription() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Listening Activity by Subscription Type");
        System.out.println("Compares engagement between free and premium users\n");

        List<ReportResult> results = reportsDao.getListeningBySubscriptionReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-18s %15s %15s %22s%n",
                    "Subscription Type", "Total Users", "Total Plays", "Avg Plays Per User");
            System.out.println("-".repeat(75));

            for (ReportResult result : results) {
                System.out.printf("%-18s %15s %15s %21.2f%n",
                        result.getString("subscription_type"),
                        result.getInt("total_users"),
                        result.getInt("total_plays"),
                        result.getDouble("avg_plays_per_user"));
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Artist Revenue report
     */
    private void showArtistRevenue() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Artist Revenue Projection (Top 50)");
        System.out.println("Estimated revenue: Premium = $0.004/play, Free = $0.001/play\n");

        List<ReportResult> results = reportsDao.getArtistRevenueReport();

        if (results.isEmpty()) {
            ConsoleColors.printWarning("No data available for this report.");
        } else {
            System.out.printf("%-40s %-20s %12s %15s %12s %18s%n",
                    "Artist", "Genre", "Total Plays", "Premium Plays", "Free Plays", "Est. Revenue USD");
            System.out.println("-".repeat(125));

            int displayCount = Math.min(results.size(), 30);
            for (int i = 0; i < displayCount; i++) {
                ReportResult result = results.get(i);
                System.out.printf("%-40s %-20s %12s %15s %12s %17.2f%n",
                        truncate(result.getString("artist_name"), 40),
                        truncate(result.getString("primary_genre"), 20),
                        result.getInt("total_plays"),
                        result.getInt("premium_plays"),
                        result.getInt("free_plays"),
                        result.getDouble("estimated_revenue_usd"));
            }

            if (results.size() > 30) {
                System.out.println("\n... and " + (results.size() - 30) + " more artists");
            }
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Shows Churn Risk report
     */
    private void showChurnRisk() {
        InputValidator.clearScreen();
        ConsoleColors.printSection("Churn Risk Users");
        System.out.println("Premium users who haven't played in 14+ days\n");

        List<ReportResult> results = reportsDao.getChurnRiskReport();

        if (results.isEmpty()) {
            ConsoleColors.printSuccess("Great news! No premium users are at risk of churning.");
        } else {
            System.out.printf("%-20s %-30s %10s %20s %18s %15s%n",
                    "Username", "Email", "Country", "Days Since Last Play", "Lifetime Plays", "Risk Level");
            System.out.println("-".repeat(125));

            int displayCount = Math.min(results.size(), 30);
            for (int i = 0; i < displayCount; i++) {
                ReportResult result = results.get(i);
                System.out.printf("%-20s %-30s %10s %20s %18s %15s%n",
                        truncate(result.getString("username"), 20),
                        truncate(result.getString("email"), 30),
                        result.getString("country"),
                        result.getInt("days_since_last_play"),
                        result.getInt("lifetime_plays"),
                        result.getString("churn_risk_level"));
            }

            if (results.size() > 30) {
                System.out.println("\n... and " + (results.size() - 30) + " more users at risk");
            }

            System.out.println("\nTotal users at risk: " + results.size());
        }

        InputValidator.pressEnterToContinue();
    }

    /**
     * Helper method to truncate strings for display
     */
    private String truncate(String text, int maxLength) {
        if (text == null) return "";
        if (text.length() <= maxLength) return text;
        return text.substring(0, maxLength - 3) + "...";
    }
}