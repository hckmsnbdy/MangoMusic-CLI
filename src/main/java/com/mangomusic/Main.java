package com.mangomusic;

import com.mangomusic.config.DatabaseConfig;
import com.mangomusic.data.*;
import com.mangomusic.ui.HomeScreen;
import com.mangomusic.ui.ReportsScreen;
import com.mangomusic.ui.SearchScreen;
import com.mangomusic.ui.SpecialReportsScreen;

public class Main {

    public static void main(String[] args) {
        System.out.println("Initializing MangoMusic Database Tool...\n");

        DataManager dataManager = null;

        try {
            dataManager = new DataManager();

            ArtistDao artistDao = new ArtistDao(dataManager);
            AlbumDao albumDao = new AlbumDao(dataManager);
            UserDao userDao = new UserDao(dataManager);
            AlbumPlayDao albumPlayDao = new AlbumPlayDao(dataManager);
            ReportsDao reportsDao = new ReportsDao(dataManager);

            SearchScreen searchScreen = new SearchScreen(artistDao, albumDao, userDao, albumPlayDao);
            ReportsScreen reportsScreen = new ReportsScreen(reportsDao);
            SpecialReportsScreen specialReportsScreen = new SpecialReportsScreen(reportsDao);
            HomeScreen homeScreen = new HomeScreen(searchScreen, reportsScreen, specialReportsScreen);

            homeScreen.display();

        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (dataManager != null) {
                dataManager.close();
            }
            DatabaseConfig.closeDataSource();
        }
    }
}