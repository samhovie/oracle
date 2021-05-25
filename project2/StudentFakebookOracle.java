package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*

    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

            FirstNameInfo info = new FirstNameInfo();
            ResultSet rst = stmt.executeQuery( 
                "SELECT DISTINCT first_name " +
                "FROM " + UsersTable + " " +
                "WHERE LENGTH(first_name) = " +
                "(SELECT MAX(LENGTH(first_name)) FROM " + UsersTable + ") " +
                "ORDER BY first_name ASC");
            
            while (rst.next()) {  
                info.addLongName(rst.getString(1));
            }

            rst = stmt.executeQuery( 
                "SELECT DISTINCT first_name " +
                "FROM " + UsersTable + " " +
                "WHERE LENGTH(first_name) = " +
                "(SELECT MIN(LENGTH(first_name)) FROM " + UsersTable + ") " +
                "ORDER BY first_name ASC");

            while (rst.next()) {
                info.addShortName(rst.getString(1));
            }

            rst = stmt.executeQuery(
                "SELECT DISTINCT COUNT(*), first_name " +
                "FROM " + UsersTable + " " +
                "GROUP BY first_name " +
                "HAVING COUNT(*) = " +
                "(SELECT MAX(COUNT(*)) FROM " + UsersTable + " " +
                "GROUP BY first_name) " +
                "ORDER BY first_name ASC ");

            while (rst.next()) {
                info.addCommonName(rst.getString(2));
                info.setCommonNameCount(rst.getInt(1));
            }

            rst.close();
            stmt.close();
            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery( 
                "SELECT user_id, first_name, last_name " +
                "FROM " + UsersTable + " " +
                "WHERE user_id NOT IN " +
                "(SELECT user1_id FROM " + FriendsTable + " " +
                "UNION " +
                "SELECT user2_id FROM " + FriendsTable + ")"
            );
            while(rst.next()) {
                results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }
            rst.close();
            stmt.close();


        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                "SELECT U.user_id, U.first_name, U.last_name " +
                "FROM " + CurrentCitiesTable + " C " +
                "JOIN " + UsersTable + " U " +
                "ON U.user_id = C.user_id " +
                "JOIN " + HometownCitiesTable + " H " +
                "ON U.user_id = H.user_id " +
                "WHERE " +
                "C.current_city_id <> H.hometown_city_id AND " +
                "C.current_city_id IS NOT NULL AND " +
                "H.hometown_city_id IS NOT NULL " +
                "ORDER BY U.user_id ASC "
            );

            while(rst.next()) {
                results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            rst.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;

    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                
                "SELECT P_INFO.num_users, P_INFO.photo_id, P_INFO.album_id, " +
                "P_INFO.photo_link, P_INFO.album_name, U.user_id, U.first_name, U.last_name " +
                "FROM " + UsersTable + " U, " + TagsTable + " T, " +
                "(SELECT COUNT(*) AS num_users, P.photo_id, A.album_id, P.photo_link, A.album_name " +
                "FROM " + PhotosTable + " P, " + AlbumsTable + " A, " + TagsTable + " T " +
                "WHERE P.album_id = A.album_id AND P.photo_id = T.tag_photo_id " +
                "GROUP BY P.photo_id, A.album_id, P.photo_link, A.album_name" +
                ") P_INFO " +
                "WHERE T.tag_subject_id = U.user_id " +
                "AND T.tag_photo_id = P_INFO.photo_id " +
                "ORDER BY users DESC, P_INFO.photo_id, U.user_id"
            
                );

            int current_photo = 0;
            while (rst.next() && current_photo < num) {
                PhotoInfo p = new PhotoInfo(
                    rst.getLong(2),
                    rst.getLong(3),
                    rst.getString(4),
                    rst.getString(5)
                );
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

                int current_user = 0;
                rst.previous();
                while (rst.next() && current_user < rst.getInt(1)) {
                    tp.addTaggedUser(
                        new UserInfo(
                            rst.getLong(6),
                            rst.getString(7),
                            rst.getString(8)
                        )
                    );
                    current_user += 1;
                }
                results.add(tp);
                rst.previous();
                current_photo += 1;
            }
            rst.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

//   "ABS(U1.year_of_birth - U2.year_of_birth) < 10 " +

ResultSet rst = stmt.executeQuery(
    "SELECT "+
    "T.FIRST, T.SECOND, "+
    "U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, "+
    "U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH, "+
    "P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME "+
    "FROM (SELECT DISTINCT LEAST(FIRST, SECOND) AS FIRST, GREATEST(FIRST, SECOND) AS SECOND "+
    "FROM (SELECT U1.USER_ID AS FIRST, U2.USER_ID AS SECOND, COUNT(T1.TAG_PHOTO_ID) "+
    "FROM " + UsersTable + " U1, " + UsersTable + " U2, " + TagsTable + "  T1, " + TagsTable + "  T2 "+
        "WHERE "+
        "U1.USER_ID = T1.TAG_SUBJECT_ID AND U2.USER_ID = T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND "+
        "U1.GENDER = U2.GENDER AND U1.USER_ID <> U2.USER_ID AND "+
        
        "ABS(U1.year_of_birth - U2.year_of_birth) <= " + yearDiff

            "AND NOT EXISTS "+
            "(SELECT * FROM " + FriendsTable + " F WHERE "+
            
            "(F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID) OR (F.USER1_ID = U2.USER_ID AND F.USER2_ID = U1.USER_ID)) " +

            "GROUP BY U1.USER_ID, U2.USER_ID ORDER BY COUNT(T1.TAG_PHOTO_ID) DESC, U1.USER_ID ASC, U2.USER_ID ASC) WHERE ROWNUM <= " + num + ") T, " + UsersTable + " U1, " + UsersTable + " U2, " + TagsTable + " T1, " + TagsTable + " T2, " + PhotosTable + " P, " + AlbumsTable + " A " +
                      "WHERE T.FIRST = U1.USER_ID AND T.SECOND = U2.USER_ID AND T.FIRST = T1.TAG_SUBJECT_ID AND T.SECOND = T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_PHOTO_ID = P.PHOTO_ID AND P.ALBUM_ID = A.ALBUM_ID ORDER BY T.FIRST ASC, T.SECOND ASC"
                      );


        Long user1ID = null;
        Long user2ID = null;
        MatchPair pair = null;
        while (rst.next()) {
            if (user1ID == null || (!user1ID.equals(rst.getLong(1)) && !user2ID.equals(rst.getLong(2)))) {
                if (pair != null) {
                    results.add(pair);
                }
                user1ID = rst.getLong(1);
                user2ID = rst.getLong(2);
                pair = new MatchPair(new UserInfo(user1ID, rst.getString(3), rst.getString(4)),
                        rst.getInt(5), new UserInfo(user2ID, rst.getString(6), rst.getString(7)), rst.getInt(8));
            }

            pair.addSharedPhoto(new PhotoInfo(rst.getInt(9), rst.getInt(10),
                    rst.getString(11), rst.getString(12)));
        }
        if (pair != null) {
            results.add(pair);
        }

        rst.next();
        user1ID = rst.getLong(1);
        user2ID = rst.getLong(2);
        pair = new MatchPair(new UserInfo(user1ID, rst.getString(3), rst.getString(4)), 
                            rst.getInt(5), new UserInfo(user2ID, rst.getString(6), rst.getString(7)), rst.getInt(8));
        pair.addSharedPhoto(new PhotoInfo(rst.getInt(9), rst.getInt(10),
                        rst.getString(11), rst.getString(12)));

        while(rst.next()) {
            if  (user1ID.equals(rst.getLong(1))) {
                pair.addSharedPhoto(new PhotoInfo(rst.getInt(9), rst.getInt(10), rst.getString(11), rst.getString(12)));
            }
            else {
                user1ID = rst.getLong(1);
                user2ID = rst.getLong(2);
                pair = new MatchPair(new UserInfo(user1ID, rst.getString(3), rst.getString(4)), 
                            rst.getInt(5), new UserInfo(user2ID, rst.getString(6), rst.getString(7)), rst.getInt(8));
                pair.addSharedPhoto(new PhotoInfo(rst.getInt(9), rst.getInt(10),
                        rst.getString(11), rst.getString(12)));
            }
        }
        rst.close();
        stmt.close();





        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW friends AS " +
                "SELECT F1.user1_id, F1.user2_id " +
                "FROM " + FriendsTable + " F1 " + 
                "UNION "+
                "SELECT F2.user2_id, F2.user1_id "+
                "FROM " + FriendsTable + " F2");

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW mutual AS " +
                "SELECT F1.user1_id AS ID1, F2.user1_id AS ID2, F1.user2_id AS M " +
                "FROM friends F1, friends F2 " +
                "WHERE " +
                "F1.USER1_ID < F2.USER1_ID AND "+
                "F1.USER2_ID = F2.USER2_ID");

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW mutual_count AS " +
                "SELECT ID1, ID2, count " +
                "FROM (SELECT ID1, ID2, COUNT(M) AS count " +
                "FROM mutual " +
                "WHERE NOT EXISTS "+
                    "(SELECT * FROM " + FriendsTable + " F "+
                    "WHERE F.user1_id = ID1 AND F.user2_id = ID2) " +
                "GROUP BY ID1, ID2 " +
                "ORDER BY count DESC, ID1 ASC, ID2 ASC) " +
                "WHERE ROWNUM <= " + num);

            ResultSet rst = stmt.executeQuery(
                "SELECT ID1, ID2, M, F1, L1, F2, L2, F3, L3 "+
                "FROM " +
                "(SELECT "+
                "C.count, C.ID1 AS ID1, C.ID2 AS ID2, "+
                "F.M AS M, "+
                "U1.first_name AS F1, U1.last_name AS L1, "+
                "U2.first_name AS F2, U2.last_name AS L2, "+
                "U3.first_name AS F3, U3.last_name AS L3 " +
                "FROM "+
                "mutual_count C, "+
                "mutual F, " + 
                UsersTable + " U1, " + 
                UsersTable + " U2, " + 
                UsersTable + " U3 " +
                "WHERE "+
                "C.ID1 = F.ID1 AND "+
                "C.ID2 = F.ID2 AND " +
                "C.ID1 = U1.user_id AND " +
                "C.ID2 = U2.user_id AND " +
                "F.M = U3.user_id " +
                "ORDER BY C.count DESC, C.ID1 ASC, C.ID2 ASC, M ASC)" );

            
            rst.next();
            Long uid1 = rst.getLong(1);
            Long uid2 = rst.getLong(2);
            UsersPair p = new UsersPair(new UserInfo(uid1, rst.getString(4), rst.getString(5)), 
                                        new UserInfo(uid2, rst.getString(6), rst.getString(7)));
            p.addSharedFriend(new UserInfo(rst.getInt(3), rst.getString(8), rst.getString(9)));

            while (rst.next()) {
                if (uid1.equals(rst.getLong(1)) && uid2.equals(rst.getLong(2))) {
                    p.addSharedFriend(new UserInfo(rst.getInt(3), rst.getString(8), rst.getString(9))); 
                } 
                else {
                    results.add(p);
                    uid1 = rst.getLong(1);
                    uid2 = rst.getLong(2);
                    p = new UsersPair(new UserInfo(uid1, rst.getString(4), rst.getString(5)), new UserInfo(uid2, rst.getString(6), rst.getString(7)));
                    p.addSharedFriend(new UserInfo(rst.getInt(3), rst.getString(8), rst.getString(9))); 
                }
                
            }

            stmt.executeUpdate("DROP VIEW mutual_count");
            stmt.executeUpdate("DROP VIEW mutual");
            stmt.executeUpdate("DROP VIEW friends");
            rst.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            
            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT COUNT(*) AS count, C.state_name " +
                "FROM " + EventsTable + " E, "  + CitiesTable  + " C " +
                "WHERE C.city_id = E.event_city_id " +
                "GROUP BY C.state_name " +
                "ORDER BY count DESC, C.state_name ASC"
            );

            int max = 0;
            EventStateInfo info = null;
            while(rst.next()) {
                if(rst.isFirst()) {
                    max = rst.getInt(1);
                    info = new EventStateInfo(rst.getInt(1));
                    info.addState(rst.getString(2));
                } else if (rst.getInt(1) == max) {
                    info.addState(rst.getString(2));
                } else {
                    break;
                }
            }

            rst.close();
            stmt.close();
            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            
            ResultSet rst = stmt.executeQuery(
                "SELECT U.user_id, U.first_name, U.last_name " +
                "FROM " + FriendsTable + " F, " + UsersTable + " U " +
                "WHERE "+
                "(F.user1_id = " + userID + " AND U.user_id = F.user2_id) OR "+
                "(F.user2_id = " + userID + " AND U.user_id = F.user1_id) " +
                "ORDER BY U.year_of_birth, U.month_of_birth, U.day_of_birth, U.user_id DESC"
            );
            UserInfo oldest = null;
            UserInfo youngest = null;
            while(rst.next()) {
                if(rst.isFirst()) {
                    oldest = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                }
                if(rst.isLast()) {
                    youngest = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                }
            }

            rst.close();
            stmt.close();
            return new AgeInfo(oldest, youngest);

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                "SELECT "+
                "U1.user_id, U1.first_name, U1.last_name, "+
                "U2.user_id, U2.first_name, U2.last_name " +
                "FROM " + 
                UsersTable + " U1, " + UsersTable + " U2, " + 
                FriendsTable + " F," + 
                HometownCitiesTable + " H1, " + HometownCitiesTable + " H2 " +
                "WHERE " +
                "U1.last_name = U2.last_name AND " +
                "H1.hometown_city_id = H2.hometown_city_id AND " +
                "U1.user_id = H1.user_id AND " +
                "U2.user_id = H2.user_id AND " +
                "U1.user_id < U2.user_id AND " +
                "U1.user_id = F.user1_id AND " + 
                "U2.user_id = F.user2_id AND " +
                "ABS(U1.year_of_birth - U2.year_of_birth) < 10 " +
                "ORDER BY U1.user_id ASC, U2.user_id ASC"
            );

            while(rst.next()){
                results.add(
                    new SiblingInfo(
                        new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)),
                        new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6))
                    )
                );
            }
            rst.close();
            stmt.close();
            

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
