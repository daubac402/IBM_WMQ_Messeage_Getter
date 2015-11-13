package ibm_wmq_messeage_getter;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import java.util.Date;
import org.apache.commons.codec.binary.Base64;

/**
 * Automatically check DB then check DB and call API if it has permission
 *
 * @author theanh@ilovex.co.jp
 */
public class IBM_WMQ_Messeage_Getter {

    /**
     * ****************************************************************************
     */
    // CONSTANT ZONE
    public static String CONFIG_FILE = "./main_config.ini";
    public static final int MAX_NUMBER_TRY_BEFORE_CLOSE_CONNECTION = 3;
    public static final int ERROR_FLAG_CONDITION_3 = 3;
    public static final int ERROR_FLAG_CONDITION_4 = 4;
    public static final int ERROR_FLAG_CONDITION_5 = 5;
    /**
     * ****************************************************************************
     */
    // GLOBAL VARIABLE ZONE
    private static Statement statement;
    private static Connection DBconnection;

    private static int TOTAL_CLIENT;
    private static int CLIENT_NO;
    private static String JDBC_CONNECT_STRING;
    private static String JDBC_DB_USER;
    private static String JDBC_DB_PASSWORD;
    private static String API_URL;
    private static String API_USER;
    private static String API_PASSWORD;
    private static String API_MAIL_TEMPLATE_NAME;
    private static int SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE;
    private static boolean IS_VERBOSE_MODE = false;
    private static boolean isDBConnected = false;
    private static int try_number_not_found_new_mq_message = 0;
    private static long startTime;

    /**
     * main function
     *
     * @param args
     * @throws java.lang.InterruptedException
     */
    @SuppressWarnings("SleepWhileInLoop")
    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0) {
            CONFIG_FILE = args[0];
        } else {
            System.err.println("Wrong command usage, Please use: java -jar \"main program jar URL\" \"main config URL\"");
            return;
        }

        try {
            loadConfiguration();

            createDBConnection();

            while (true) {
                int processed_messages = readMessageFromQueue();
                if (processed_messages == 0) {
                    System.out.println("Not found new Message in queue, Automatically recheck in " + SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE + " miliseconds.");
                    try_number_not_found_new_mq_message++;
                    if (try_number_not_found_new_mq_message == MAX_NUMBER_TRY_BEFORE_CLOSE_CONNECTION) {
                        closeDBConnection();
                    }
                    Thread.sleep(SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE);
                }
            }
//            closeDBConnection();
        } catch (JMSException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can not read config file");
        }
    }

    /**
     * Make connection to database
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void createDBConnection() throws ClassNotFoundException, SQLException {
        if (isDBConnected) {
            return;
        }
        System.out.println("Connecting to DB");
//        Class.forName("oracle.jdbc.OracleDriver");
        DBconnection = DriverManager.getConnection(JDBC_CONNECT_STRING, JDBC_DB_USER, JDBC_DB_PASSWORD);
        System.out.println("DB connected");
        System.out.println("Auto commit: " + DBconnection.getAutoCommit());
        isDBConnected = true;
        statement = DBconnection.createStatement();
    }

    /**
     * Close connection to database
     *
     * @throws SQLException
     */
    public static void closeDBConnection() throws SQLException {
        if (!isDBConnected) {
            return;
        }
        isDBConnected = false;
        DBconnection.close();
        System.out.println("DB connection is closed");
    }

    private static void loadConfiguration() throws IOException {
        System.out.println("Start loading Configuration");
        Properties prop = new Properties();
        InputStream inputConfigStream = new FileInputStream(CONFIG_FILE);
        prop.load(inputConfigStream);
        try {
            TOTAL_CLIENT = Integer.parseInt(prop.getProperty("total_client"));
            System.out.println("total_client = " + TOTAL_CLIENT);
        } catch (NumberFormatException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Total client must be integer");
        }
        try {
            CLIENT_NO = Integer.parseInt(prop.getProperty("client_no"));
            System.out.println("client_no = " + CLIENT_NO);
        } catch (NumberFormatException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Client No. must be integer");
        }
        JDBC_CONNECT_STRING = prop.getProperty("jdbc_connect_string");
        System.out.println("jdbc_connect_string = " + JDBC_CONNECT_STRING);
        JDBC_DB_USER = prop.getProperty("jdbc_user");
        System.out.println("jdbc_user = " + JDBC_DB_USER);
        JDBC_DB_PASSWORD = prop.getProperty("jdbc_password");
        System.out.println("jdbc_password = " + JDBC_DB_PASSWORD);
        API_URL = prop.getProperty("api_url");
        System.out.println("api_url = " + API_URL);
        API_USER = prop.getProperty("api_user");
        System.out.println("api_user = " + API_URL);
        API_PASSWORD = prop.getProperty("api_password");
        System.out.println("api_password = " + API_PASSWORD);
        API_MAIL_TEMPLATE_NAME = prop.getProperty("api_mail_template_name");
        System.out.println("api_mail_template_name = " + API_MAIL_TEMPLATE_NAME);
        try {
            SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE = Integer.parseInt(prop.getProperty("sleep_miliseconds_if_not_found_any_new_mq_message"));
            System.out.println("sleep_miliseconds_if_not_found_any_new_file = " + SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE);
        } catch (NumberFormatException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Sleep time(miliseconds) must be integer");
        }
        IS_VERBOSE_MODE = ("1".equals(prop.getProperty("is_verbose_mode")));
        System.out.println("is_verbose_mode = " + IS_VERBOSE_MODE);
        System.out.println("Done loading Configuration");
    }

    private static int readMessageFromQueue() throws JMSException {
        int process_message = 0;

        try {
            // try to reconnect DB
            createDBConnection();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can not connect to DB.");
        }

        startTime = System.nanoTime();
        MyMessage receivedMessage = getMessage();
        if (IS_VERBOSE_MODE) {
            System.out.println("Get Message from MQ cost: " + (System.nanoTime() - startTime));
        }
        if (receivedMessage != null) {

            // Marked got message to DB
            SimpleDateFormat format_mq_get_date_time = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss:SSS");
            String mq_get_date_time = format_mq_get_date_time.format(new Date());

            // Check conditions
            int error_flag = 0;
            String message_key = receivedMessage.getKaiInKey();

            // condition 3
            error_flag = checkCondition(
                    error_flag,
                    ERROR_FLAG_CONDITION_3,
                    "select Permission from M_CLIENT where CLIENTKEY = 'GGGGGGGGGG' AND mailsyu = '000'",
                    receivedMessage
            );

            // condition 4
            String message_key_cond_4 = (message_key.length() >= 8) ? message_key.substring(3, 8) : message_key;
            error_flag = checkCondition(
                    error_flag,
                    ERROR_FLAG_CONDITION_4,
                    "select Permission from M_CLIENTSTORE where CLIENTKEY = 'GGGGGGGGGG' and STOREKEY = '" + message_key_cond_4 + "'",
                    receivedMessage
            );

            // condition 5
            String message_key_cond_5 = (message_key.length() > 10) ? message_key.substring(message_key.length() - 10) : message_key;
            error_flag = checkCondition(
                    error_flag,
                    ERROR_FLAG_CONDITION_5,
                    "select Permission from M_KAIIN where Kaiinkey = '" + message_key_cond_5 + "'",
                    receivedMessage
            );

            // Call API and update result to DB, 
            if (error_flag == 0) {
                if (IS_VERBOSE_MODE) {
                    System.out.println("Call API");
                }
                startTime = System.nanoTime();
                callSendMailAPI();
                if (IS_VERBOSE_MODE) {
                    System.out.println("Call API cost: " + (System.nanoTime() - startTime));
                }
                
                // If it's ok, update SENDSTATUS Message to 999
                String update_complete_query_string = "UPDATE S_MSGFILE SET SENDSTATUS = '999' WHERE FOLDERNAME = '" + receivedMessage.getFolderName() + "' AND SEQNO = " + receivedMessage.getSeqNo();
                try {
                    startTime = System.nanoTime();
                    statement.execute(update_complete_query_string);
                    if (IS_VERBOSE_MODE) {
                        System.out.println("UPDATE S_MSGFILE to DB cost: " + (System.nanoTime() - startTime));
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
                    System.out.println("Can not marked complete messeage successfully to DB");
                    System.out.println(update_complete_query_string);
                }
                

                // If it's ok, insert Message to SENDMAILMSG
                String sendMail_query_string = "INSERT INTO S_SENDMAILMSG(FOLDERNAME,SEQNO,MAILSENDDATE) VALUES ('"
                            + receivedMessage.getInsertDate() + "',"
                            + receivedMessage.getSeqNo() + ",'"
                            + format_mq_get_date_time.format(new Date())
                            + "')";
                try {
                    startTime = System.nanoTime();
                    statement.execute(sendMail_query_string);
                    if (IS_VERBOSE_MODE) {
                        System.out.println("INSERT INTO S_SENDMAILMSG to DB cost: " + (System.nanoTime() - startTime));
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
                    System.out.println("Can not marked send messeage successfully to DB");
                    System.out.println(sendMail_query_string);
                }
            }
            process_message++;
        }

        return process_message;
    }

    private static int checkCondition(int current_error_flag, int error_value, String queryString, MyMessage message) {
        if (current_error_flag != 0) {
            return current_error_flag;
        }
        int new_error_flag = error_value;
        try {
            startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery(queryString);
            if (IS_VERBOSE_MODE) {
                System.out.println("Check condition " + error_value + " from DB cost: " + (System.nanoTime() - startTime));
            }
            while (rs.next()) {
                if ("1".equals(rs.getString("Permission"))) {
                    new_error_flag = 0;
                    break;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
        }
        updateSendFlagMessageStatus(new_error_flag, message);
        return new_error_flag;
    }

    private static boolean updateSendFlagMessageStatus(int error_flag, MyMessage message) {
        if (error_flag == 0) {
            return false;
        }
        String query_string = "UPDATE S_MSGFILE SET SENDSTATUS = '" + error_flag
                + "' WHERE FOLDERNAME = '" + message.getFolderName() + "' AND SEQNO = " + message.getSeqNo();
        try {
            startTime = System.nanoTime();
            boolean result = statement.execute(query_string);
            if (IS_VERBOSE_MODE) {
                System.out.println("UPDATE S_MSGFILE to DB cost: " + (System.nanoTime() - startTime));
            }
            return result;
        } catch (SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can not update SENDSTATUS messeage to DB");
            System.out.println(query_string);
        }
        return false;
    }

    private static void callSendMailAPI() {
        try {
            HttpURLConnection con = (HttpURLConnection) new URL(API_URL).openConnection();

            // for user and password authentication
            String authentication_account = API_USER + ":" + API_PASSWORD;
            con.setRequestProperty("Authorization", "Basic " + new String(new Base64().encode(authentication_account.getBytes())));

            //add reuqest header
            con.setRequestMethod("POST");
            String POST_string = "";
            POST_string += "cmd=send_mail&";
            POST_string += "name=" + API_MAIL_TEMPLATE_NAME + "&";
            POST_string += "from=" + "sugiyama@ilovex.co.jp" + "&";
            POST_string += "to=" + "theanh@ilovex.co.jp";

            // Send post request
            con.setDoOutput(true);
            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.writeBytes(POST_string);
                wr.flush();
            }

//            int responseCode = con.getResponseCode();
//            System.out.println("Response Code : " + responseCode);
            StringBuilder response;
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()))) {
                String inputLine;
                response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
            }

            if (IS_VERBOSE_MODE) {
                System.out.println(response.toString());
            }
        } catch (MalformedURLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.printf("Error - Malformed URL.");
        } catch (IOException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.printf("Error IO");
        }
    }

    private static MyMessage getMessage() {
        MyMessage msg = null;
        String queryString = "select * from S_MSGFILE where MOD(SEQNO," + TOTAL_CLIENT + ") = " + CLIENT_NO + " AND ROWNUM <= 1 and NVL(SENDSTATUS,0) = 0";
        try {
            ResultSet rs = statement.executeQuery(queryString);
            while (rs.next()) {
                msg = new MyMessage(
                        rs.getString("FOLDERNAME"),
                        rs.getInt("SEQNO"),
                        rs.getString("IFFILENAME"),
                        rs.getString("KAIINKEY"),
                        rs.getString("ITEM01"),
                        rs.getString("ITEM02"),
                        rs.getString("ITEM03"),
                        rs.getString("INSERTDATE"),
                        rs.getString("SENDSTATUS"),
                        rs.getString("UPDATEDATE"),
                        rs.getString("UPDATEPROGRAM"),
                        rs.getString("ARGUMENT")
                );
                break;
            }
        } catch (SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can not excute: " + queryString);
        }
        return msg;
    }
}
