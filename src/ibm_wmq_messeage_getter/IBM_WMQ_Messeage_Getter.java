package ibm_wmq_messeage_getter;

import com.ibm.jms.JMSTextMessage;
import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueReceiver;
import com.ibm.mq.jms.MQQueueSession;
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
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Date;

/**
 * Automatically check MQ then check DB and call API if it has permission
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
    private static MQQueueConnection MQconnection;
    private static MQQueueSession session;
    private static MQQueueReceiver receiver;
    private static Statement statement;
    private static Connection DBconnection;

    private static String HOST_NAME;
    private static int PORT_NUMBER;
    private static String QUEUE_MANAGER_NAME;
    private static String QUEUE_NAME;
    private static String LOGIN_USERNAME;
    private static String LOGIN_PASSWORD;
    private static String JDBC_CONNECT_STRING;
    private static String JDBC_DB_USER;
    private static String JDBC_DB_PASSWORD;
    private static int SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE;
    private static String API_URL;
    private static boolean isMQConnected = false;
    private static boolean isDBConnected = false;
    private static int try_number_not_found_new_mq_message = 0;

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

            createMQConnection();
            createDBConnection();

            while (true) {
                int processed_messages = readMessageFromQueue();
                if (processed_messages == 0) {
                    System.out.println("Not found new MQ Message in queue, Automatically recheck in " + SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE + " miliseconds.");
                    try_number_not_found_new_mq_message++;
                    if (try_number_not_found_new_mq_message == MAX_NUMBER_TRY_BEFORE_CLOSE_CONNECTION) {
                        closeDBConnection();
//                        closeMQConnection();
                    }
                    Thread.sleep(SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE);
                }
            }
//            closeMQConnection();
//            closeDBConnection();
        } catch (JMSException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can not read config file");
        }
    }

    /**
     * Make connection to IBM MQ
     *
     * @throws JMSException
     */
    public static void createMQConnection() throws JMSException {
        if (isMQConnected) {
            return;
        }
        System.out.println("Connecting to MQ");
        MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
        cf.setHostName(HOST_NAME);
        cf.setPort(PORT_NUMBER);
        cf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
        cf.setQueueManager(QUEUE_MANAGER_NAME);
        cf.setChannel("SYSTEM.DEF.SVRCONN");
        MQconnection = (MQQueueConnection) cf.createQueueConnection(LOGIN_USERNAME, LOGIN_PASSWORD);
        session = (MQQueueSession) MQconnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        MQQueue queue = (MQQueue) session.createQueue(QUEUE_NAME);
        receiver = (MQQueueReceiver) session.createReceiver((Queue) queue);
        MQconnection.start();
        isMQConnected = true;
        System.out.println("MQ connected");
    }

    /**
     * Close connection to IBM MQ
     *
     * @throws JMSException
     */
    public static void closeMQConnection() throws JMSException {
        if (!isMQConnected) {
            return;
        }
        receiver.close();
        session.close();
        MQconnection.close();
        isMQConnected = false;
        System.out.println("MQ connection is closed");
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
        HOST_NAME = prop.getProperty("mq_host_name");
        System.out.println("mq_host_name = " + HOST_NAME);
        try {
            PORT_NUMBER = Integer.parseInt(prop.getProperty("mq_port_number"));
            System.out.println("mq_port_number = " + PORT_NUMBER);
        } catch (NumberFormatException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Port number must be integer");
        }
        QUEUE_MANAGER_NAME = prop.getProperty("mq_queue_manager");
        System.out.println("mq_queue_manager = " + QUEUE_MANAGER_NAME);
        QUEUE_NAME = prop.getProperty("mq_queue_name");
        System.out.println("mq_queue_name = " + QUEUE_NAME);
        LOGIN_USERNAME = prop.getProperty("mq_os_login_user");
        System.out.println("mq_os_login_user = " + LOGIN_USERNAME);
        LOGIN_PASSWORD = prop.getProperty("mq_os_login_password");
        System.out.println("mq_os_login_password = " + LOGIN_PASSWORD);
        JDBC_CONNECT_STRING = prop.getProperty("jdbc_connect_string");
        System.out.println("jdbc_connect_string = " + JDBC_CONNECT_STRING);
        JDBC_DB_USER = prop.getProperty("jdbc_user");
        System.out.println("jdbc_user = " + JDBC_DB_USER);
        JDBC_DB_PASSWORD = prop.getProperty("jdbc_password");
        System.out.println("jdbc_password = " + JDBC_DB_PASSWORD);
        try {
            SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE = Integer.parseInt(prop.getProperty("sleep_miliseconds_if_not_found_any_new_mq_message"));
            System.out.println("sleep_miliseconds_if_not_found_any_new_file = " + SLEEP_MILISECOND_IF_NOT_FOUND_NEW_MQ_MESSAGE);
        } catch (NumberFormatException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Sleep time(miliseconds) must be integer");
        }
        API_URL = prop.getProperty("api_url");
        System.out.println("api_url = " + API_URL);
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

        JMSTextMessage receivedMessage = (JMSTextMessage) receiver.receive(500);
        if (receivedMessage != null) {
            // detact receivedMessage; receivedMessage must be in format: Inserted_time,SeqNo,content
            String[] parts = receivedMessage.getText().split(",");
            if (parts.length >= 3) {
                String mq_insert_date = parts[0];
                String seq_no = parts[1];
                String message_content = "";
                for (int i = 2; i < parts.length; i++) {
                    if ("".equals(message_content)) {
                        message_content = parts[i];
                    } else {
                        message_content += "," + parts[i];
                    }
                }

                // Marked got message to DB
                SimpleDateFormat format_mq_get_date_time = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss:SSS");
                String mq_get_date_time = format_mq_get_date_time.format(new Date());
                try {
                    statement.execute("INSERT INTO MQINSMSG(MQGETDATE,MQINSERTDATE,SEQNO,MQMSG) VALUES ('"
                            + mq_get_date_time + "','"
                            + mq_insert_date + "','"
                            + seq_no + "','"
                            + message_content
                            + "')");
                } catch (SQLException ex) {
                    Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
                    System.out.println("Can not marked read messeage to DB");
                }

                // Check conditions
                int error_flag = 0;
                String message_key = message_content.split(",")[0];
                message_key = message_key.replaceAll("\"", "");

                // condition 3
                error_flag = checkCondition(
                        error_flag,
                        ERROR_FLAG_CONDITION_3,
                        "select Permission from M_CLIENT where CLIENTKEY = 'GGGGGGGGGG' AND mailsyu = '000'",
                        mq_get_date_time,
                        mq_insert_date,
                        seq_no
                );

                // condition 4
                String message_key_cond_4 = (message_key.length() >= 8) ? message_key.substring(3,8) : message_key;
                error_flag = checkCondition(
                        error_flag,
                        ERROR_FLAG_CONDITION_4,
                        "select Permission from M_CLIENTSTORE where CLIENTKEY = 'GGGGGGGGGG' and STOREKEY = '" + message_key_cond_4 + "'",
                        mq_get_date_time,
                        mq_insert_date,
                        seq_no
                );

                // condition 5
                String message_key_cond_5 = (message_key.length() > 10) ? message_key.substring(message_key.length() - 10) : message_key;
                error_flag = checkCondition(
                        error_flag,
                        ERROR_FLAG_CONDITION_5,
                        "select Permission from M_KAIIN where Kaiinkey = '" + message_key_cond_5 + "'",
                        mq_get_date_time,
                        mq_insert_date,
                        seq_no
                );

                // Call API and update result to DB, 
                if (error_flag == 0) {
                    System.out.println("Call API");
                    makeAPICall();
                    
                    // If it's ok, insert Message to SENDMAILMSG
                    try {
                        statement.execute("INSERT INTO SENDMAILMSG(MQINSERTDATE,SEQNO,INSERTDATE) VALUES ('"
                                + mq_insert_date + "','"
                                + seq_no + "','"
                                + format_mq_get_date_time.format(new Date())
                                + "')");
                    } catch (SQLException ex) {
                        Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
                        System.out.println("Can not marked send messeage successfully to DB");
                    }
                }

                process_message++;
            }
        }

        return process_message;
    }

    private static int checkCondition(int current_error_flag, int error_value, String queryString, String mq_get_date_time, String mq_insert_date, String seq_no) {
        if (current_error_flag != 0) {
            return current_error_flag;
        }
        int new_error_flag = error_value;
        try {
            ResultSet rs = statement.executeQuery(queryString);
            while (rs.next()) {
                if ("1".equals(rs.getString("Permission"))) {
                    new_error_flag = 0;
                    break;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
        }
        updateSendFlagMessageStatus(new_error_flag, mq_get_date_time, mq_insert_date, seq_no);
        return new_error_flag;
    }

    private static boolean updateSendFlagMessageStatus(int error_flag, String mq_get_date_time, String mq_insert_date, String seq_no) {
        if (error_flag == 0) {
            return false;
        }
        try {
            
            return statement.execute("UPDATE MQINSMSG SET SENDCKSTATUS=" + error_flag + " WHERE "
                    + "MQGETDATE = '"+ mq_get_date_time + "' AND "
                    + "MQINSERTDATE = '" + mq_insert_date + "' AND "
                    + "SEQNO = " + seq_no);
        } catch (SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can not update SENDCKSTATUS messeage to DB");
        }
        return false;
    }

    private static void makeAPICall() {
        try {
            String url = API_URL;
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            
            //add reuqest header
            con.setRequestMethod("POST");
            
            String urlParameters = "sn=hihi&num=12345";
            
            // Send post request
            con.setDoOutput(true);
            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.writeBytes(urlParameters);
                wr.flush();
            }
            
            int responseCode = con.getResponseCode();
//            System.out.println("\nSending 'POST' request to URL : " + url);
//            System.out.println("Post parameters : " + urlParameters);
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
            
//            System.out.println(response.toString());
        } catch (MalformedURLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Getter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
