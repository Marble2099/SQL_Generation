import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBConnector {
    private static final char[] numbers = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    public static void main(String[] args) throws IOException {
        String rootPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResource("")).getPath();
        String appConfigPath = rootPath + "generator.properties";

        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appConfigPath));

        //вручную подключаемся к шардам по очереди

        String url = appProps.getProperty("db_url");
        String user = appProps.getProperty("db_user");
        String password = appProps.getProperty("db_password");

        int count_registers = Integer.parseInt(appProps.getProperty("count_registers"));
        int min_turns = Integer.parseInt(appProps.getProperty("min_turns"));
        int max_turns = Integer.parseInt(appProps.getProperty("max_turns"));
        String uniq_id = appProps.getProperty("uniq_id");
        String list_of_registers = appProps.getProperty("list_of_registers");

        //Пишем имя клиента
        String ccname_client = "ООО \"Алхемакс Интернешнл\"";

        Logger lgr = Logger.getLogger(DBConnector.class.getName());
        FileWriter fwriter = new FileWriter(list_of_registers, true);
        BufferedReader buff = new BufferedReader(fwriter);

        String query_client = "INSERT ";
        String query_register = "INSERT ";
        String query_turn = "INSERT ";
        String query_docdata = "INSERT ";

        try (Connection con = DriverManager.getConnection(url, user, password);
             PreparedStatement pst_client = con.prepareStatement(query_client);
             PreparedStatement pst_register = con.prepareStatement(query_register);
             PreparedStatement pst_turn = con.prepareStatement(query_turn);
             PreparedStatement pst_docdata = con.prepareStatement(query_docdata)){

            //в цикле указываем сколько сгенерить регистров
            for (int i = 1; i <= count_registers; i++) {
                // Случайным образом генерим значения для выбранных полей и при помощи плейсхолдеров подставляем в insert запрос
                String rand1 = RandomStringUtils.random(15, numbers); //для objectid_client
                String rand2 = RandomStringUtils.random(15, numbers); //для ccregister_register
                String rand3 = RandomStringUtils.random(15, numbers); //для objectid_register

                String objectid_client = uniq_id + String.valueOf(rand1);
                String ccregister_register = uniq_id + String.valueOf(rand2);
                String objectid_register = uniq_id + String.valueOf(rand3);

                lgr.log(Level.INFO, ccregister_register);
                buff.write(ccregister_register + '\n');

                pst_client.setString(1, ccname_client);
                pst_client.setString(2, objectid_client);
                pst_client.executeUpdate();

                pst_register.setString(1, objectid_client);
                pst_register.setString(2, ccregister_register);
                pst_register.setString(3, ccname_client);
                pst_register.setString(4, objectid_register);
                pst_register.executeUpdate();

                int turns = (int)(Math.random() * (max_turns - min_turns) + min_turns);

                lgr.log(Level.INFO, String.valueOf(turns));

                //В цикле указываем сколько сгенерить проводок для счетов
                for (int j = 1; j <= turns; j++) {
                    String rand4 = RandomStringUtils.random(17, numbers); // для objectid_turn
                    String rand5 = RandomStringUtils.random(11, numbers); // для objectid_docdata
                    String rand6 = RandomStringUtils.random(14, numbers); // для objectid_docdata
                    String rand7 = RandomStringUtils.random(9, numbers); // для cceksid_turn

                    String objectid_turn = uniq_id + String.valueOf(rand4);
                    String cceksid_turn = uniq_id + String.valueOf(rand7);
                    String objectid_docdata = uniq_id + String.valueOf(rand5) + "|" + uniq_id + String.valueOf(rand6);

                    String[] list = {"01", "02", "06", "16", "17"};
                    Random r = new Random();
                    String typeoper = list[r.nextInt(list.length)];

                    pst_turn.setString(1, objectid_register);
                    pst_turn.setString(2, cceksid_turn);
                    pst_turn.setString(3, Integer.parseInt(typeoper));
                    pst_turn.setString(4, objectid_turn);
                    pst_turn.executeUpdate();

                    pst_docdata.setString(1, objectid_turn);
                    pst_docdata.setString(2, ccname_client);
                    pst_docdata.setString(3, objectid_docdata);
                    pst_docdata.executeUpdate();
                }
            }
        } catch (SQLException ex) {
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
        buff.close();
        fwriter.close();
    }
}
