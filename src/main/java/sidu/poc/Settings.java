package sidu.poc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Settings {
    private static final Properties CONFIG;

    static {
        CONFIG = new Properties();
        try (InputStream input = Settings.class.getClassLoader().getResourceAsStream("config.properties")) {
            CONFIG.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties getConfig() {
        return CONFIG;
    }
}
