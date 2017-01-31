package automation_framework;

import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Log {

    private Logger logger;

    public Log(){
        logger = Logger.getLogger("TableWorkerLogger");
        FileHandler fh;
        try {
            fh = new FileHandler("C:\\Users\\Jeff\\Documents\\workspace\\log_dump\\log.txt");
            logger.addHandler(fh);
            logger.setUseParentHandlers(false);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    synchronized public void log(String message){
        logger.info(message);
    }
}
