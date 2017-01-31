package pump;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Log {

    private Logger logger;

    public Log(){
        logger = LogManager.getLogger();
    }

    synchronized public void log(String message){
        logger.info(message);
    }
}
