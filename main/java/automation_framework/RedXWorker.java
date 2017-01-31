package automation_framework;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxBinary;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;

class RedXWorker implements Callable{
    private WebDriver driver;
    private Integer longWait = 10;
    private String url = "https://crediblebh.com";

    RedXWorker(){
        ClassLoader loader = this.getClass().getClassLoader();
        InputStream in = loader.getResourceAsStream("config.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);
        }catch(IOException e){
            throw new RuntimeException("can't find the config.properties file, check again");
        }
        String firefoxBinaryPath;
        String os = System.getProperty("os.name").toLowerCase();
        if(os.contains("win")) {
            firefoxBinaryPath = properties.getProperty("firefox_binary_path_windows");
        }else if(os.contains("nux")) {
            firefoxBinaryPath = properties.getProperty("firefox_binary_path_linux");
        }else{
            System.out.println(os);
            throw new RuntimeException("your operating system is not supported");
        }
        FirefoxBinary bin = new FirefoxBinary(new File(firefoxBinaryPath));
        driver = new FirefoxDriver(bin,new FirefoxProfile());
        driver.get(url);
        WebElement loginButton = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("btnLogin")));
        WebElement usernameField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("UserName")));
        WebElement passwordField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("Password")));
        WebElement domainField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("DomainName")));
        try {
            usernameField.sendKeys(Robot.algyWhisperer.getEncryptedKeyByName("root_username",Robot.internalKey,Robot.keys));
            passwordField.sendKeys(Robot.algyWhisperer.getEncryptedKeyByName("root_password",Robot.internalKey,Robot.keys));
            domainField.sendKeys(Robot.algyWhisperer.getEncryptedKeyByName("domain",Robot.internalKey,Robot.keys));
        }catch(Exception e){
            e.printStackTrace();
        }
        loginButton.click();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
    }

	@Override
	public String call(){
		TreeMap<String,TreeMap<String,String>> newRedXData = new TreeMap<>();
		String filterButtonXpath = "/html/body/table/tbody/tr/td[2]/table/tbody/tr[4]/td/form/table/tbody/tr/td/table/tbody/tr/td[2]/input[6]";
        String serviceList = "/visit/list_cvs.asp?appr=0&p=444&e=4353";
        driver.get(url+ serviceList);
		WebElement mySelectElm = new WebDriverWait(driver, 4).until(ExpectedConditions.presenceOfElementLocated(By.name("appr")));
		Select mySelect= new Select(mySelectElm);
        try{
            Thread.sleep(30000);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        mySelect.selectByValue("9");
		WebElement filterButton = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.xpath(filterButtonXpath)));
		filterButton.submit();
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		WebElement tableElement = driver.findElement(By.xpath("/html/body/table/tbody/tr/td[2]/table/tbody/tr[6]/td/table[1]/tbody/tr/td/table/tbody"));
		String innerHtml = tableElement.getAttribute("innerHTML");
		driver.close();
		org.jsoup.nodes.Document doc2 = Jsoup.parse(innerHtml);
		Elements tableRows = doc2.getAllElements();
		Iterator<Element> it = tableRows.iterator();
		while(it.hasNext()){
			Element element = it.next();
			if(element.attr("Title").equals("View Service Details")&& !element.className().equals("listbtn")){
				TreeMap<String,String> redXData = new TreeMap<>();
				redXData.put("service_id",element.text());
				Element reasonElement = it.next();
				redXData.put("red_x_reason",reasonElement.attr("Title"));
				Element clientElement = it.next();
				redXData.put("client_id",clientElement.attr("href").split("=")[1]);
				redXData.put("client_name",clientElement.text());
				Element staffElement = it.next();
				redXData.put("staff_id",staffElement.attr("href").split("=")[1]);
				redXData.put("staff_name",staffElement.text());
				newRedXData.put(redXData.get("service_id"),redXData);
			}
		}
        try {
            Data serviceReport = new Data(Robot.algyWhisperer.getEncryptedKeyByName("jkc_cheaphack_redx",Robot.internalKey,Robot.keys), "", "");
            Map<String,Map<String,String>> serviceData = serviceReport.getMapData();
            for(String serviceId : newRedXData.keySet()){
                Map<String,String> redXRecord = newRedXData.get(serviceId);
                Map<String,String> serviceDetails = serviceData.get(serviceId);
                try {
                    redXRecord.put("service_date", serviceDetails.get("service_date"));
                    redXRecord.put("service_type",serviceDetails.get("visittype"));
                    redXRecord.put("team",serviceDetails.get("team_name"));
                    redXRecord.put("currently_redx","1");
                }catch(NullPointerException e){
                    System.out.println(serviceId);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
		try {
			Robot.access.addRedXRecords(newRedXData);
		}catch(SQLException e){
			e.printStackTrace();
		}
        return new Date().toString();
	}
}