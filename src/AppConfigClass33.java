import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class AppConfigClass33 {

	private static final String CONFIG_FILE_PATH = "C:\\Users\\Lenovo\\eclipse-workspace\\DBengine\\src\\resources\\DBApp.config\\";
	
	public AppConfigClass33(){}
	
	public String getMetaDataFromConfig() throws DBAppException {
		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Read the current value of appCreated
			String appCreatedValue = properties.getProperty("metaData");
			return appCreatedValue;

		} catch (IOException e) {
			System.err.println("Error reading the configuration file: " + e.getMessage());
		}

		throw new DBAppException("couldnt find value from .config");
	}

	public String getAppCreatedFromConfig() throws DBAppException {
		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Read the current value of appCreated
			String appCreatedValue = properties.getProperty("appCreated");
			return appCreatedValue;

		} catch (IOException e) {
			System.err.println("Error reading the configuration file: " + e.getMessage());
		}

		throw new DBAppException("couldnt find value from .config");
	}

	public String getmMaxEntriesPerPageFromConfig() throws DBAppException {
		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Read the current value of appCreated
			String appCreatedValue = properties.getProperty("maxEntriesPerPage");
			return appCreatedValue;

		} catch (IOException e) {
			System.err.println("Error reading the configuration file: " + e.getMessage());
		}

		throw new DBAppException("couldnt find value from .config");
	}

	public String getAppPathFromConfig() throws DBAppException {
		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Read the current value of appCreated
			String appCreatedValue = properties.getProperty("appPath");
			return appCreatedValue;

		} catch (IOException e) {
			System.err.println("Error reading the configuration file: " + e.getMessage());
		}

		throw new DBAppException("couldnt find value from .config");
	}

	public void setMetaDataInConfig(String string) {

		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Update the value of appCreated
			properties.setProperty("metaData", string);

			// Save the updated properties to the file
			try (FileOutputStream fos = new FileOutputStream(CONFIG_FILE_PATH)) {
				properties.store(fos, null);
				System.out.println("metaData value updated successfully to " + string + ".");
			}

		} catch (IOException e) {
			System.err.println("Error reading or updating the configuration file: " + e.getMessage());
		}

	}

	public void setAppCreatedInConfig(String string) {

		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Update the value of appCreated
			properties.setProperty("appCreated", string);

			// Save the updated properties to the file
			try (FileOutputStream fos = new FileOutputStream(CONFIG_FILE_PATH)) {
				properties.store(fos, null);
				System.out.println("appCreated value updated successfully to " + string + ".");
			}

		} catch (IOException e) {
			System.err.println("Error reading or updating the configuration file: " + e.getMessage());
		}

	}
	
	public void setMaxEntriesPerPageInConfig(String string) {

		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Update the value of appCreated
			properties.setProperty("maxEntriesPerPage", string);

			// Save the updated properties to the file
			try (FileOutputStream fos = new FileOutputStream(CONFIG_FILE_PATH)) {
				properties.store(fos, null);
				System.out.println("maxEntriesPerPage value updated successfully to " + string + ".");
			}

		} catch (IOException e) {
			System.err.println("Error reading or updating the configuration file: " + e.getMessage());
		}

	}
	
	public void setAppPathPerPageInConfig(String string) {

		Properties properties = new Properties();

		try (FileInputStream fis = new FileInputStream(CONFIG_FILE_PATH)) {
			properties.load(fis);

			// Update the value of appCreated
			properties.setProperty("appPath", string);

			// Save the updated properties to the file
			try (FileOutputStream fos = new FileOutputStream(CONFIG_FILE_PATH)) {
				properties.store(fos, null);
				System.out.println("appPath value updated successfully to " + string + ".");
			}

		} catch (IOException e) {
			System.err.println("Error reading or updating the configuration file: " + e.getMessage());
		}

	}

	public static void main(String[] args) throws DBAppException {
		
		AppConfigClass33 acc=new AppConfigClass33();
		acc.setAppCreatedInConfig("bla");
		System.out.println(acc.getAppCreatedFromConfig());
	}
}