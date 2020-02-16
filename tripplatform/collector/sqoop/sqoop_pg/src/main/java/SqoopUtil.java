import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.validation.Status;

public class SqoopUtil {

	public static final String SQOOP_URL="http://master:12000/sqoop/";
	public static SqoopClient client=new SqoopClient(SQOOP_URL);
	
	//创建一个link来连接linux上的mysql
	public static void createLinkFrompostgreSQL() {
		MLink mysqlLink = client.createLink("generic-jdbc-connector");
		mysqlLink.setName("from_postgreSQL_link");
		mysqlLink.setCreationUser("root");
		
		MLinkConfig linkConfig = mysqlLink.getConnectorLinkConfig();
		linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:postgresql://192.168.6.187:5432/WscHMS");
		linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("org.postgresql.Driver");
		linkConfig.getStringInput("linkConfig.username").setValue("postgres");
		linkConfig.getStringInput("linkConfig.password").setValue("123456");
		linkConfig.getStringInput("dialect.identifierEnclose").setValue("`");

		List<MConfig> configs = linkConfig.getConfigs();
		describeConfigs(configs);
		deleteLink("from_postgreSQL_link");
		Status status = client.saveLink(mysqlLink);
		if (status.canProceed()) {
			System.out.println("创建link成功");
		}else {
			System.out.println("创建link失败"+status.toString());
		}
	}

	public static void createHdfsLink() {
		MLink hdfsLink = client.createLink("hdfs-connector");

		MLinkConfig linkConfig = hdfsLink.getConnectorLinkConfig();
		linkConfig.getStringInput("linkConfig.uri").setValue("hdfs://master:9000");
		//hadoop的配置文件路径
		linkConfig.getStringInput("linkConfig.confDir").setValue("/opt/SoftWare/hadoop-2.7.3/etc/hadoop");
//    linkConfig.getMapInput("linkConfig.configOverrides").setValue()
		hdfsLink.setName("to_hdfs");
		deleteLink("to_hdfs");
		List<MConfig> configs = linkConfig.getConfigs();
		describeConfigs(configs);
		Status status = client.saveLink(hdfsLink);
		if (status.canProceed()) {
			System.out.println("创建link成功");
		}else {
			System.out.println("创建link失败"+status.toString());
		}
	}

	public static void deleteLink(String name){

		try {
			client.deleteLink(name);

		}catch (Exception e){
			e.printStackTrace();
		}

	}
	
	public static void createJobpostgreSQLToHdfs() {
		MJob job=client.createJob("from_postgreSQL_link", "to_hdfs");
		job.setName("postgreSQL_to_hdfs");
		MFromConfig fromJobConfig = job.getFromJobConfig();
		MToConfig toJobConfig = job.getToJobConfig();
		System.out.println("打印from job的配置参数");
		describeConfigs(fromJobConfig.getConfigs());
		fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("wsc");
		fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("wsc.tb_company");
		fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("company_id");
		System.out.println("打印to job的配置参数");
		describeConfigs(toJobConfig.getConfigs());
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/sqoop/btrip_pg");
		toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE");
		toJobConfig.getBooleanInput("toJobConfig.appendMode").setValue(true);
		
		Status status = client.saveJob(job);
		if (status.canProceed()) {
			System.out.println("创建job成功");
		}else {
			System.out.println("job创建失败"+status.toString());
		}

		
	}
	//打印配置项
	public static void describeConfigs(List<MConfig> configs) {
		for (MConfig mConfig : configs) {
			List<MInput<?>> inputs = mConfig.getInputs();
			for (MInput<?> mInput : inputs) {
				System.out.println(mInput);
			}
		}
	}
	
	public static void startJob(String jobName) {
		MSubmission submission = client.startJob(jobName);
		while (submission.getStatus().isRunning()) {
			System.out.println("程序运行中请稍候");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		if (submission.getStatus().isFailure()) {
			System.out.println("job运行失败");
		}else {
			System.out.println("job运行成功");

		}
	}
	
	public static void main(String[] args) {
//		createLinkFrompostgreSQL();
//		createJobMysqlToHdfs();
//		startJob("javaAPI_mysql_to_hdfs");
//		createHdfsLink();
//		createJobpostgreSQLToHdfs();

//		startJob("postgreSQL_to_hdfs");

	}
}













