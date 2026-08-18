
package ecommercepoc.orders_sqlserver_to_snowflake_0_1;

import routines.Numeric;
import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.MDM;
import routines.StringHandling;
import routines.Relational;
import routines.TalendDate;
import routines.Mathematical;
import routines.SQLike;
import routines.system.*;
import routines.system.api.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.math.BigDecimal;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.Comparator;

@SuppressWarnings("unused")

/**
 * Job: Orders_SQLServer_To_Snowflake Purpose: Extract orders from SQL Server to
 * Snowflake RAW<br>
 * Description: <br>
 * 
 * @author Gupta, Shivam
 * @version 8.0.1.20260724_0953-patch
 * @status
 */
public class Orders_SQLServer_To_Snowflake implements TalendJob {
	static {
		System.setProperty("TalendJob.log", "Orders_SQLServer_To_Snowflake.log");
	}

	private static org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager
			.getLogger(Orders_SQLServer_To_Snowflake.class);

	static {
		System.setProperty("talend.component.record.nullable.check", "true");
		String javaUtilLoggingConfigFile = System.getProperty("java.util.logging.config.file");
		if (javaUtilLoggingConfigFile == null) {
			setupDefaultJavaUtilLogging();
		}
	}

	/**
	 * This class replaces the default {@code System.err} stream used by Java Util
	 * Logging (JUL). You can use your own configuration through the
	 * {@code java.util.logging.config.file} system property, enabling you to
	 * specify an external logging configuration file for tailored logging setup.
	 */
	public static class StandardConsoleHandler extends java.util.logging.StreamHandler {
		public StandardConsoleHandler() {
			// Set System.out as default log output stream
			super(System.out, new java.util.logging.SimpleFormatter());
		}

		/**
		 * Publish a {@code LogRecord}. The logging request was made initially to a
		 * {@code Logger} object, which initialized the {@code LogRecord} and forwarded
		 * it here.
		 *
		 * @param record description of the log event. A null record is silently ignored
		 *               and is not published
		 */
		@Override
		public void publish(java.util.logging.LogRecord record) {
			super.publish(record);
			flush();
		}

		/**
		 * Override {@code StreamHandler.close} to do a flush but not to close the
		 * output stream. That is, we do <b>not</b> close {@code System.out}.
		 */
		@Override
		public void close() {
			flush();
		}
	}

	protected static void setupDefaultJavaUtilLogging() {
		java.util.logging.LogManager logManager = java.util.logging.LogManager.getLogManager();

		// Get the root logger
		java.util.logging.Logger rootLogger = logManager.getLogger("");

		// Remove existing handlers to set standard console handler only
		java.util.logging.Handler[] handlers = rootLogger.getHandlers();
		for (java.util.logging.Handler handler : handlers) {
			rootLogger.removeHandler(handler);
		}

		rootLogger.addHandler(new StandardConsoleHandler());
		rootLogger.setLevel(java.util.logging.Level.INFO);
	}

	protected static void logIgnoredError(String message, Throwable cause) {
		log.error(message, cause);

	}

	public final Object obj = new Object();

	// for transmiting parameters purpose
	private Object valueObject = null;

	public Object getValueObject() {
		return this.valueObject;
	}

	public void setValueObject(Object valueObject) {
		this.valueObject = valueObject;
	}

	private final static String defaultCharset = java.nio.charset.Charset.defaultCharset().name();

	private final static String utf8Charset = "UTF-8";

	// volatile is needed for OSGi: the thread that calls the blueprint destroy
	// method is not the same that sets the values
	public static volatile String taskExecutionId = null;
	public static volatile String jobExecutionId = null;
	public static volatile boolean sealCounters = false;

	public static final java.util.Set<Thread> threadList = java.util.Collections
			.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

	// contains type for every context property
	public class PropertiesWithType extends java.util.Properties {
		private static final long serialVersionUID = 1L;
		private java.util.Map<String, String> propertyTypes = new java.util.HashMap<>();

		public PropertiesWithType(java.util.Properties properties) {
			super(properties);
		}

		public PropertiesWithType() {
			super();
		}

		public void setContextType(String key, String type) {
			propertyTypes.put(key, type);
		}

		public String getContextType(String key) {
			return propertyTypes.get(key);
		}
	}

	// create and load default properties
	private java.util.Properties defaultProps = new java.util.Properties();

	// create application properties with default
	public class ContextProperties extends PropertiesWithType {

		private static final long serialVersionUID = 1L;

		public ContextProperties(java.util.Properties properties) {
			super(properties);
		}

		public ContextProperties() {
			super();
		}

		public void synchronizeContext() {

		}

		// if the stored or passed value is "<TALEND_NULL>" string, it mean null
		public String getStringValue(String key) {
			String origin_value = this.getProperty(key);
			if (NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY.equals(origin_value)) {
				return null;
			}
			return origin_value;
		}

	}

	protected ContextProperties context = new ContextProperties(); // will be instanciated by MS.

	public ContextProperties getContext() {
		return this.context;
	}

	protected java.util.Map<String, String> defaultProperties = new java.util.HashMap<String, String>();
	protected java.util.Map<String, String> additionalProperties = new java.util.HashMap<String, String>();

	public java.util.Map<String, String> getDefaultProperties() {
		return this.defaultProperties;
	}

	public java.util.Map<String, String> getAdditionalProperties() {
		return this.additionalProperties;
	}

	private final String jobVersion = "0.1";
	private final String jobName = "Orders_SQLServer_To_Snowflake";
	private final String projectName = "ECOMMERCEPOC";
	public Integer errorCode = null;
	private String currentComponent = "";
	public static boolean isStandaloneMS = Boolean.valueOf("false");

	private void s(final String component) {
		try {
			org.talend.metrics.DataReadTracker.setCurrentComponent(jobName, component);
		} catch (Exception | NoClassDefFoundError e) {
			// ignore
		}
	}

	private void mdc(final String subJobName, final String subJobPidPrefix) {
		mdcInfo.forEach(org.slf4j.MDC::put);
		org.slf4j.MDC.put("_subJobName", subJobName);
		org.slf4j.MDC.put("_subJobPid", subJobPidPrefix + subJobPidCounter.getAndIncrement());
	}

	private void sh(final String componentId) {
		ok_Hash.put(componentId, false);
		start_Hash.put(componentId, System.currentTimeMillis());
	}

	{
		s("none");
	}

	private String cLabel = null;

	private final java.util.Map<String, Object> globalMap = new java.util.HashMap<String, Object>();
	private final static java.util.Map<String, Object> junitGlobalMap = new java.util.HashMap<String, Object>();

	private final java.util.Map<String, Long> start_Hash = new java.util.HashMap<String, Long>();
	private final java.util.Map<String, Long> end_Hash = new java.util.HashMap<String, Long>();
	private final java.util.Map<String, Boolean> ok_Hash = new java.util.HashMap<String, Boolean>();
	public final java.util.List<String[]> globalBuffer = new java.util.ArrayList<String[]>();

	private final JobStructureCatcherUtils talendJobLog = new JobStructureCatcherUtils(jobName,
			"_MbDe4JpnEfGJXNU8GVdb3A", "0.1");
	private org.talend.job.audit.JobAuditLogger auditLogger_talendJobLog = null;

	private RunStat runStat = new RunStat(talendJobLog, System.getProperty("audit.interval"));

	// OSGi DataSource
	private final static String KEY_DB_DATASOURCES = "KEY_DB_DATASOURCES";

	private final static String KEY_DB_DATASOURCES_RAW = "KEY_DB_DATASOURCES_RAW";

	public void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {
		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		for (java.util.Map.Entry<String, javax.sql.DataSource> dataSourceEntry : dataSources.entrySet()) {
			talendDataSources.put(dataSourceEntry.getKey(),
					new routines.system.TalendDataSource(dataSourceEntry.getValue()));
		}
		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}

	public void setDataSourceReferences(List serviceReferences) throws Exception {

		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		java.util.Map<String, javax.sql.DataSource> dataSources = new java.util.HashMap<String, javax.sql.DataSource>();

		for (java.util.Map.Entry<String, javax.sql.DataSource> entry : BundleUtils
				.getServices(serviceReferences, javax.sql.DataSource.class).entrySet()) {
			dataSources.put(entry.getKey(), entry.getValue());
			talendDataSources.put(entry.getKey(), new routines.system.TalendDataSource(entry.getValue()));
		}

		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}

	LogCatcherUtils tLogCatcher_1 = new LogCatcherUtils();

	private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
	private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(new java.io.BufferedOutputStream(baos));

	public String getExceptionStackTrace() {
		if ("failure".equals(this.getStatus())) {
			errorMessagePS.flush();
			return baos.toString();
		}
		return null;
	}

	private Exception exception;

	public Exception getException() {
		if ("failure".equals(this.getStatus())) {
			return this.exception;
		}
		return null;
	}

	private class TalendException extends Exception {

		private static final long serialVersionUID = 1L;

		private java.util.Map<String, Object> globalMap = null;
		private Exception e = null;

		private String currentComponent = null;
		private String cLabel = null;

		private String virtualComponentName = null;

		public void setVirtualComponentName(String virtualComponentName) {
			this.virtualComponentName = virtualComponentName;
		}

		private TalendException(Exception e, String errorComponent, final java.util.Map<String, Object> globalMap) {
			this.currentComponent = errorComponent;
			this.globalMap = globalMap;
			this.e = e;
		}

		private TalendException(Exception e, String errorComponent, String errorComponentLabel,
				final java.util.Map<String, Object> globalMap) {
			this(e, errorComponent, globalMap);
			this.cLabel = errorComponentLabel;
		}

		public Exception getException() {
			return this.e;
		}

		public String getCurrentComponent() {
			return this.currentComponent;
		}

		public String getExceptionCauseMessage(Exception e) {
			Throwable cause = e;
			String message = null;
			int i = 10;
			while (null != cause && 0 < i--) {
				message = cause.getMessage();
				if (null == message) {
					cause = cause.getCause();
				} else {
					break;
				}
			}
			if (null == message) {
				message = e.getClass().getName();
			}
			return message;
		}

		@Override
		public void printStackTrace() {
			if (!(e instanceof TalendException || e instanceof TDieException)) {
				if (virtualComponentName != null && currentComponent.indexOf(virtualComponentName + "_") == 0) {
					globalMap.put(virtualComponentName + "_ERROR_MESSAGE", getExceptionCauseMessage(e));
				}
				globalMap.put(currentComponent + "_ERROR_MESSAGE", getExceptionCauseMessage(e));
				System.err.println("Exception in component " + currentComponent + " (" + jobName + ")");
			}
			if (!(e instanceof TDieException)) {
				if (e instanceof TalendException) {
					e.printStackTrace();
				} else {
					e.printStackTrace();
					e.printStackTrace(errorMessagePS);
				}
			}
			if (!(e instanceof TalendException)) {
				Orders_SQLServer_To_Snowflake.this.exception = e;
			}
			if (!(e instanceof TalendException)) {
				try {
					for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
						if (m.getName().compareTo(currentComponent + "_error") == 0) {
							m.invoke(Orders_SQLServer_To_Snowflake.this,
									new Object[] { e, currentComponent, globalMap });
							break;
						}
					}

					if (!(e instanceof TDieException)) {
						if (enableLogStash) {
							talendJobLog.addJobExceptionMessage(currentComponent, cLabel, null, e);
							talendJobLogProcess(globalMap);
						}
						tLogCatcher_1.addMessage("Java Exception", currentComponent, 6,
								e.getClass().getName() + ":" + e.getMessage(), errorCode == null ? 1 : errorCode);
						tLogCatcher_1Process(globalMap);
					}
				} catch (TalendException e) {
					// do nothing

				} catch (Exception e) {
					this.e.printStackTrace();
				}
			}
		}
	}

	public void tDBInput_1_error(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		status = "failure";

		tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
	}

	public void tMap_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap)
			throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		status = "failure";

		tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
	}

	public void tDBOutput_1_error(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		try {

			if (this.execStat) {
				runStat.updateStatOnConnection("OnComponentError1", 0, "error");
			}

			errorCode = null;
			tLogCatcher_1Process(globalMap);
			if (!"failure".equals(status)) {
				status = "end";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
	}

	public void tLogCatcher_1_error(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		status = "failure";

		tLogCatcher_1_onSubJobError(exception, errorComponent, globalMap);
	}

	public void tLogRow_2_error(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		status = "failure";

		tLogCatcher_1_onSubJobError(exception, errorComponent, globalMap);
	}

	public void tDie_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap)
			throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		status = "failure";

		tDie_1_onSubJobError(exception, errorComponent, globalMap);
	}

	public void tLogRow_1_error(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		status = "failure";

		tDBInput_1_onSubJobError(exception, errorComponent, globalMap);
	}

	public void talendJobLog_error(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		end_Hash.put(errorComponent, System.currentTimeMillis());

		status = "failure";

		talendJobLog_onSubJobError(exception, errorComponent, globalMap);
	}

	public void tDBInput_1_onSubJobError(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		resumeUtil.addLog("SYSTEM_LOG", "NODE:" + errorComponent, "", Thread.currentThread().getId() + "", "FATAL", "",
				exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception), "");

	}

	public void tLogCatcher_1_onSubJobError(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		resumeUtil.addLog("SYSTEM_LOG", "NODE:" + errorComponent, "", Thread.currentThread().getId() + "", "FATAL", "",
				exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception), "");

	}

	public void tDie_1_onSubJobError(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		resumeUtil.addLog("SYSTEM_LOG", "NODE:" + errorComponent, "", Thread.currentThread().getId() + "", "FATAL", "",
				exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception), "");

	}

	public void talendJobLog_onSubJobError(Exception exception, String errorComponent,
			final java.util.Map<String, Object> globalMap) throws TalendException {

		resumeUtil.addLog("SYSTEM_LOG", "NODE:" + errorComponent, "", Thread.currentThread().getId() + "", "FATAL", "",
				exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception), "");

	}

	public static class valid_ordersStruct implements routines.system.IPersistableRow<valid_ordersStruct> {
		final static byte[] commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];
		static byte[] commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];

		public Integer order_id;

		public Integer getOrder_id() {
			return this.order_id;
		}

		public Boolean order_idIsNullable() {
			return true;
		}

		public Boolean order_idIsKey() {
			return false;
		}

		public Integer order_idLength() {
			return 10;
		}

		public Integer order_idPrecision() {
			return 0;
		}

		public String order_idDefault() {

			return "";

		}

		public String order_idComment() {

			return "";

		}

		public String order_idPattern() {

			return "";

		}

		public String order_idOriginalDbColumnName() {

			return "order_id";

		}

		public Integer customer_id;

		public Integer getCustomer_id() {
			return this.customer_id;
		}

		public Boolean customer_idIsNullable() {
			return true;
		}

		public Boolean customer_idIsKey() {
			return false;
		}

		public Integer customer_idLength() {
			return 10;
		}

		public Integer customer_idPrecision() {
			return 0;
		}

		public String customer_idDefault() {

			return "";

		}

		public String customer_idComment() {

			return "";

		}

		public String customer_idPattern() {

			return "";

		}

		public String customer_idOriginalDbColumnName() {

			return "customer_id";

		}

		public Integer product_id;

		public Integer getProduct_id() {
			return this.product_id;
		}

		public Boolean product_idIsNullable() {
			return true;
		}

		public Boolean product_idIsKey() {
			return false;
		}

		public Integer product_idLength() {
			return 10;
		}

		public Integer product_idPrecision() {
			return 0;
		}

		public String product_idDefault() {

			return "";

		}

		public String product_idComment() {

			return "";

		}

		public String product_idPattern() {

			return "";

		}

		public String product_idOriginalDbColumnName() {

			return "product_id";

		}

		public Integer quantity;

		public Integer getQuantity() {
			return this.quantity;
		}

		public Boolean quantityIsNullable() {
			return true;
		}

		public Boolean quantityIsKey() {
			return false;
		}

		public Integer quantityLength() {
			return 10;
		}

		public Integer quantityPrecision() {
			return 0;
		}

		public String quantityDefault() {

			return "";

		}

		public String quantityComment() {

			return "";

		}

		public String quantityPattern() {

			return "";

		}

		public String quantityOriginalDbColumnName() {

			return "quantity";

		}

		public BigDecimal unit_price;

		public BigDecimal getUnit_price() {
			return this.unit_price;
		}

		public Boolean unit_priceIsNullable() {
			return true;
		}

		public Boolean unit_priceIsKey() {
			return false;
		}

		public Integer unit_priceLength() {
			return 10;
		}

		public Integer unit_pricePrecision() {
			return 2;
		}

		public String unit_priceDefault() {

			return "";

		}

		public String unit_priceComment() {

			return "";

		}

		public String unit_pricePattern() {

			return "";

		}

		public String unit_priceOriginalDbColumnName() {

			return "unit_price";

		}

		public BigDecimal total_amount;

		public BigDecimal getTotal_amount() {
			return this.total_amount;
		}

		public Boolean total_amountIsNullable() {
			return true;
		}

		public Boolean total_amountIsKey() {
			return false;
		}

		public Integer total_amountLength() {
			return 10;
		}

		public Integer total_amountPrecision() {
			return 2;
		}

		public String total_amountDefault() {

			return "";

		}

		public String total_amountComment() {

			return "";

		}

		public String total_amountPattern() {

			return "";

		}

		public String total_amountOriginalDbColumnName() {

			return "total_amount";

		}

		public String order_date;

		public String getOrder_date() {
			return this.order_date;
		}

		public Boolean order_dateIsNullable() {
			return true;
		}

		public Boolean order_dateIsKey() {
			return false;
		}

		public Integer order_dateLength() {
			return 20;
		}

		public Integer order_datePrecision() {
			return 0;
		}

		public String order_dateDefault() {

			return null;

		}

		public String order_dateComment() {

			return "";

		}

		public String order_datePattern() {

			return "";

		}

		public String order_dateOriginalDbColumnName() {

			return "order_date";

		}

		public String order_status;

		public String getOrder_status() {
			return this.order_status;
		}

		public Boolean order_statusIsNullable() {
			return true;
		}

		public Boolean order_statusIsKey() {
			return false;
		}

		public Integer order_statusLength() {
			return 20;
		}

		public Integer order_statusPrecision() {
			return 0;
		}

		public String order_statusDefault() {

			return null;

		}

		public String order_statusComment() {

			return "";

		}

		public String order_statusPattern() {

			return "";

		}

		public String order_statusOriginalDbColumnName() {

			return "order_status";

		}

		public String payment_method;

		public String getPayment_method() {
			return this.payment_method;
		}

		public Boolean payment_methodIsNullable() {
			return true;
		}

		public Boolean payment_methodIsKey() {
			return false;
		}

		public Integer payment_methodLength() {
			return 20;
		}

		public Integer payment_methodPrecision() {
			return 0;
		}

		public String payment_methodDefault() {

			return null;

		}

		public String payment_methodComment() {

			return "";

		}

		public String payment_methodPattern() {

			return "";

		}

		public String payment_methodOriginalDbColumnName() {

			return "payment_method";

		}

		public String region;

		public String getRegion() {
			return this.region;
		}

		public Boolean regionIsNullable() {
			return true;
		}

		public Boolean regionIsKey() {
			return false;
		}

		public Integer regionLength() {
			return 20;
		}

		public Integer regionPrecision() {
			return 0;
		}

		public String regionDefault() {

			return null;

		}

		public String regionComment() {

			return "";

		}

		public String regionPattern() {

			return "";

		}

		public String regionOriginalDbColumnName() {

			return "region";

		}

		private Integer readInteger(ObjectInputStream dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException {
			if (intNum == null) {
				dos.writeByte(-1);
			} else {
				dos.writeByte(0);
				dos.writeInt(intNum);
			}
		}

		private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (intNum == null) {
				marshaller.writeByte(-1);
			} else {
				marshaller.writeByte(0);
				marshaller.writeInt(intNum);
			}
		}

		private String readString(ObjectInputStream dis) throws IOException {
			String strReturn = null;
			int length = 0;
			length = dis.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				dis.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException {
			String strReturn = null;
			int length = 0;
			length = unmarshaller.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				unmarshaller.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private void writeString(String str, ObjectOutputStream dos) throws IOException {
			if (str == null) {
				dos.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				dos.writeInt(byteArray.length);
				dos.write(byteArray);
			}
		}

		private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (str == null) {
				marshaller.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				marshaller.writeInt(byteArray.length);
				marshaller.write(byteArray);
			}
		}

		public void readData(ObjectInputStream dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.order_id = readInteger(dis);

					this.customer_id = readInteger(dis);

					this.product_id = readInteger(dis);

					this.quantity = readInteger(dis);

					this.unit_price = (BigDecimal) dis.readObject();

					this.total_amount = (BigDecimal) dis.readObject();

					this.order_date = readString(dis);

					this.order_status = readString(dis);

					this.payment_method = readString(dis);

					this.region = readString(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				} catch (ClassNotFoundException eCNFE) {
					throw new RuntimeException(eCNFE);

				}

			}

		}

		public void readData(org.jboss.marshalling.Unmarshaller dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.order_id = readInteger(dis);

					this.customer_id = readInteger(dis);

					this.product_id = readInteger(dis);

					this.quantity = readInteger(dis);

					this.unit_price = (BigDecimal) dis.readObject();

					this.total_amount = (BigDecimal) dis.readObject();

					this.order_date = readString(dis);

					this.order_status = readString(dis);

					this.payment_method = readString(dis);

					this.region = readString(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				} catch (ClassNotFoundException eCNFE) {
					throw new RuntimeException(eCNFE);

				}

			}

		}

		public void writeData(ObjectOutputStream dos) {
			try {

				// Integer

				writeInteger(this.order_id, dos);

				// Integer

				writeInteger(this.customer_id, dos);

				// Integer

				writeInteger(this.product_id, dos);

				// Integer

				writeInteger(this.quantity, dos);

				// BigDecimal

				dos.writeObject(this.unit_price);

				// BigDecimal

				dos.writeObject(this.total_amount);

				// String

				writeString(this.order_date, dos);

				// String

				writeString(this.order_status, dos);

				// String

				writeString(this.payment_method, dos);

				// String

				writeString(this.region, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public void writeData(org.jboss.marshalling.Marshaller dos) {
			try {

				// Integer

				writeInteger(this.order_id, dos);

				// Integer

				writeInteger(this.customer_id, dos);

				// Integer

				writeInteger(this.product_id, dos);

				// Integer

				writeInteger(this.quantity, dos);

				// BigDecimal

				dos.clearInstanceCache();
				dos.writeObject(this.unit_price);

				// BigDecimal

				dos.clearInstanceCache();
				dos.writeObject(this.total_amount);

				// String

				writeString(this.order_date, dos);

				// String

				writeString(this.order_status, dos);

				// String

				writeString(this.payment_method, dos);

				// String

				writeString(this.region, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");
			sb.append("order_id=" + String.valueOf(order_id));
			sb.append(",customer_id=" + String.valueOf(customer_id));
			sb.append(",product_id=" + String.valueOf(product_id));
			sb.append(",quantity=" + String.valueOf(quantity));
			sb.append(",unit_price=" + String.valueOf(unit_price));
			sb.append(",total_amount=" + String.valueOf(total_amount));
			sb.append(",order_date=" + order_date);
			sb.append(",order_status=" + order_status);
			sb.append(",payment_method=" + payment_method);
			sb.append(",region=" + region);
			sb.append("]");

			return sb.toString();
		}

		public String toLogString() {
			StringBuilder sb = new StringBuilder();

			if (order_id == null) {
				sb.append("<null>");
			} else {
				sb.append(order_id);
			}

			sb.append("|");

			if (customer_id == null) {
				sb.append("<null>");
			} else {
				sb.append(customer_id);
			}

			sb.append("|");

			if (product_id == null) {
				sb.append("<null>");
			} else {
				sb.append(product_id);
			}

			sb.append("|");

			if (quantity == null) {
				sb.append("<null>");
			} else {
				sb.append(quantity);
			}

			sb.append("|");

			if (unit_price == null) {
				sb.append("<null>");
			} else {
				sb.append(unit_price);
			}

			sb.append("|");

			if (total_amount == null) {
				sb.append("<null>");
			} else {
				sb.append(total_amount);
			}

			sb.append("|");

			if (order_date == null) {
				sb.append("<null>");
			} else {
				sb.append(order_date);
			}

			sb.append("|");

			if (order_status == null) {
				sb.append("<null>");
			} else {
				sb.append(order_status);
			}

			sb.append("|");

			if (payment_method == null) {
				sb.append("<null>");
			} else {
				sb.append(payment_method);
			}

			sb.append("|");

			if (region == null) {
				sb.append("<null>");
			} else {
				sb.append(region);
			}

			sb.append("|");

			return sb.toString();
		}

		/**
		 * Compare keys
		 */
		public int compareTo(valid_ordersStruct other) {

			int returnValue = -1;

			return returnValue;
		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(), object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class rejected_ordersStruct implements routines.system.IPersistableRow<rejected_ordersStruct> {
		final static byte[] commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];
		static byte[] commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];

		public Integer order_id;

		public Integer getOrder_id() {
			return this.order_id;
		}

		public Boolean order_idIsNullable() {
			return true;
		}

		public Boolean order_idIsKey() {
			return false;
		}

		public Integer order_idLength() {
			return 10;
		}

		public Integer order_idPrecision() {
			return 0;
		}

		public String order_idDefault() {

			return "";

		}

		public String order_idComment() {

			return "";

		}

		public String order_idPattern() {

			return "";

		}

		public String order_idOriginalDbColumnName() {

			return "order_id";

		}

		public Integer customer_id;

		public Integer getCustomer_id() {
			return this.customer_id;
		}

		public Boolean customer_idIsNullable() {
			return true;
		}

		public Boolean customer_idIsKey() {
			return false;
		}

		public Integer customer_idLength() {
			return 10;
		}

		public Integer customer_idPrecision() {
			return 0;
		}

		public String customer_idDefault() {

			return "";

		}

		public String customer_idComment() {

			return "";

		}

		public String customer_idPattern() {

			return "";

		}

		public String customer_idOriginalDbColumnName() {

			return "customer_id";

		}

		public Integer product_id;

		public Integer getProduct_id() {
			return this.product_id;
		}

		public Boolean product_idIsNullable() {
			return true;
		}

		public Boolean product_idIsKey() {
			return false;
		}

		public Integer product_idLength() {
			return 10;
		}

		public Integer product_idPrecision() {
			return 0;
		}

		public String product_idDefault() {

			return "";

		}

		public String product_idComment() {

			return "";

		}

		public String product_idPattern() {

			return "";

		}

		public String product_idOriginalDbColumnName() {

			return "product_id";

		}

		public Integer quantity;

		public Integer getQuantity() {
			return this.quantity;
		}

		public Boolean quantityIsNullable() {
			return true;
		}

		public Boolean quantityIsKey() {
			return false;
		}

		public Integer quantityLength() {
			return 10;
		}

		public Integer quantityPrecision() {
			return 0;
		}

		public String quantityDefault() {

			return "";

		}

		public String quantityComment() {

			return "";

		}

		public String quantityPattern() {

			return "";

		}

		public String quantityOriginalDbColumnName() {

			return "quantity";

		}

		public BigDecimal unit_price;

		public BigDecimal getUnit_price() {
			return this.unit_price;
		}

		public Boolean unit_priceIsNullable() {
			return true;
		}

		public Boolean unit_priceIsKey() {
			return false;
		}

		public Integer unit_priceLength() {
			return 10;
		}

		public Integer unit_pricePrecision() {
			return 2;
		}

		public String unit_priceDefault() {

			return "";

		}

		public String unit_priceComment() {

			return "";

		}

		public String unit_pricePattern() {

			return "";

		}

		public String unit_priceOriginalDbColumnName() {

			return "unit_price";

		}

		public BigDecimal total_amount;

		public BigDecimal getTotal_amount() {
			return this.total_amount;
		}

		public Boolean total_amountIsNullable() {
			return true;
		}

		public Boolean total_amountIsKey() {
			return false;
		}

		public Integer total_amountLength() {
			return 10;
		}

		public Integer total_amountPrecision() {
			return 2;
		}

		public String total_amountDefault() {

			return "";

		}

		public String total_amountComment() {

			return "";

		}

		public String total_amountPattern() {

			return "";

		}

		public String total_amountOriginalDbColumnName() {

			return "total_amount";

		}

		public String order_date;

		public String getOrder_date() {
			return this.order_date;
		}

		public Boolean order_dateIsNullable() {
			return true;
		}

		public Boolean order_dateIsKey() {
			return false;
		}

		public Integer order_dateLength() {
			return 20;
		}

		public Integer order_datePrecision() {
			return 0;
		}

		public String order_dateDefault() {

			return null;

		}

		public String order_dateComment() {

			return "";

		}

		public String order_datePattern() {

			return "";

		}

		public String order_dateOriginalDbColumnName() {

			return "order_date";

		}

		public String order_status;

		public String getOrder_status() {
			return this.order_status;
		}

		public Boolean order_statusIsNullable() {
			return true;
		}

		public Boolean order_statusIsKey() {
			return false;
		}

		public Integer order_statusLength() {
			return 20;
		}

		public Integer order_statusPrecision() {
			return 0;
		}

		public String order_statusDefault() {

			return null;

		}

		public String order_statusComment() {

			return "";

		}

		public String order_statusPattern() {

			return "";

		}

		public String order_statusOriginalDbColumnName() {

			return "order_status";

		}

		public String payment_method;

		public String getPayment_method() {
			return this.payment_method;
		}

		public Boolean payment_methodIsNullable() {
			return true;
		}

		public Boolean payment_methodIsKey() {
			return false;
		}

		public Integer payment_methodLength() {
			return 20;
		}

		public Integer payment_methodPrecision() {
			return 0;
		}

		public String payment_methodDefault() {

			return null;

		}

		public String payment_methodComment() {

			return "";

		}

		public String payment_methodPattern() {

			return "";

		}

		public String payment_methodOriginalDbColumnName() {

			return "payment_method";

		}

		public String region;

		public String getRegion() {
			return this.region;
		}

		public Boolean regionIsNullable() {
			return true;
		}

		public Boolean regionIsKey() {
			return false;
		}

		public Integer regionLength() {
			return 20;
		}

		public Integer regionPrecision() {
			return 0;
		}

		public String regionDefault() {

			return null;

		}

		public String regionComment() {

			return "";

		}

		public String regionPattern() {

			return "";

		}

		public String regionOriginalDbColumnName() {

			return "region";

		}

		private Integer readInteger(ObjectInputStream dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException {
			if (intNum == null) {
				dos.writeByte(-1);
			} else {
				dos.writeByte(0);
				dos.writeInt(intNum);
			}
		}

		private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (intNum == null) {
				marshaller.writeByte(-1);
			} else {
				marshaller.writeByte(0);
				marshaller.writeInt(intNum);
			}
		}

		private String readString(ObjectInputStream dis) throws IOException {
			String strReturn = null;
			int length = 0;
			length = dis.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				dis.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException {
			String strReturn = null;
			int length = 0;
			length = unmarshaller.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				unmarshaller.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private void writeString(String str, ObjectOutputStream dos) throws IOException {
			if (str == null) {
				dos.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				dos.writeInt(byteArray.length);
				dos.write(byteArray);
			}
		}

		private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (str == null) {
				marshaller.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				marshaller.writeInt(byteArray.length);
				marshaller.write(byteArray);
			}
		}

		public void readData(ObjectInputStream dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.order_id = readInteger(dis);

					this.customer_id = readInteger(dis);

					this.product_id = readInteger(dis);

					this.quantity = readInteger(dis);

					this.unit_price = (BigDecimal) dis.readObject();

					this.total_amount = (BigDecimal) dis.readObject();

					this.order_date = readString(dis);

					this.order_status = readString(dis);

					this.payment_method = readString(dis);

					this.region = readString(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				} catch (ClassNotFoundException eCNFE) {
					throw new RuntimeException(eCNFE);

				}

			}

		}

		public void readData(org.jboss.marshalling.Unmarshaller dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.order_id = readInteger(dis);

					this.customer_id = readInteger(dis);

					this.product_id = readInteger(dis);

					this.quantity = readInteger(dis);

					this.unit_price = (BigDecimal) dis.readObject();

					this.total_amount = (BigDecimal) dis.readObject();

					this.order_date = readString(dis);

					this.order_status = readString(dis);

					this.payment_method = readString(dis);

					this.region = readString(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				} catch (ClassNotFoundException eCNFE) {
					throw new RuntimeException(eCNFE);

				}

			}

		}

		public void writeData(ObjectOutputStream dos) {
			try {

				// Integer

				writeInteger(this.order_id, dos);

				// Integer

				writeInteger(this.customer_id, dos);

				// Integer

				writeInteger(this.product_id, dos);

				// Integer

				writeInteger(this.quantity, dos);

				// BigDecimal

				dos.writeObject(this.unit_price);

				// BigDecimal

				dos.writeObject(this.total_amount);

				// String

				writeString(this.order_date, dos);

				// String

				writeString(this.order_status, dos);

				// String

				writeString(this.payment_method, dos);

				// String

				writeString(this.region, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public void writeData(org.jboss.marshalling.Marshaller dos) {
			try {

				// Integer

				writeInteger(this.order_id, dos);

				// Integer

				writeInteger(this.customer_id, dos);

				// Integer

				writeInteger(this.product_id, dos);

				// Integer

				writeInteger(this.quantity, dos);

				// BigDecimal

				dos.clearInstanceCache();
				dos.writeObject(this.unit_price);

				// BigDecimal

				dos.clearInstanceCache();
				dos.writeObject(this.total_amount);

				// String

				writeString(this.order_date, dos);

				// String

				writeString(this.order_status, dos);

				// String

				writeString(this.payment_method, dos);

				// String

				writeString(this.region, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");
			sb.append("order_id=" + String.valueOf(order_id));
			sb.append(",customer_id=" + String.valueOf(customer_id));
			sb.append(",product_id=" + String.valueOf(product_id));
			sb.append(",quantity=" + String.valueOf(quantity));
			sb.append(",unit_price=" + String.valueOf(unit_price));
			sb.append(",total_amount=" + String.valueOf(total_amount));
			sb.append(",order_date=" + order_date);
			sb.append(",order_status=" + order_status);
			sb.append(",payment_method=" + payment_method);
			sb.append(",region=" + region);
			sb.append("]");

			return sb.toString();
		}

		public String toLogString() {
			StringBuilder sb = new StringBuilder();

			if (order_id == null) {
				sb.append("<null>");
			} else {
				sb.append(order_id);
			}

			sb.append("|");

			if (customer_id == null) {
				sb.append("<null>");
			} else {
				sb.append(customer_id);
			}

			sb.append("|");

			if (product_id == null) {
				sb.append("<null>");
			} else {
				sb.append(product_id);
			}

			sb.append("|");

			if (quantity == null) {
				sb.append("<null>");
			} else {
				sb.append(quantity);
			}

			sb.append("|");

			if (unit_price == null) {
				sb.append("<null>");
			} else {
				sb.append(unit_price);
			}

			sb.append("|");

			if (total_amount == null) {
				sb.append("<null>");
			} else {
				sb.append(total_amount);
			}

			sb.append("|");

			if (order_date == null) {
				sb.append("<null>");
			} else {
				sb.append(order_date);
			}

			sb.append("|");

			if (order_status == null) {
				sb.append("<null>");
			} else {
				sb.append(order_status);
			}

			sb.append("|");

			if (payment_method == null) {
				sb.append("<null>");
			} else {
				sb.append(payment_method);
			}

			sb.append("|");

			if (region == null) {
				sb.append("<null>");
			} else {
				sb.append(region);
			}

			sb.append("|");

			return sb.toString();
		}

		/**
		 * Compare keys
		 */
		public int compareTo(rejected_ordersStruct other) {

			int returnValue = -1;

			return returnValue;
		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(), object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class row1Struct implements routines.system.IPersistableRow<row1Struct> {
		final static byte[] commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];
		static byte[] commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];

		public Integer order_id;

		public Integer getOrder_id() {
			return this.order_id;
		}

		public Boolean order_idIsNullable() {
			return true;
		}

		public Boolean order_idIsKey() {
			return false;
		}

		public Integer order_idLength() {
			return 10;
		}

		public Integer order_idPrecision() {
			return 0;
		}

		public String order_idDefault() {

			return "";

		}

		public String order_idComment() {

			return "";

		}

		public String order_idPattern() {

			return "";

		}

		public String order_idOriginalDbColumnName() {

			return "order_id";

		}

		public Integer customer_id;

		public Integer getCustomer_id() {
			return this.customer_id;
		}

		public Boolean customer_idIsNullable() {
			return true;
		}

		public Boolean customer_idIsKey() {
			return false;
		}

		public Integer customer_idLength() {
			return 10;
		}

		public Integer customer_idPrecision() {
			return 0;
		}

		public String customer_idDefault() {

			return "";

		}

		public String customer_idComment() {

			return "";

		}

		public String customer_idPattern() {

			return "";

		}

		public String customer_idOriginalDbColumnName() {

			return "customer_id";

		}

		public Integer product_id;

		public Integer getProduct_id() {
			return this.product_id;
		}

		public Boolean product_idIsNullable() {
			return true;
		}

		public Boolean product_idIsKey() {
			return false;
		}

		public Integer product_idLength() {
			return 10;
		}

		public Integer product_idPrecision() {
			return 0;
		}

		public String product_idDefault() {

			return "";

		}

		public String product_idComment() {

			return "";

		}

		public String product_idPattern() {

			return "";

		}

		public String product_idOriginalDbColumnName() {

			return "product_id";

		}

		public Integer quantity;

		public Integer getQuantity() {
			return this.quantity;
		}

		public Boolean quantityIsNullable() {
			return true;
		}

		public Boolean quantityIsKey() {
			return false;
		}

		public Integer quantityLength() {
			return 10;
		}

		public Integer quantityPrecision() {
			return 0;
		}

		public String quantityDefault() {

			return "";

		}

		public String quantityComment() {

			return "";

		}

		public String quantityPattern() {

			return "";

		}

		public String quantityOriginalDbColumnName() {

			return "quantity";

		}

		public BigDecimal unit_price;

		public BigDecimal getUnit_price() {
			return this.unit_price;
		}

		public Boolean unit_priceIsNullable() {
			return true;
		}

		public Boolean unit_priceIsKey() {
			return false;
		}

		public Integer unit_priceLength() {
			return 10;
		}

		public Integer unit_pricePrecision() {
			return 2;
		}

		public String unit_priceDefault() {

			return "";

		}

		public String unit_priceComment() {

			return "";

		}

		public String unit_pricePattern() {

			return "";

		}

		public String unit_priceOriginalDbColumnName() {

			return "unit_price";

		}

		public BigDecimal total_amount;

		public BigDecimal getTotal_amount() {
			return this.total_amount;
		}

		public Boolean total_amountIsNullable() {
			return true;
		}

		public Boolean total_amountIsKey() {
			return false;
		}

		public Integer total_amountLength() {
			return 10;
		}

		public Integer total_amountPrecision() {
			return 2;
		}

		public String total_amountDefault() {

			return "";

		}

		public String total_amountComment() {

			return "";

		}

		public String total_amountPattern() {

			return "";

		}

		public String total_amountOriginalDbColumnName() {

			return "total_amount";

		}

		public String order_date;

		public String getOrder_date() {
			return this.order_date;
		}

		public Boolean order_dateIsNullable() {
			return true;
		}

		public Boolean order_dateIsKey() {
			return false;
		}

		public Integer order_dateLength() {
			return 20;
		}

		public Integer order_datePrecision() {
			return 0;
		}

		public String order_dateDefault() {

			return null;

		}

		public String order_dateComment() {

			return "";

		}

		public String order_datePattern() {

			return "";

		}

		public String order_dateOriginalDbColumnName() {

			return "order_date";

		}

		public String order_status;

		public String getOrder_status() {
			return this.order_status;
		}

		public Boolean order_statusIsNullable() {
			return true;
		}

		public Boolean order_statusIsKey() {
			return false;
		}

		public Integer order_statusLength() {
			return 20;
		}

		public Integer order_statusPrecision() {
			return 0;
		}

		public String order_statusDefault() {

			return null;

		}

		public String order_statusComment() {

			return "";

		}

		public String order_statusPattern() {

			return "";

		}

		public String order_statusOriginalDbColumnName() {

			return "order_status";

		}

		public String payment_method;

		public String getPayment_method() {
			return this.payment_method;
		}

		public Boolean payment_methodIsNullable() {
			return true;
		}

		public Boolean payment_methodIsKey() {
			return false;
		}

		public Integer payment_methodLength() {
			return 20;
		}

		public Integer payment_methodPrecision() {
			return 0;
		}

		public String payment_methodDefault() {

			return null;

		}

		public String payment_methodComment() {

			return "";

		}

		public String payment_methodPattern() {

			return "";

		}

		public String payment_methodOriginalDbColumnName() {

			return "payment_method";

		}

		public String region;

		public String getRegion() {
			return this.region;
		}

		public Boolean regionIsNullable() {
			return true;
		}

		public Boolean regionIsKey() {
			return false;
		}

		public Integer regionLength() {
			return 20;
		}

		public Integer regionPrecision() {
			return 0;
		}

		public String regionDefault() {

			return null;

		}

		public String regionComment() {

			return "";

		}

		public String regionPattern() {

			return "";

		}

		public String regionOriginalDbColumnName() {

			return "region";

		}

		private Integer readInteger(ObjectInputStream dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException {
			if (intNum == null) {
				dos.writeByte(-1);
			} else {
				dos.writeByte(0);
				dos.writeInt(intNum);
			}
		}

		private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (intNum == null) {
				marshaller.writeByte(-1);
			} else {
				marshaller.writeByte(0);
				marshaller.writeInt(intNum);
			}
		}

		private String readString(ObjectInputStream dis) throws IOException {
			String strReturn = null;
			int length = 0;
			length = dis.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				dis.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException {
			String strReturn = null;
			int length = 0;
			length = unmarshaller.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				unmarshaller.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private void writeString(String str, ObjectOutputStream dos) throws IOException {
			if (str == null) {
				dos.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				dos.writeInt(byteArray.length);
				dos.write(byteArray);
			}
		}

		private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (str == null) {
				marshaller.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				marshaller.writeInt(byteArray.length);
				marshaller.write(byteArray);
			}
		}

		public void readData(ObjectInputStream dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.order_id = readInteger(dis);

					this.customer_id = readInteger(dis);

					this.product_id = readInteger(dis);

					this.quantity = readInteger(dis);

					this.unit_price = (BigDecimal) dis.readObject();

					this.total_amount = (BigDecimal) dis.readObject();

					this.order_date = readString(dis);

					this.order_status = readString(dis);

					this.payment_method = readString(dis);

					this.region = readString(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				} catch (ClassNotFoundException eCNFE) {
					throw new RuntimeException(eCNFE);

				}

			}

		}

		public void readData(org.jboss.marshalling.Unmarshaller dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.order_id = readInteger(dis);

					this.customer_id = readInteger(dis);

					this.product_id = readInteger(dis);

					this.quantity = readInteger(dis);

					this.unit_price = (BigDecimal) dis.readObject();

					this.total_amount = (BigDecimal) dis.readObject();

					this.order_date = readString(dis);

					this.order_status = readString(dis);

					this.payment_method = readString(dis);

					this.region = readString(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				} catch (ClassNotFoundException eCNFE) {
					throw new RuntimeException(eCNFE);

				}

			}

		}

		public void writeData(ObjectOutputStream dos) {
			try {

				// Integer

				writeInteger(this.order_id, dos);

				// Integer

				writeInteger(this.customer_id, dos);

				// Integer

				writeInteger(this.product_id, dos);

				// Integer

				writeInteger(this.quantity, dos);

				// BigDecimal

				dos.writeObject(this.unit_price);

				// BigDecimal

				dos.writeObject(this.total_amount);

				// String

				writeString(this.order_date, dos);

				// String

				writeString(this.order_status, dos);

				// String

				writeString(this.payment_method, dos);

				// String

				writeString(this.region, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public void writeData(org.jboss.marshalling.Marshaller dos) {
			try {

				// Integer

				writeInteger(this.order_id, dos);

				// Integer

				writeInteger(this.customer_id, dos);

				// Integer

				writeInteger(this.product_id, dos);

				// Integer

				writeInteger(this.quantity, dos);

				// BigDecimal

				dos.clearInstanceCache();
				dos.writeObject(this.unit_price);

				// BigDecimal

				dos.clearInstanceCache();
				dos.writeObject(this.total_amount);

				// String

				writeString(this.order_date, dos);

				// String

				writeString(this.order_status, dos);

				// String

				writeString(this.payment_method, dos);

				// String

				writeString(this.region, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");
			sb.append("order_id=" + String.valueOf(order_id));
			sb.append(",customer_id=" + String.valueOf(customer_id));
			sb.append(",product_id=" + String.valueOf(product_id));
			sb.append(",quantity=" + String.valueOf(quantity));
			sb.append(",unit_price=" + String.valueOf(unit_price));
			sb.append(",total_amount=" + String.valueOf(total_amount));
			sb.append(",order_date=" + order_date);
			sb.append(",order_status=" + order_status);
			sb.append(",payment_method=" + payment_method);
			sb.append(",region=" + region);
			sb.append("]");

			return sb.toString();
		}

		public String toLogString() {
			StringBuilder sb = new StringBuilder();

			if (order_id == null) {
				sb.append("<null>");
			} else {
				sb.append(order_id);
			}

			sb.append("|");

			if (customer_id == null) {
				sb.append("<null>");
			} else {
				sb.append(customer_id);
			}

			sb.append("|");

			if (product_id == null) {
				sb.append("<null>");
			} else {
				sb.append(product_id);
			}

			sb.append("|");

			if (quantity == null) {
				sb.append("<null>");
			} else {
				sb.append(quantity);
			}

			sb.append("|");

			if (unit_price == null) {
				sb.append("<null>");
			} else {
				sb.append(unit_price);
			}

			sb.append("|");

			if (total_amount == null) {
				sb.append("<null>");
			} else {
				sb.append(total_amount);
			}

			sb.append("|");

			if (order_date == null) {
				sb.append("<null>");
			} else {
				sb.append(order_date);
			}

			sb.append("|");

			if (order_status == null) {
				sb.append("<null>");
			} else {
				sb.append(order_status);
			}

			sb.append("|");

			if (payment_method == null) {
				sb.append("<null>");
			} else {
				sb.append(payment_method);
			}

			sb.append("|");

			if (region == null) {
				sb.append("<null>");
			} else {
				sb.append(region);
			}

			sb.append("|");

			return sb.toString();
		}

		/**
		 * Compare keys
		 */
		public int compareTo(row1Struct other) {

			int returnValue = -1;

			return returnValue;
		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(), object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public void tDBInput_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
		globalMap.put("tDBInput_1_SUBPROCESS_STATE", 0);

		final boolean execStat = this.execStat;

		mdc("tDBInput_1", "VvaqYt_");

		String iterateId = "";

		String currentComponent = "";
		s("none");
		String cLabel = null;
		java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

		try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { // start the resume
				globalResumeTicket = true;

				row1Struct row1 = new row1Struct();
				valid_ordersStruct valid_orders = new valid_ordersStruct();
				rejected_ordersStruct rejected_orders = new rejected_ordersStruct();

				/**
				 * [tDBOutput_1 begin ] start
				 */

				sh("tDBOutput_1");

				s(currentComponent = "tDBOutput_1");

				runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, 0, 0, "valid_orders");

				int tos_count_tDBOutput_1 = 0;

				if (log.isDebugEnabled())
					log.debug("tDBOutput_1 - " + ("Start to work."));
				if (log.isDebugEnabled()) {
					class BytesLimit65535_tDBOutput_1 {
						public void limitLog4jByte() throws Exception {
							StringBuilder log4jParamters_tDBOutput_1 = new StringBuilder();
							log4jParamters_tDBOutput_1.append("Parameters:");
							log4jParamters_tDBOutput_1
									.append("configuration.dataSet.dataStore.account" + " = " + "\"JRRAPJN-QP02531\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.dataSet.dataStore.snowflakeAuth.authenticationType" + " = "
											+ "BASIC");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("configuration.dataSet.dataStore.snowflakeAuth.userId"
									+ " = " + "\"KAN9922SNOWFLAKE\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("configuration.dataSet.dataStore.snowflakeAuth.password"
									+ " = "
									+ String.valueOf(
											"enc:routine.encryption.key.v1:oxoX+plo8zb6gkwZhGK5xt+CnwDNZ2S4rgumFyhZl/WWCEe6H3TyIw1ljbRY")
											.substring(0, 4)
									+ "...");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.dataSet.dataStore.warehouse" + " = " + "\"COMPUTE_WH\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.dataSet.dataStore.dbSchema" + " = " + "\"RAW\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.dataSet.dataStore.database" + " = " + "\"ECOMMERCE_POC\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("configuration.dataSet.tableName" + " = " + "RAW_ORDERS");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.snowflakeOutputCommonConfig.tableAction" + " = " + "NONE");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.snowflakeOutputCommonConfig.dataAction" + " = " + "INSERT");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("configuration.dieOnError" + " = " + "false");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.dataSet.dataStore.jdbcParameters" + " = " + "\"\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.dataSet.dataStore.loginTimeout" + " = " + "15");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("configuration.dataSet.dataStore.role" + " = " + "\"\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("configuration.dataSet.dataStore.jdbcUrlSuffix" + " = "
									+ "\".snowflakecomputing.com\"");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append(
									"configuration.snowflakeOutputCommonConfig.convertColumnsAndTableToUppercase"
											+ " = " + "true");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.snowflakeOutputCommonConfig.enforceDatabaseSchema" + " = "
											+ "false");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.snowflakeOutputCommonConfig.useVectors" + " = " + "false");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1
									.append("configuration.convertEmptyStringsToNull" + " = " + "false");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("configuration.useSchemaDatePattern" + " = " + "false");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("USE_EXISTING_CONNECTION" + " = " + "false");
							log4jParamters_tDBOutput_1.append(" | ");
							log4jParamters_tDBOutput_1.append("UNIFIED_COMPONENTS" + " = " + "SnowflakeOutput");
							log4jParamters_tDBOutput_1.append(" | ");
							if (log.isDebugEnabled())
								log.debug("tDBOutput_1 - " + (log4jParamters_tDBOutput_1));
						}
					}
					new BytesLimit65535_tDBOutput_1().limitLog4jByte();
				}
				if (enableLogStash) {
					talendJobLog.addCM("tDBOutput_1", "tDBOutput_1", "SnowflakeOutput");
					talendJobLogProcess(globalMap);
					s(currentComponent);
				}

				final org.talend.sdk.component.runtime.manager.ComponentManager mgr_tDBOutput_1 = org.talend.sdk.component.runtime.manager.ComponentManager
						.instance();
				mgr_tDBOutput_1.autoDiscoverPluginsIfEmpty(false, true);

				final java.util.Map<String, String> configuration_tDBOutput_1 = new java.util.HashMap<>();
				final java.util.Map<String, String> registry_metadata_tDBOutput_1 = new java.util.HashMap<>();

				final class SettingHelper_tDBOutput_1 {
					final java.util.Map<String, String> configuration;

					SettingHelper_tDBOutput_1(final java.util.Map<String, String> configuration) {
						this.configuration = configuration;
					}

					void put(String key, String value) {
						if (value != null) {
							configuration.put(key, value);
						}
					}
				}

				final SettingHelper_tDBOutput_1 s_tDBOutput_1 = new SettingHelper_tDBOutput_1(
						configuration_tDBOutput_1);
				Object dv_tDBOutput_1;
				java.net.URL mappings_url_tDBOutput_1 = this.getClass().getResource("/xmlMappings");
				globalMap.put("tDBOutput_1_MAPPINGS_URL", mappings_url_tDBOutput_1);
				globalMap.putIfAbsent("TALEND_PRODUCT_VERSION", "8.0");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.account", "JRRAPJN-QP02531");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.snowflakeAuth.authenticationType", "BASIC");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.snowflakeAuth.userId", "KAN9922SNOWFLAKE");
				s_tDBOutput_1.put("configuration.dataSet.dataStore.snowflakeAuth.password",
						routines.system.PasswordEncryptUtil.decryptPassword(
								"enc:routine.encryption.key.v1:JSweI9CpUICwotxvR0CR85oUgJc9hOmpB2MzDYGvoQDAYMlJ9VpoTidctUwl"));

				s_tDBOutput_1.put("configuration.dataSet.dataStore.warehouse", "COMPUTE_WH");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.dbSchema", "RAW");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.database", "ECOMMERCE_POC");

				s_tDBOutput_1.put("configuration.dataSet.tableName", "RAW_ORDERS");
				final class SchemaSettingHelper_tDBOutput_1_1 {

					public void set(final java.util.Map<String, String> configuration) throws java.lang.Exception {
						set_0(configuration);
					}

					public void set_0(final java.util.Map<String, String> configuration) throws java.lang.Exception {
						configuration.put("configuration.dataSet.schema[0].comment", "");
						configuration.put("configuration.dataSet.schema[0].defaultValue", "");
						configuration.put("configuration.dataSet.schema[0].key", "false");
						configuration.put("configuration.dataSet.schema[0].label", "order_id");
						configuration.put("configuration.dataSet.schema[0].length", "10");
						configuration.put("configuration.dataSet.schema[0].nullable", "true");
						configuration.put("configuration.dataSet.schema[0].originalDbColumnName", "order_id");
						configuration.put("configuration.dataSet.schema[0].pattern", "");
						configuration.put("configuration.dataSet.schema[0].precision", "0");
						configuration.put("configuration.dataSet.schema[0].talendType", "id_Integer");
						configuration.put("configuration.dataSet.schema[0].type", "INT");
						configuration.put("configuration.dataSet.schema[1].comment", "");
						configuration.put("configuration.dataSet.schema[1].defaultValue", "");
						configuration.put("configuration.dataSet.schema[1].key", "false");
						configuration.put("configuration.dataSet.schema[1].label", "customer_id");
						configuration.put("configuration.dataSet.schema[1].length", "10");
						configuration.put("configuration.dataSet.schema[1].nullable", "true");
						configuration.put("configuration.dataSet.schema[1].originalDbColumnName", "customer_id");
						configuration.put("configuration.dataSet.schema[1].pattern", "");
						configuration.put("configuration.dataSet.schema[1].precision", "0");
						configuration.put("configuration.dataSet.schema[1].talendType", "id_Integer");
						configuration.put("configuration.dataSet.schema[1].type", "INT");
						configuration.put("configuration.dataSet.schema[2].comment", "");
						configuration.put("configuration.dataSet.schema[2].defaultValue", "");
						configuration.put("configuration.dataSet.schema[2].key", "false");
						configuration.put("configuration.dataSet.schema[2].label", "product_id");
						configuration.put("configuration.dataSet.schema[2].length", "10");
						configuration.put("configuration.dataSet.schema[2].nullable", "true");
						configuration.put("configuration.dataSet.schema[2].originalDbColumnName", "product_id");
						configuration.put("configuration.dataSet.schema[2].pattern", "");
						configuration.put("configuration.dataSet.schema[2].precision", "0");
						configuration.put("configuration.dataSet.schema[2].talendType", "id_Integer");
						configuration.put("configuration.dataSet.schema[2].type", "INT");
						configuration.put("configuration.dataSet.schema[3].comment", "");
						configuration.put("configuration.dataSet.schema[3].defaultValue", "");
						configuration.put("configuration.dataSet.schema[3].key", "false");
						configuration.put("configuration.dataSet.schema[3].label", "quantity");
						configuration.put("configuration.dataSet.schema[3].length", "10");
						configuration.put("configuration.dataSet.schema[3].nullable", "true");
						configuration.put("configuration.dataSet.schema[3].originalDbColumnName", "quantity");
						configuration.put("configuration.dataSet.schema[3].pattern", "");
						configuration.put("configuration.dataSet.schema[3].precision", "0");
						configuration.put("configuration.dataSet.schema[3].talendType", "id_Integer");
						configuration.put("configuration.dataSet.schema[3].type", "INT");
						configuration.put("configuration.dataSet.schema[4].comment", "");
						configuration.put("configuration.dataSet.schema[4].defaultValue", "");
						configuration.put("configuration.dataSet.schema[4].key", "false");
						configuration.put("configuration.dataSet.schema[4].label", "unit_price");
						configuration.put("configuration.dataSet.schema[4].length", "10");
						configuration.put("configuration.dataSet.schema[4].nullable", "true");
						configuration.put("configuration.dataSet.schema[4].originalDbColumnName", "unit_price");
						configuration.put("configuration.dataSet.schema[4].pattern", "");
						configuration.put("configuration.dataSet.schema[4].precision", "2");
						configuration.put("configuration.dataSet.schema[4].talendType", "id_BigDecimal");
						configuration.put("configuration.dataSet.schema[4].type", "DECIMAL");
						configuration.put("configuration.dataSet.schema[5].comment", "");
						configuration.put("configuration.dataSet.schema[5].defaultValue", "");
						configuration.put("configuration.dataSet.schema[5].key", "false");
						configuration.put("configuration.dataSet.schema[5].label", "total_amount");
						configuration.put("configuration.dataSet.schema[5].length", "10");
						configuration.put("configuration.dataSet.schema[5].nullable", "true");
						configuration.put("configuration.dataSet.schema[5].originalDbColumnName", "total_amount");
						configuration.put("configuration.dataSet.schema[5].pattern", "");
						configuration.put("configuration.dataSet.schema[5].precision", "2");
						configuration.put("configuration.dataSet.schema[5].talendType", "id_BigDecimal");
						configuration.put("configuration.dataSet.schema[5].type", "DECIMAL");
						configuration.put("configuration.dataSet.schema[6].comment", "");
						configuration.put("configuration.dataSet.schema[6].defaultValue", "null");
						configuration.put("configuration.dataSet.schema[6].key", "false");
						configuration.put("configuration.dataSet.schema[6].label", "order_date");
						configuration.put("configuration.dataSet.schema[6].length", "20");
						configuration.put("configuration.dataSet.schema[6].nullable", "true");
						configuration.put("configuration.dataSet.schema[6].originalDbColumnName", "order_date");
						configuration.put("configuration.dataSet.schema[6].pattern", "");
						configuration.put("configuration.dataSet.schema[6].precision", "0");
						configuration.put("configuration.dataSet.schema[6].talendType", "id_String");
						configuration.put("configuration.dataSet.schema[6].type", "VARCHAR");
						configuration.put("configuration.dataSet.schema[7].comment", "");
						configuration.put("configuration.dataSet.schema[7].defaultValue", "null");
						configuration.put("configuration.dataSet.schema[7].key", "false");
						configuration.put("configuration.dataSet.schema[7].label", "order_status");
						configuration.put("configuration.dataSet.schema[7].length", "20");
						configuration.put("configuration.dataSet.schema[7].nullable", "true");
						configuration.put("configuration.dataSet.schema[7].originalDbColumnName", "order_status");
						configuration.put("configuration.dataSet.schema[7].pattern", "");
						configuration.put("configuration.dataSet.schema[7].precision", "0");
						configuration.put("configuration.dataSet.schema[7].talendType", "id_String");
						configuration.put("configuration.dataSet.schema[7].type", "VARCHAR");
						configuration.put("configuration.dataSet.schema[8].comment", "");
						configuration.put("configuration.dataSet.schema[8].defaultValue", "null");
						configuration.put("configuration.dataSet.schema[8].key", "false");
						configuration.put("configuration.dataSet.schema[8].label", "payment_method");
						configuration.put("configuration.dataSet.schema[8].length", "20");
						configuration.put("configuration.dataSet.schema[8].nullable", "true");
						configuration.put("configuration.dataSet.schema[8].originalDbColumnName", "payment_method");
						configuration.put("configuration.dataSet.schema[8].pattern", "");
						configuration.put("configuration.dataSet.schema[8].precision", "0");
						configuration.put("configuration.dataSet.schema[8].talendType", "id_String");
						configuration.put("configuration.dataSet.schema[8].type", "VARCHAR");
						configuration.put("configuration.dataSet.schema[9].comment", "");
						configuration.put("configuration.dataSet.schema[9].defaultValue", "null");
						configuration.put("configuration.dataSet.schema[9].key", "false");
						configuration.put("configuration.dataSet.schema[9].label", "region");
						configuration.put("configuration.dataSet.schema[9].length", "20");
						configuration.put("configuration.dataSet.schema[9].nullable", "true");
						configuration.put("configuration.dataSet.schema[9].originalDbColumnName", "region");
						configuration.put("configuration.dataSet.schema[9].pattern", "");
						configuration.put("configuration.dataSet.schema[9].precision", "0");
						configuration.put("configuration.dataSet.schema[9].talendType", "id_String");
						configuration.put("configuration.dataSet.schema[9].type", "VARCHAR");
					}
				}
				new SchemaSettingHelper_tDBOutput_1_1().set(configuration_tDBOutput_1);

				s_tDBOutput_1.put("configuration.snowflakeOutputCommonConfig.tableAction", "NONE");

				s_tDBOutput_1.put("configuration.snowflakeOutputCommonConfig.dataAction", "INSERT");

				s_tDBOutput_1.put("configuration.dieOnError", "false");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.jdbcParameters", "");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.loginTimeout", "15");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.role", "");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.jdbcUrlSuffix", ".snowflakecomputing.com");

				s_tDBOutput_1.put("configuration.snowflakeOutputCommonConfig.convertColumnsAndTableToUppercase",
						"true");

				s_tDBOutput_1.put("configuration.snowflakeOutputCommonConfig.enforceDatabaseSchema", "false");

				s_tDBOutput_1.put("configuration.snowflakeOutputCommonConfig.useVectors", "false");

				s_tDBOutput_1.put("configuration.convertEmptyStringsToNull", "false");

				s_tDBOutput_1.put("configuration.useSchemaDatePattern", "false");

				s_tDBOutput_1.put("configuration.dataSet.__version", "-1");

				s_tDBOutput_1.put("configuration.dataSet.dataStore.__version", "-1");

				final int chunkSize_tDBOutput_1 = globalMap.containsKey("MAX_BATCH_SIZE_tDBOutput_1")
						? (Integer) globalMap.get("MAX_BATCH_SIZE_tDBOutput_1")
						: 100;

				final org.talend.sdk.component.runtime.output.Processor processorImpl_tDBOutput_1 = mgr_tDBOutput_1
						.findProcessor("Snowflake", "Output", 1, configuration_tDBOutput_1)
						.orElseThrow(() -> new IllegalArgumentException("Can't find Snowflake#Output"));
				org.talend.sdk.component.runtime.di.studio.RuntimeContextInjector.injectLifecycle(
						processorImpl_tDBOutput_1,
						new org.talend.sdk.component.api.context.RuntimeContextHolder("tDBOutput_1", globalMap));

				final org.talend.sdk.component.runtime.di.studio.ParameterSetter changer_tDBOutput_1 = new org.talend.sdk.component.runtime.di.studio.ParameterSetter(
						processorImpl_tDBOutput_1);

				final javax.json.bind.Jsonb jsonb_tDBOutput_1 = (javax.json.bind.Jsonb) mgr_tDBOutput_1
						.findPlugin(processorImpl_tDBOutput_1.plugin()).get()
						.get(org.talend.sdk.component.runtime.manager.ComponentManager.AllServices.class).getServices()
						.get(javax.json.bind.Jsonb.class);

				final java.util.Map<Class<?>, Object> servicesMapper_tDBOutput_1 = mgr_tDBOutput_1
						.findPlugin(processorImpl_tDBOutput_1.plugin()).get()
						.get(org.talend.sdk.component.runtime.manager.ComponentManager.AllServices.class).getServices();

				final org.talend.sdk.component.runtime.di.AutoChunkProcessor processor_tDBOutput_1 = new org.talend.sdk.component.runtime.di.AutoChunkProcessor(
						chunkSize_tDBOutput_1, processorImpl_tDBOutput_1);
				org.talend.sdk.component.runtime.di.JobStateAware.init(processorImpl_tDBOutput_1, globalMap);
				processor_tDBOutput_1.start();
				globalMap.put("processor_tDBOutput_1", processor_tDBOutput_1);

				int nbLineInput_tDBOutput_1 = 0;
				int nbLineOutput_tDBOutput_1 = 0;

				final org.talend.sdk.component.runtime.di.InputsHandler inputsHandler_tDBOutput_1 = new org.talend.sdk.component.runtime.di.InputsHandler(
						jsonb_tDBOutput_1, servicesMapper_tDBOutput_1);
				inputsHandler_tDBOutput_1.addConnection("FLOW",
						valid_orders != null ? valid_orders.getClass() : valid_ordersStruct.class);
				final org.talend.sdk.component.runtime.output.InputFactory inputs_tDBOutput_1 = inputsHandler_tDBOutput_1
						.asInputFactory();

				final org.talend.sdk.component.runtime.di.OutputsHandler outputHandler_tDBOutput_1 = new org.talend.sdk.component.runtime.di.OutputsHandler(
						jsonb_tDBOutput_1, servicesMapper_tDBOutput_1);
				final org.talend.sdk.component.runtime.output.OutputFactory outputs_tDBOutput_1 = outputHandler_tDBOutput_1
						.asOutputFactory();

				/**
				 * [tDBOutput_1 begin ] stop
				 */

				/**
				 * [tLogRow_1 begin ] start
				 */

				sh("tLogRow_1");

				s(currentComponent = "tLogRow_1");

				runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, 0, 0, "rejected_orders");

				int tos_count_tLogRow_1 = 0;

				if (log.isDebugEnabled())
					log.debug("tLogRow_1 - " + ("Start to work."));
				if (log.isDebugEnabled()) {
					class BytesLimit65535_tLogRow_1 {
						public void limitLog4jByte() throws Exception {
							StringBuilder log4jParamters_tLogRow_1 = new StringBuilder();
							log4jParamters_tLogRow_1.append("Parameters:");
							log4jParamters_tLogRow_1.append("BASIC_MODE" + " = " + "false");
							log4jParamters_tLogRow_1.append(" | ");
							log4jParamters_tLogRow_1.append("TABLE_PRINT" + " = " + "true");
							log4jParamters_tLogRow_1.append(" | ");
							log4jParamters_tLogRow_1.append("VERTICAL" + " = " + "false");
							log4jParamters_tLogRow_1.append(" | ");
							log4jParamters_tLogRow_1.append("PRINT_CONTENT_WITH_LOG4J" + " = " + "true");
							log4jParamters_tLogRow_1.append(" | ");
							if (log.isDebugEnabled())
								log.debug("tLogRow_1 - " + (log4jParamters_tLogRow_1));
						}
					}
					new BytesLimit65535_tLogRow_1().limitLog4jByte();
				}
				if (enableLogStash) {
					talendJobLog.addCM("tLogRow_1", "tLogRow_1", "tLogRow");
					talendJobLogProcess(globalMap);
					s(currentComponent);
				}

				///////////////////////

				class Util_tLogRow_1 {

					String[] des_top = { ".", ".", "-", "+" };

					String[] des_head = { "|=", "=|", "-", "+" };

					String[] des_bottom = { "'", "'", "-", "+" };

					String name = "";

					java.util.List<String[]> list = new java.util.ArrayList<String[]>();

					int[] colLengths = new int[10];

					public void addRow(String[] row) {

						for (int i = 0; i < 10; i++) {
							if (row[i] != null) {
								colLengths[i] = Math.max(colLengths[i], row[i].length());
							}
						}
						list.add(row);
					}

					public void setTableName(String name) {

						this.name = name;
					}

					public StringBuilder format() {

						StringBuilder sb = new StringBuilder();

						sb.append(print(des_top));

						int totals = 0;
						for (int i = 0; i < colLengths.length; i++) {
							totals = totals + colLengths[i];
						}

						// name
						sb.append("|");
						int k = 0;
						for (k = 0; k < (totals + 9 - name.length()) / 2; k++) {
							sb.append(' ');
						}
						sb.append(name);
						for (int i = 0; i < totals + 9 - name.length() - k; i++) {
							sb.append(' ');
						}
						sb.append("|\n");

						// head and rows
						sb.append(print(des_head));
						for (int i = 0; i < list.size(); i++) {

							String[] row = list.get(i);

							java.util.Formatter formatter = new java.util.Formatter(new StringBuilder());

							StringBuilder sbformat = new StringBuilder();
							sbformat.append("|%1$-");
							sbformat.append(colLengths[0]);
							sbformat.append("s");

							sbformat.append("|%2$-");
							sbformat.append(colLengths[1]);
							sbformat.append("s");

							sbformat.append("|%3$-");
							sbformat.append(colLengths[2]);
							sbformat.append("s");

							sbformat.append("|%4$-");
							sbformat.append(colLengths[3]);
							sbformat.append("s");

							sbformat.append("|%5$-");
							sbformat.append(colLengths[4]);
							sbformat.append("s");

							sbformat.append("|%6$-");
							sbformat.append(colLengths[5]);
							sbformat.append("s");

							sbformat.append("|%7$-");
							sbformat.append(colLengths[6]);
							sbformat.append("s");

							sbformat.append("|%8$-");
							sbformat.append(colLengths[7]);
							sbformat.append("s");

							sbformat.append("|%9$-");
							sbformat.append(colLengths[8]);
							sbformat.append("s");

							sbformat.append("|%10$-");
							sbformat.append(colLengths[9]);
							sbformat.append("s");

							sbformat.append("|\n");

							formatter.format(sbformat.toString(), (Object[]) row);

							sb.append(formatter.toString());
							if (i == 0)
								sb.append(print(des_head)); // print the head
						}

						// end
						sb.append(print(des_bottom));
						return sb;
					}

					private StringBuilder print(String[] fillChars) {
						StringBuilder sb = new StringBuilder();
						// first column
						sb.append(fillChars[0]);
						for (int i = 0; i < colLengths[0] - fillChars[0].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);

						for (int i = 0; i < colLengths[1] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);
						for (int i = 0; i < colLengths[2] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);
						for (int i = 0; i < colLengths[3] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);
						for (int i = 0; i < colLengths[4] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);
						for (int i = 0; i < colLengths[5] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);
						for (int i = 0; i < colLengths[6] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);
						for (int i = 0; i < colLengths[7] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);
						for (int i = 0; i < colLengths[8] - fillChars[3].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[3]);

						// last column
						for (int i = 0; i < colLengths[9] - fillChars[1].length() + 1; i++) {
							sb.append(fillChars[2]);
						}
						sb.append(fillChars[1]);
						sb.append("\n");
						return sb;
					}

					public boolean isTableEmpty() {
						if (list.size() > 1)
							return false;
						return true;
					}
				}
				Util_tLogRow_1 util_tLogRow_1 = new Util_tLogRow_1();
				util_tLogRow_1.setTableName("tLogRow_1");
				util_tLogRow_1.addRow(new String[] { "order_id", "customer_id", "product_id", "quantity", "unit_price",
						"total_amount", "order_date", "order_status", "payment_method", "region", });
				StringBuilder strBuffer_tLogRow_1 = null;
				int nb_line_tLogRow_1 = 0;
///////////////////////    			

				/**
				 * [tLogRow_1 begin ] stop
				 */

				/**
				 * [tMap_1 begin ] start
				 */

				sh("tMap_1");

				s(currentComponent = "tMap_1");

				runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, 0, 0, "row1");

				int tos_count_tMap_1 = 0;

				if (log.isDebugEnabled())
					log.debug("tMap_1 - " + ("Start to work."));
				if (log.isDebugEnabled()) {
					class BytesLimit65535_tMap_1 {
						public void limitLog4jByte() throws Exception {
							StringBuilder log4jParamters_tMap_1 = new StringBuilder();
							log4jParamters_tMap_1.append("Parameters:");
							log4jParamters_tMap_1.append("LINK_STYLE" + " = " + "AUTO");
							log4jParamters_tMap_1.append(" | ");
							log4jParamters_tMap_1.append("TEMPORARY_DATA_DIRECTORY" + " = " + "");
							log4jParamters_tMap_1.append(" | ");
							log4jParamters_tMap_1.append("ROWS_BUFFER_SIZE" + " = " + "2000000");
							log4jParamters_tMap_1.append(" | ");
							log4jParamters_tMap_1.append("CHANGE_HASH_AND_EQUALS_FOR_BIGDECIMAL" + " = " + "true");
							log4jParamters_tMap_1.append(" | ");
							if (log.isDebugEnabled())
								log.debug("tMap_1 - " + (log4jParamters_tMap_1));
						}
					}
					new BytesLimit65535_tMap_1().limitLog4jByte();
				}
				if (enableLogStash) {
					talendJobLog.addCM("tMap_1", "tMap_1", "tMap");
					talendJobLogProcess(globalMap);
					s(currentComponent);
				}

// ###############################
// # Lookup's keys initialization
				int count_row1_tMap_1 = 0;

// ###############################        

// ###############################
// # Vars initialization
				class Var__tMap_1__Struct {
				}
				Var__tMap_1__Struct Var__tMap_1 = new Var__tMap_1__Struct();
// ###############################

// ###############################
// # Outputs initialization
				int count_valid_orders_tMap_1 = 0;

				valid_ordersStruct valid_orders_tmp = new valid_ordersStruct();
				int count_rejected_orders_tMap_1 = 0;

				rejected_ordersStruct rejected_orders_tmp = new rejected_ordersStruct();
// ###############################

				/**
				 * [tMap_1 begin ] stop
				 */

				/**
				 * [tDBInput_1 begin ] start
				 */

				sh("tDBInput_1");

				s(currentComponent = "tDBInput_1");

				int tos_count_tDBInput_1 = 0;

				if (log.isDebugEnabled())
					log.debug("tDBInput_1 - " + ("Start to work."));
				if (log.isDebugEnabled()) {
					class BytesLimit65535_tDBInput_1 {
						public void limitLog4jByte() throws Exception {
							StringBuilder log4jParamters_tDBInput_1 = new StringBuilder();
							log4jParamters_tDBInput_1.append("Parameters:");
							log4jParamters_tDBInput_1.append("USE_EXISTING_CONNECTION" + " = " + "false");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("HOST" + " = " + "\"KANINFOCOM\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("DRIVER" + " = " + "MSSQL_PROP");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("PORT" + " = " + "\"1433\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("DB_SCHEMA" + " = " + "\"dbo\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("DBNAME" + " = " + "\"EcommerceDB\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("USER" + " = " + "\"talend_user\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("PASS" + " = " + String.valueOf(
									"enc:routine.encryption.key.v1:/AM5g317LoDAZJWvmiHGVEspX5GJQIGE6mG5xE4AioXmpFuHQoi5HzM=")
									.substring(0, 4) + "...");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("TABLE" + " = " + "\"raw_orders\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("QUERYSTORE" + " = " + "\"\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("QUERY" + " = "
									+ "\"SELECT      order_id,      customer_id,      product_id,      quantity,      unit_price,      total_amount,      order_date,      order_status,      payment_method,      region  FROM dbo.raw_orders\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("SPECIFY_DATASOURCE_ALIAS" + " = " + "false");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1
									.append("PROPERTIES" + " = " + "\"encrypt=false;trustServerCertificate=true\"");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("ACTIVE_DIR_AUTH" + " = " + "false");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("ENABLE_ALWAYS_ENCRYPTED" + " = " + "false");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("TRIM_ALL_COLUMN" + " = " + "false");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("TRIM_COLUMN" + " = " + "[{TRIM=" + ("false")
									+ ", SCHEMA_COLUMN=" + ("order_id") + "}, {TRIM=" + ("false") + ", SCHEMA_COLUMN="
									+ ("customer_id") + "}, {TRIM=" + ("false") + ", SCHEMA_COLUMN=" + ("product_id")
									+ "}, {TRIM=" + ("false") + ", SCHEMA_COLUMN=" + ("quantity") + "}, {TRIM="
									+ ("false") + ", SCHEMA_COLUMN=" + ("unit_price") + "}, {TRIM=" + ("false")
									+ ", SCHEMA_COLUMN=" + ("total_amount") + "}, {TRIM=" + ("false")
									+ ", SCHEMA_COLUMN=" + ("order_date") + "}, {TRIM=" + ("false") + ", SCHEMA_COLUMN="
									+ ("order_status") + "}, {TRIM=" + ("false") + ", SCHEMA_COLUMN="
									+ ("payment_method") + "}, {TRIM=" + ("false") + ", SCHEMA_COLUMN=" + ("region")
									+ "}]");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("SET_QUERY_TIMEOUT" + " = " + "false");
							log4jParamters_tDBInput_1.append(" | ");
							log4jParamters_tDBInput_1.append("UNIFIED_COMPONENTS" + " = " + "tMSSqlInput");
							log4jParamters_tDBInput_1.append(" | ");
							if (log.isDebugEnabled())
								log.debug("tDBInput_1 - " + (log4jParamters_tDBInput_1));
						}
					}
					new BytesLimit65535_tDBInput_1().limitLog4jByte();
				}
				if (enableLogStash) {
					talendJobLog.addCM("tDBInput_1", "tDBInput_1", "tMSSqlInput");
					talendJobLogProcess(globalMap);
					s(currentComponent);
				}

				org.talend.designer.components.util.mssql.MSSqlGenerateTimestampUtil mssqlGTU_tDBInput_1 = org.talend.designer.components.util.mssql.MSSqlUtilFactory
						.getMSSqlGenerateTimestampUtil();

				java.util.List<String> talendToDBList_tDBInput_1 = new java.util.ArrayList();
				String[] talendToDBArray_tDBInput_1 = new String[] { "FLOAT", "NUMERIC", "NUMERIC IDENTITY", "DECIMAL",
						"DECIMAL IDENTITY", "REAL" };
				java.util.Collections.addAll(talendToDBList_tDBInput_1, talendToDBArray_tDBInput_1);
				int nb_line_tDBInput_1 = 0;
				java.sql.Connection conn_tDBInput_1 = null;
				String driverClass_tDBInput_1 = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
				java.lang.Class jdbcclazz_tDBInput_1 = java.lang.Class.forName(driverClass_tDBInput_1);
				String dbUser_tDBInput_1 = "talend_user";

				final String decryptedPassword_tDBInput_1 = java.util.Optional
						.ofNullable(routines.system.PasswordEncryptUtil.decryptPassword(
								"enc:routine.encryption.key.v1:k8jUDVi0YjBomqTgM0iJv5aGFgtgyHhfujcQJwFXeG3y3CgNqAMjLVY="))
						.orElse("");

				String dbPwd_tDBInput_1 = decryptedPassword_tDBInput_1;

				String port_tDBInput_1 = "1433";
				String dbname_tDBInput_1 = "EcommerceDB";
				String url_tDBInput_1 = "jdbc:sqlserver://" + "KANINFOCOM";
				if (!"".equals(port_tDBInput_1)) {
					url_tDBInput_1 += ":" + "1433";
				}
				if (!"".equals(dbname_tDBInput_1)) {
					url_tDBInput_1 += ";databaseName=" + "EcommerceDB";
				}
				url_tDBInput_1 += ";appName=" + projectName + ";" + "encrypt=false;trustServerCertificate=true";
				String dbschema_tDBInput_1 = "dbo";

				log.debug("tDBInput_1 - Driver ClassName: " + driverClass_tDBInput_1 + ".");

				log.debug("tDBInput_1 - Connection attempt to '"
						+ url_tDBInput_1.replaceAll("(?<=trustStorePassword=)[^;]*", "********")
						+ "' with the username '" + dbUser_tDBInput_1 + "'.");

				conn_tDBInput_1 = java.sql.DriverManager.getConnection(url_tDBInput_1, dbUser_tDBInput_1,
						dbPwd_tDBInput_1);
				log.debug("tDBInput_1 - Connection to '"
						+ url_tDBInput_1.replaceAll("(?<=trustStorePassword=)[^;]*", "********") + "' has succeeded.");

				java.sql.Statement stmt_tDBInput_1 = conn_tDBInput_1.createStatement();

				String dbquery_tDBInput_1 = new StringBuilder().append(
						"SELECT\n    order_id,\n    customer_id,\n    product_id,\n    quantity,\n    unit_price,\n    total_amount,\n    order_"
								+ "date,\n    order_status,\n    payment_method,\n    region\nFROM dbo.raw_orders")
						.toString();

				log.debug("tDBInput_1 - Executing the query: '" + dbquery_tDBInput_1 + "'.");

				globalMap.put("tDBInput_1_QUERY", dbquery_tDBInput_1);

				java.sql.ResultSet rs_tDBInput_1 = null;

				try {
					rs_tDBInput_1 = stmt_tDBInput_1.executeQuery(dbquery_tDBInput_1);
					java.sql.ResultSetMetaData rsmd_tDBInput_1 = rs_tDBInput_1.getMetaData();
					int colQtyInRs_tDBInput_1 = rsmd_tDBInput_1.getColumnCount();

					String tmpContent_tDBInput_1 = null;

					log.debug("tDBInput_1 - Retrieving records from the database.");

					while (rs_tDBInput_1.next()) {
						nb_line_tDBInput_1++;

						if (colQtyInRs_tDBInput_1 < 1) {
							row1.order_id = null;
						} else {

							row1.order_id = rs_tDBInput_1.getInt(1);
							if (rs_tDBInput_1.wasNull()) {
								row1.order_id = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 2) {
							row1.customer_id = null;
						} else {

							row1.customer_id = rs_tDBInput_1.getInt(2);
							if (rs_tDBInput_1.wasNull()) {
								row1.customer_id = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 3) {
							row1.product_id = null;
						} else {

							row1.product_id = rs_tDBInput_1.getInt(3);
							if (rs_tDBInput_1.wasNull()) {
								row1.product_id = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 4) {
							row1.quantity = null;
						} else {

							row1.quantity = rs_tDBInput_1.getInt(4);
							if (rs_tDBInput_1.wasNull()) {
								row1.quantity = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 5) {
							row1.unit_price = null;
						} else {

							row1.unit_price = rs_tDBInput_1.getBigDecimal(5);
							if (rs_tDBInput_1.wasNull()) {
								row1.unit_price = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 6) {
							row1.total_amount = null;
						} else {

							row1.total_amount = rs_tDBInput_1.getBigDecimal(6);
							if (rs_tDBInput_1.wasNull()) {
								row1.total_amount = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 7) {
							row1.order_date = null;
						} else {

							tmpContent_tDBInput_1 = rs_tDBInput_1.getString(7);
							if (tmpContent_tDBInput_1 != null) {
								if (talendToDBList_tDBInput_1.contains(
										rsmd_tDBInput_1.getColumnTypeName(7).toUpperCase(java.util.Locale.ENGLISH))) {
									row1.order_date = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
								} else {
									row1.order_date = tmpContent_tDBInput_1;
								}
							} else {
								row1.order_date = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 8) {
							row1.order_status = null;
						} else {

							tmpContent_tDBInput_1 = rs_tDBInput_1.getString(8);
							if (tmpContent_tDBInput_1 != null) {
								if (talendToDBList_tDBInput_1.contains(
										rsmd_tDBInput_1.getColumnTypeName(8).toUpperCase(java.util.Locale.ENGLISH))) {
									row1.order_status = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
								} else {
									row1.order_status = tmpContent_tDBInput_1;
								}
							} else {
								row1.order_status = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 9) {
							row1.payment_method = null;
						} else {

							tmpContent_tDBInput_1 = rs_tDBInput_1.getString(9);
							if (tmpContent_tDBInput_1 != null) {
								if (talendToDBList_tDBInput_1.contains(
										rsmd_tDBInput_1.getColumnTypeName(9).toUpperCase(java.util.Locale.ENGLISH))) {
									row1.payment_method = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
								} else {
									row1.payment_method = tmpContent_tDBInput_1;
								}
							} else {
								row1.payment_method = null;
							}
						}
						if (colQtyInRs_tDBInput_1 < 10) {
							row1.region = null;
						} else {

							tmpContent_tDBInput_1 = rs_tDBInput_1.getString(10);
							if (tmpContent_tDBInput_1 != null) {
								if (talendToDBList_tDBInput_1.contains(
										rsmd_tDBInput_1.getColumnTypeName(10).toUpperCase(java.util.Locale.ENGLISH))) {
									row1.region = FormatterUtils.formatUnwithE(tmpContent_tDBInput_1);
								} else {
									row1.region = tmpContent_tDBInput_1;
								}
							} else {
								row1.region = null;
							}
						}

						log.debug("tDBInput_1 - Retrieving the record " + nb_line_tDBInput_1 + ".");

						/**
						 * [tDBInput_1 begin ] stop
						 */

						/**
						 * [tDBInput_1 main ] start
						 */

						s(currentComponent = "tDBInput_1");

						tos_count_tDBInput_1++;

						/**
						 * [tDBInput_1 main ] stop
						 */

						/**
						 * [tDBInput_1 process_data_begin ] start
						 */

						s(currentComponent = "tDBInput_1");

						/**
						 * [tDBInput_1 process_data_begin ] stop
						 */

						/**
						 * [tMap_1 main ] start
						 */

						s(currentComponent = "tMap_1");

						if (runStat.update(execStat, enableLogStash, iterateId, 1, 1

								, "row1", "tDBInput_1", "tDBInput_1", "tMSSqlInput", "tMap_1", "tMap_1", "tMap"

						)) {
							talendJobLogProcess(globalMap);
						}

						if (log.isTraceEnabled()) {
							log.trace("row1 - " + (row1 == null ? "" : row1.toLogString()));
						}

						boolean hasCasePrimitiveKeyWithNull_tMap_1 = false;

						// ###############################
						// # Input tables (lookups)

						boolean rejectedInnerJoin_tMap_1 = false;
						boolean mainRowRejected_tMap_1 = false;
						// ###############################
						{ // start of Var scope

							// ###############################
							// # Vars tables

							Var__tMap_1__Struct Var = Var__tMap_1;// ###############################
							// ###############################
							// # Output tables

							valid_orders = null;
							rejected_orders = null;

// # Output table : 'valid_orders'
// # Filter conditions 
							if (

							row1.order_id != 0 && row1.total_amount.compareTo(java.math.BigDecimal.ZERO) > 0
									&& row1.customer_id != 0

							) {
								count_valid_orders_tMap_1++;

								valid_orders_tmp.order_id = row1.order_id;
								valid_orders_tmp.customer_id = row1.customer_id;
								valid_orders_tmp.product_id = row1.product_id;
								valid_orders_tmp.quantity = row1.quantity;
								valid_orders_tmp.unit_price = row1.unit_price;
								valid_orders_tmp.total_amount = row1.total_amount;
								valid_orders_tmp.order_date = row1.order_date;
								valid_orders_tmp.order_status = row1.order_status;
								valid_orders_tmp.payment_method = row1.payment_method;
								valid_orders_tmp.region = row1.region;
								valid_orders = valid_orders_tmp;
								log.debug("tMap_1 - Outputting the record " + count_valid_orders_tMap_1
										+ " of the output table 'valid_orders'.");

							} // closing filter/reject

// # Output table : 'rejected_orders'
// # Filter conditions 
							if (

							row1.order_id == 0 || row1.total_amount.compareTo(java.math.BigDecimal.ZERO) <= 0

							) {
								count_rejected_orders_tMap_1++;

								rejected_orders_tmp.order_id = row1.order_id;
								rejected_orders_tmp.customer_id = row1.customer_id;
								rejected_orders_tmp.product_id = row1.product_id;
								rejected_orders_tmp.quantity = row1.quantity;
								rejected_orders_tmp.unit_price = row1.unit_price;
								rejected_orders_tmp.total_amount = row1.total_amount;
								rejected_orders_tmp.order_date = row1.order_date;
								rejected_orders_tmp.order_status = row1.order_status;
								rejected_orders_tmp.payment_method = row1.payment_method;
								rejected_orders_tmp.region = row1.region;
								rejected_orders = rejected_orders_tmp;
								log.debug("tMap_1 - Outputting the record " + count_rejected_orders_tMap_1
										+ " of the output table 'rejected_orders'.");

							} // closing filter/reject
// ###############################

						} // end of Var scope

						rejectedInnerJoin_tMap_1 = false;

						tos_count_tMap_1++;

						/**
						 * [tMap_1 main ] stop
						 */

						/**
						 * [tMap_1 process_data_begin ] start
						 */

						s(currentComponent = "tMap_1");

						/**
						 * [tMap_1 process_data_begin ] stop
						 */

// Start of branch "valid_orders"
						if (valid_orders != null) {

							/**
							 * [tDBOutput_1 main ] start
							 */

							s(currentComponent = "tDBOutput_1");

							if (runStat.update(execStat, enableLogStash, iterateId, 1, 1

									, "valid_orders", "tMap_1", "tMap_1", "tMap", "tDBOutput_1", "tDBOutput_1",
									"SnowflakeOutput"

							)) {
								talendJobLogProcess(globalMap);
							}

							if (log.isTraceEnabled()) {
								log.trace("valid_orders - " + (valid_orders == null ? "" : valid_orders.toLogString()));
							}

							if (valid_orders != null) {
								inputsHandler_tDBOutput_1.setInputValue("FLOW", valid_orders);
							}

							processor_tDBOutput_1.onElement(inputs_tDBOutput_1, outputs_tDBOutput_1);
							nbLineInput_tDBOutput_1++;

							tos_count_tDBOutput_1++;

							/**
							 * [tDBOutput_1 main ] stop
							 */

							/**
							 * [tDBOutput_1 process_data_begin ] start
							 */

							s(currentComponent = "tDBOutput_1");

							/**
							 * [tDBOutput_1 process_data_begin ] stop
							 */

							/**
							 * [tDBOutput_1 process_data_end ] start
							 */

							s(currentComponent = "tDBOutput_1");

							/**
							 * [tDBOutput_1 process_data_end ] stop
							 */

						} // End of branch "valid_orders"

// Start of branch "rejected_orders"
						if (rejected_orders != null) {

							/**
							 * [tLogRow_1 main ] start
							 */

							s(currentComponent = "tLogRow_1");

							if (runStat.update(execStat, enableLogStash, iterateId, 1, 1

									, "rejected_orders", "tMap_1", "tMap_1", "tMap", "tLogRow_1", "tLogRow_1", "tLogRow"

							)) {
								talendJobLogProcess(globalMap);
							}

							if (log.isTraceEnabled()) {
								log.trace("rejected_orders - "
										+ (rejected_orders == null ? "" : rejected_orders.toLogString()));
							}

///////////////////////		

							String[] row_tLogRow_1 = new String[10];

							if (rejected_orders.order_id != null) { //
								row_tLogRow_1[0] = String.valueOf(rejected_orders.order_id);

							} //

							if (rejected_orders.customer_id != null) { //
								row_tLogRow_1[1] = String.valueOf(rejected_orders.customer_id);

							} //

							if (rejected_orders.product_id != null) { //
								row_tLogRow_1[2] = String.valueOf(rejected_orders.product_id);

							} //

							if (rejected_orders.quantity != null) { //
								row_tLogRow_1[3] = String.valueOf(rejected_orders.quantity);

							} //

							if (rejected_orders.unit_price != null) { //
								row_tLogRow_1[4] = rejected_orders.unit_price
										.setScale(2, java.math.RoundingMode.HALF_UP).toPlainString();

							} //

							if (rejected_orders.total_amount != null) { //
								row_tLogRow_1[5] = rejected_orders.total_amount
										.setScale(2, java.math.RoundingMode.HALF_UP).toPlainString();

							} //

							if (rejected_orders.order_date != null) { //
								row_tLogRow_1[6] = String.valueOf(rejected_orders.order_date);

							} //

							if (rejected_orders.order_status != null) { //
								row_tLogRow_1[7] = String.valueOf(rejected_orders.order_status);

							} //

							if (rejected_orders.payment_method != null) { //
								row_tLogRow_1[8] = String.valueOf(rejected_orders.payment_method);

							} //

							if (rejected_orders.region != null) { //
								row_tLogRow_1[9] = String.valueOf(rejected_orders.region);

							} //

							util_tLogRow_1.addRow(row_tLogRow_1);
							nb_line_tLogRow_1++;
							log.info("tLogRow_1 - Content of row " + nb_line_tLogRow_1 + ": "
									+ TalendString.unionString("|", row_tLogRow_1));
//////

//////                    

///////////////////////    			

							tos_count_tLogRow_1++;

							/**
							 * [tLogRow_1 main ] stop
							 */

							/**
							 * [tLogRow_1 process_data_begin ] start
							 */

							s(currentComponent = "tLogRow_1");

							/**
							 * [tLogRow_1 process_data_begin ] stop
							 */

							/**
							 * [tLogRow_1 process_data_end ] start
							 */

							s(currentComponent = "tLogRow_1");

							/**
							 * [tLogRow_1 process_data_end ] stop
							 */

						} // End of branch "rejected_orders"

						/**
						 * [tMap_1 process_data_end ] start
						 */

						s(currentComponent = "tMap_1");

						/**
						 * [tMap_1 process_data_end ] stop
						 */

						/**
						 * [tDBInput_1 process_data_end ] start
						 */

						s(currentComponent = "tDBInput_1");

						/**
						 * [tDBInput_1 process_data_end ] stop
						 */

						/**
						 * [tDBInput_1 end ] start
						 */

						s(currentComponent = "tDBInput_1");

					}
				} finally {
					if (rs_tDBInput_1 != null) {
						rs_tDBInput_1.close();
					}
					if (stmt_tDBInput_1 != null) {
						stmt_tDBInput_1.close();
					}
					if (conn_tDBInput_1 != null && !conn_tDBInput_1.isClosed()) {

						log.debug("tDBInput_1 - Closing the connection to the database.");

						conn_tDBInput_1.close();

						if ("com.mysql.cj.jdbc.Driver".equals((String) globalMap.get("driverClass_"))
								&& routines.system.BundleUtils.inOSGi()) {
							Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread")
									.getMethod("checkedShutdown").invoke(null, (Object[]) null);
						}

						log.debug("tDBInput_1 - Connection to the database closed.");

					}
				}
				globalMap.put("tDBInput_1_NB_LINE", nb_line_tDBInput_1);
				log.debug("tDBInput_1 - Retrieved records count: " + nb_line_tDBInput_1 + " .");

				if (log.isDebugEnabled())
					log.debug("tDBInput_1 - " + ("Done."));

				ok_Hash.put("tDBInput_1", true);
				end_Hash.put("tDBInput_1", System.currentTimeMillis());

				/**
				 * [tDBInput_1 end ] stop
				 */

				/**
				 * [tMap_1 end ] start
				 */

				s(currentComponent = "tMap_1");

// ###############################
// # Lookup hashes releasing
// ###############################      
				log.debug("tMap_1 - Written records count in the table 'valid_orders': " + count_valid_orders_tMap_1
						+ ".");
				log.debug("tMap_1 - Written records count in the table 'rejected_orders': "
						+ count_rejected_orders_tMap_1 + ".");

				if (runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, "row1", 2, 0,
						"tDBInput_1", "tDBInput_1", "tMSSqlInput", "tMap_1", "tMap_1", "tMap", "output")) {
					talendJobLogProcess(globalMap);
				}

				if (log.isDebugEnabled())
					log.debug("tMap_1 - " + ("Done."));

				ok_Hash.put("tMap_1", true);
				end_Hash.put("tMap_1", System.currentTimeMillis());

				/**
				 * [tMap_1 end ] stop
				 */

				/**
				 * [tDBOutput_1 process_records_end ] start
				 */

				s(currentComponent = "tDBOutput_1");

				processor_tDBOutput_1.flush(outputs_tDBOutput_1);

// extract after variables from the processor map and put to after variables map of job
				final java.util.Map<String, Object> afterVariablesMap_tDBOutput_1 = org.talend.sdk.component.runtime.di.studio.AfterVariableExtracter
						.extractAfterVariables(processorImpl_tDBOutput_1);
				for (java.util.Map.Entry<String, Object> entry_tDBOutput_1 : afterVariablesMap_tDBOutput_1.entrySet()) {
					globalMap.put("tDBOutput_1_" + entry_tDBOutput_1.getKey(), entry_tDBOutput_1.getValue());
				}

				if (processor_tDBOutput_1 != null) {
					processor_tDBOutput_1.stop();
				}

				globalMap.remove("processor_tDBOutput_1");

				/**
				 * [tDBOutput_1 process_records_end ] stop
				 */

				/**
				 * [tDBOutput_1 process_data_begin ] start
				 */

				s(currentComponent = "tDBOutput_1");

				/**
				 * [tDBOutput_1 process_data_begin ] stop
				 */

				/**
				 * [tDBOutput_1 process_data_end ] start
				 */

				s(currentComponent = "tDBOutput_1");

				/**
				 * [tDBOutput_1 process_data_end ] stop
				 */

				/**
				 * [tDBOutput_1 end ] start
				 */

				s(currentComponent = "tDBOutput_1");

				final int n_tDBOutput_1 = nbLineOutput_tDBOutput_1 > 0 ? nbLineOutput_tDBOutput_1
						: nbLineInput_tDBOutput_1;
				globalMap.computeIfAbsent("tDBOutput_1_NB_LINE", k -> n_tDBOutput_1);
				if (runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, "valid_orders", 2, 0,
						"tMap_1", "tMap_1", "tMap", "tDBOutput_1", "tDBOutput_1", "SnowflakeOutput", "output")) {
					talendJobLogProcess(globalMap);
				}

				if (log.isDebugEnabled())
					log.debug("tDBOutput_1 - " + ("Done."));

				ok_Hash.put("tDBOutput_1", true);
				end_Hash.put("tDBOutput_1", System.currentTimeMillis());

				/**
				 * [tDBOutput_1 end ] stop
				 */

				/**
				 * [tLogRow_1 end ] start
				 */

				s(currentComponent = "tLogRow_1");

//////

				java.io.PrintStream consoleOut_tLogRow_1 = null;
				if (globalMap.get("tLogRow_CONSOLE") != null) {
					consoleOut_tLogRow_1 = (java.io.PrintStream) globalMap.get("tLogRow_CONSOLE");
				} else {
					consoleOut_tLogRow_1 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));
					globalMap.put("tLogRow_CONSOLE", consoleOut_tLogRow_1);
				}

				consoleOut_tLogRow_1.println(util_tLogRow_1.format().toString());
				consoleOut_tLogRow_1.flush();
//////
				globalMap.put("tLogRow_1_NB_LINE", nb_line_tLogRow_1);
				if (log.isInfoEnabled())
					log.info("tLogRow_1 - " + ("Printed row count: ") + (nb_line_tLogRow_1) + ("."));

///////////////////////    			

				if (runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, "rejected_orders", 2, 0,
						"tMap_1", "tMap_1", "tMap", "tLogRow_1", "tLogRow_1", "tLogRow", "output")) {
					talendJobLogProcess(globalMap);
				}

				if (log.isDebugEnabled())
					log.debug("tLogRow_1 - " + ("Done."));

				ok_Hash.put("tLogRow_1", true);
				end_Hash.put("tLogRow_1", System.currentTimeMillis());

				/**
				 * [tLogRow_1 end ] stop
				 */

			} // end the resume

		} catch (java.lang.Exception e) {

			if (!(e instanceof TalendException) && !(e instanceof TDieException)) {
				log.fatal(currentComponent + " " + e.getMessage(), e);
			}

			TalendException te = new TalendException(e, currentComponent, cLabel, globalMap);

			throw te;
		} catch (java.lang.Error error) {

			runStat.stopThreadStat();

			throw error;
		} finally {

			try {

				/**
				 * [tDBInput_1 finally ] start
				 */

				s(currentComponent = "tDBInput_1");

				/**
				 * [tDBInput_1 finally ] stop
				 */

				/**
				 * [tMap_1 finally ] start
				 */

				s(currentComponent = "tMap_1");

				/**
				 * [tMap_1 finally ] stop
				 */

				/**
				 * [tDBOutput_1 finally ] start
				 */

				s(currentComponent = "tDBOutput_1");

				final org.talend.sdk.component.runtime.di.AutoChunkProcessor processor_tDBOutput_1 = org.talend.sdk.component.runtime.di.AutoChunkProcessor.class
						.cast(globalMap.remove("processor_tDBOutput_1"));
				try {
					if (processor_tDBOutput_1 != null) {
						processor_tDBOutput_1.stop();
					}
				} catch (final RuntimeException re) {
					throw new TalendException(re, currentComponent, cLabel, globalMap);
				}

				/**
				 * [tDBOutput_1 finally ] stop
				 */

				/**
				 * [tLogRow_1 finally ] start
				 */

				s(currentComponent = "tLogRow_1");

				/**
				 * [tLogRow_1 finally ] stop
				 */

			} catch (java.lang.Exception e) {
				// ignore
			} catch (java.lang.Error error) {
				// ignore
			}
			resourceMap = null;
		}

		globalMap.put("tDBInput_1_SUBPROCESS_STATE", 1);
	}

	public static class row2Struct implements routines.system.IPersistableRow<row2Struct> {
		final static byte[] commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];
		static byte[] commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[0];

		public java.util.Date moment;

		public java.util.Date getMoment() {
			return this.moment;
		}

		public Boolean momentIsNullable() {
			return true;
		}

		public Boolean momentIsKey() {
			return false;
		}

		public Integer momentLength() {
			return 0;
		}

		public Integer momentPrecision() {
			return 0;
		}

		public String momentDefault() {

			return "";

		}

		public String momentComment() {

			return null;

		}

		public String momentPattern() {

			return "yyyy-MM-dd HH:mm:ss";

		}

		public String momentOriginalDbColumnName() {

			return "moment";

		}

		public String pid;

		public String getPid() {
			return this.pid;
		}

		public Boolean pidIsNullable() {
			return true;
		}

		public Boolean pidIsKey() {
			return false;
		}

		public Integer pidLength() {
			return 20;
		}

		public Integer pidPrecision() {
			return 0;
		}

		public String pidDefault() {

			return "";

		}

		public String pidComment() {

			return null;

		}

		public String pidPattern() {

			return null;

		}

		public String pidOriginalDbColumnName() {

			return "pid";

		}

		public String root_pid;

		public String getRoot_pid() {
			return this.root_pid;
		}

		public Boolean root_pidIsNullable() {
			return true;
		}

		public Boolean root_pidIsKey() {
			return false;
		}

		public Integer root_pidLength() {
			return 20;
		}

		public Integer root_pidPrecision() {
			return 0;
		}

		public String root_pidDefault() {

			return "";

		}

		public String root_pidComment() {

			return null;

		}

		public String root_pidPattern() {

			return null;

		}

		public String root_pidOriginalDbColumnName() {

			return "root_pid";

		}

		public String father_pid;

		public String getFather_pid() {
			return this.father_pid;
		}

		public Boolean father_pidIsNullable() {
			return true;
		}

		public Boolean father_pidIsKey() {
			return false;
		}

		public Integer father_pidLength() {
			return 20;
		}

		public Integer father_pidPrecision() {
			return 0;
		}

		public String father_pidDefault() {

			return "";

		}

		public String father_pidComment() {

			return null;

		}

		public String father_pidPattern() {

			return null;

		}

		public String father_pidOriginalDbColumnName() {

			return "father_pid";

		}

		public String project;

		public String getProject() {
			return this.project;
		}

		public Boolean projectIsNullable() {
			return true;
		}

		public Boolean projectIsKey() {
			return false;
		}

		public Integer projectLength() {
			return 50;
		}

		public Integer projectPrecision() {
			return 0;
		}

		public String projectDefault() {

			return "";

		}

		public String projectComment() {

			return null;

		}

		public String projectPattern() {

			return null;

		}

		public String projectOriginalDbColumnName() {

			return "project";

		}

		public String job;

		public String getJob() {
			return this.job;
		}

		public Boolean jobIsNullable() {
			return true;
		}

		public Boolean jobIsKey() {
			return false;
		}

		public Integer jobLength() {
			return 255;
		}

		public Integer jobPrecision() {
			return 0;
		}

		public String jobDefault() {

			return "";

		}

		public String jobComment() {

			return null;

		}

		public String jobPattern() {

			return null;

		}

		public String jobOriginalDbColumnName() {

			return "job";

		}

		public String context;

		public String getContext() {
			return this.context;
		}

		public Boolean contextIsNullable() {
			return true;
		}

		public Boolean contextIsKey() {
			return false;
		}

		public Integer contextLength() {
			return 50;
		}

		public Integer contextPrecision() {
			return 0;
		}

		public String contextDefault() {

			return "";

		}

		public String contextComment() {

			return null;

		}

		public String contextPattern() {

			return null;

		}

		public String contextOriginalDbColumnName() {

			return "context";

		}

		public Integer priority;

		public Integer getPriority() {
			return this.priority;
		}

		public Boolean priorityIsNullable() {
			return true;
		}

		public Boolean priorityIsKey() {
			return false;
		}

		public Integer priorityLength() {
			return 3;
		}

		public Integer priorityPrecision() {
			return 0;
		}

		public String priorityDefault() {

			return "";

		}

		public String priorityComment() {

			return null;

		}

		public String priorityPattern() {

			return null;

		}

		public String priorityOriginalDbColumnName() {

			return "priority";

		}

		public String type;

		public String getType() {
			return this.type;
		}

		public Boolean typeIsNullable() {
			return true;
		}

		public Boolean typeIsKey() {
			return false;
		}

		public Integer typeLength() {
			return 255;
		}

		public Integer typePrecision() {
			return 0;
		}

		public String typeDefault() {

			return "";

		}

		public String typeComment() {

			return null;

		}

		public String typePattern() {

			return null;

		}

		public String typeOriginalDbColumnName() {

			return "type";

		}

		public String origin;

		public String getOrigin() {
			return this.origin;
		}

		public Boolean originIsNullable() {
			return true;
		}

		public Boolean originIsKey() {
			return false;
		}

		public Integer originLength() {
			return 255;
		}

		public Integer originPrecision() {
			return 0;
		}

		public String originDefault() {

			return "";

		}

		public String originComment() {

			return null;

		}

		public String originPattern() {

			return null;

		}

		public String originOriginalDbColumnName() {

			return "origin";

		}

		public String message;

		public String getMessage() {
			return this.message;
		}

		public Boolean messageIsNullable() {
			return true;
		}

		public Boolean messageIsKey() {
			return false;
		}

		public Integer messageLength() {
			return 255;
		}

		public Integer messagePrecision() {
			return 0;
		}

		public String messageDefault() {

			return "";

		}

		public String messageComment() {

			return null;

		}

		public String messagePattern() {

			return null;

		}

		public String messageOriginalDbColumnName() {

			return "message";

		}

		public Integer code;

		public Integer getCode() {
			return this.code;
		}

		public Boolean codeIsNullable() {
			return true;
		}

		public Boolean codeIsKey() {
			return false;
		}

		public Integer codeLength() {
			return 3;
		}

		public Integer codePrecision() {
			return 0;
		}

		public String codeDefault() {

			return "";

		}

		public String codeComment() {

			return null;

		}

		public String codePattern() {

			return null;

		}

		public String codeOriginalDbColumnName() {

			return "code";

		}

		private java.util.Date readDate(ObjectInputStream dis) throws IOException {
			java.util.Date dateReturn = null;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				dateReturn = null;
			} else {
				dateReturn = new Date(dis.readLong());
			}
			return dateReturn;
		}

		private java.util.Date readDate(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException {
			java.util.Date dateReturn = null;
			int length = 0;
			length = unmarshaller.readByte();
			if (length == -1) {
				dateReturn = null;
			} else {
				dateReturn = new Date(unmarshaller.readLong());
			}
			return dateReturn;
		}

		private void writeDate(java.util.Date date1, ObjectOutputStream dos) throws IOException {
			if (date1 == null) {
				dos.writeByte(-1);
			} else {
				dos.writeByte(0);
				dos.writeLong(date1.getTime());
			}
		}

		private void writeDate(java.util.Date date1, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (date1 == null) {
				marshaller.writeByte(-1);
			} else {
				marshaller.writeByte(0);
				marshaller.writeLong(date1.getTime());
			}
		}

		private String readString(ObjectInputStream dis) throws IOException {
			String strReturn = null;
			int length = 0;
			length = dis.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				dis.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException {
			String strReturn = null;
			int length = 0;
			length = unmarshaller.readInt();
			if (length == -1) {
				strReturn = null;
			} else {
				if (length > commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length) {
					if (length < 1024 && commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake.length == 0) {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[1024];
					} else {
						commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake = new byte[2 * length];
					}
				}
				unmarshaller.readFully(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length);
				strReturn = new String(commonByteArray_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake, 0, length,
						utf8Charset);
			}
			return strReturn;
		}

		private void writeString(String str, ObjectOutputStream dos) throws IOException {
			if (str == null) {
				dos.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				dos.writeInt(byteArray.length);
				dos.write(byteArray);
			}
		}

		private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (str == null) {
				marshaller.writeInt(-1);
			} else {
				byte[] byteArray = str.getBytes(utf8Charset);
				marshaller.writeInt(byteArray.length);
				marshaller.write(byteArray);
			}
		}

		private Integer readInteger(ObjectInputStream dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException {
			Integer intReturn;
			int length = 0;
			length = dis.readByte();
			if (length == -1) {
				intReturn = null;
			} else {
				intReturn = dis.readInt();
			}
			return intReturn;
		}

		private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException {
			if (intNum == null) {
				dos.writeByte(-1);
			} else {
				dos.writeByte(0);
				dos.writeInt(intNum);
			}
		}

		private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException {
			if (intNum == null) {
				marshaller.writeByte(-1);
			} else {
				marshaller.writeByte(0);
				marshaller.writeInt(intNum);
			}
		}

		public void readData(ObjectInputStream dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.moment = readDate(dis);

					this.pid = readString(dis);

					this.root_pid = readString(dis);

					this.father_pid = readString(dis);

					this.project = readString(dis);

					this.job = readString(dis);

					this.context = readString(dis);

					this.priority = readInteger(dis);

					this.type = readString(dis);

					this.origin = readString(dis);

					this.message = readString(dis);

					this.code = readInteger(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				}

			}

		}

		public void readData(org.jboss.marshalling.Unmarshaller dis) {

			synchronized (commonByteArrayLock_ECOMMERCEPOC_Orders_SQLServer_To_Snowflake) {

				try {

					int length = 0;

					this.moment = readDate(dis);

					this.pid = readString(dis);

					this.root_pid = readString(dis);

					this.father_pid = readString(dis);

					this.project = readString(dis);

					this.job = readString(dis);

					this.context = readString(dis);

					this.priority = readInteger(dis);

					this.type = readString(dis);

					this.origin = readString(dis);

					this.message = readString(dis);

					this.code = readInteger(dis);

				} catch (IOException e) {
					throw new RuntimeException(e);

				}

			}

		}

		public void writeData(ObjectOutputStream dos) {
			try {

				// java.util.Date

				writeDate(this.moment, dos);

				// String

				writeString(this.pid, dos);

				// String

				writeString(this.root_pid, dos);

				// String

				writeString(this.father_pid, dos);

				// String

				writeString(this.project, dos);

				// String

				writeString(this.job, dos);

				// String

				writeString(this.context, dos);

				// Integer

				writeInteger(this.priority, dos);

				// String

				writeString(this.type, dos);

				// String

				writeString(this.origin, dos);

				// String

				writeString(this.message, dos);

				// Integer

				writeInteger(this.code, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public void writeData(org.jboss.marshalling.Marshaller dos) {
			try {

				// java.util.Date

				writeDate(this.moment, dos);

				// String

				writeString(this.pid, dos);

				// String

				writeString(this.root_pid, dos);

				// String

				writeString(this.father_pid, dos);

				// String

				writeString(this.project, dos);

				// String

				writeString(this.job, dos);

				// String

				writeString(this.context, dos);

				// Integer

				writeInteger(this.priority, dos);

				// String

				writeString(this.type, dos);

				// String

				writeString(this.origin, dos);

				// String

				writeString(this.message, dos);

				// Integer

				writeInteger(this.code, dos);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");
			sb.append("moment=" + String.valueOf(moment));
			sb.append(",pid=" + pid);
			sb.append(",root_pid=" + root_pid);
			sb.append(",father_pid=" + father_pid);
			sb.append(",project=" + project);
			sb.append(",job=" + job);
			sb.append(",context=" + context);
			sb.append(",priority=" + String.valueOf(priority));
			sb.append(",type=" + type);
			sb.append(",origin=" + origin);
			sb.append(",message=" + message);
			sb.append(",code=" + String.valueOf(code));
			sb.append("]");

			return sb.toString();
		}

		public String toLogString() {
			StringBuilder sb = new StringBuilder();

			if (moment == null) {
				sb.append("<null>");
			} else {
				sb.append(moment);
			}

			sb.append("|");

			if (pid == null) {
				sb.append("<null>");
			} else {
				sb.append(pid);
			}

			sb.append("|");

			if (root_pid == null) {
				sb.append("<null>");
			} else {
				sb.append(root_pid);
			}

			sb.append("|");

			if (father_pid == null) {
				sb.append("<null>");
			} else {
				sb.append(father_pid);
			}

			sb.append("|");

			if (project == null) {
				sb.append("<null>");
			} else {
				sb.append(project);
			}

			sb.append("|");

			if (job == null) {
				sb.append("<null>");
			} else {
				sb.append(job);
			}

			sb.append("|");

			if (context == null) {
				sb.append("<null>");
			} else {
				sb.append(context);
			}

			sb.append("|");

			if (priority == null) {
				sb.append("<null>");
			} else {
				sb.append(priority);
			}

			sb.append("|");

			if (type == null) {
				sb.append("<null>");
			} else {
				sb.append(type);
			}

			sb.append("|");

			if (origin == null) {
				sb.append("<null>");
			} else {
				sb.append(origin);
			}

			sb.append("|");

			if (message == null) {
				sb.append("<null>");
			} else {
				sb.append(message);
			}

			sb.append("|");

			if (code == null) {
				sb.append("<null>");
			} else {
				sb.append(code);
			}

			sb.append("|");

			return sb.toString();
		}

		/**
		 * Compare keys
		 */
		public int compareTo(row2Struct other) {

			int returnValue = -1;

			return returnValue;
		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(), object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public void tLogCatcher_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
		globalMap.put("tLogCatcher_1_SUBPROCESS_STATE", 0);

		final boolean execStat = this.execStat;

		mdc("tLogCatcher_1", "6BqgvU_");

		String iterateId = "";

		String currentComponent = "";
		s("none");
		String cLabel = null;
		java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

		try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { // start the resume
				globalResumeTicket = true;

				row2Struct row2 = new row2Struct();

				/**
				 * [tLogRow_2 begin ] start
				 */

				sh("tLogRow_2");

				s(currentComponent = "tLogRow_2");

				runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, 0, 0, "row2");

				int tos_count_tLogRow_2 = 0;

				if (log.isDebugEnabled())
					log.debug("tLogRow_2 - " + ("Start to work."));
				if (log.isDebugEnabled()) {
					class BytesLimit65535_tLogRow_2 {
						public void limitLog4jByte() throws Exception {
							StringBuilder log4jParamters_tLogRow_2 = new StringBuilder();
							log4jParamters_tLogRow_2.append("Parameters:");
							log4jParamters_tLogRow_2.append("BASIC_MODE" + " = " + "true");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("TABLE_PRINT" + " = " + "false");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("VERTICAL" + " = " + "false");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("FIELDSEPARATOR" + " = " + "\"|\"");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("PRINT_HEADER" + " = " + "false");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("PRINT_UNIQUE_NAME" + " = " + "false");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("PRINT_COLNAMES" + " = " + "false");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("USE_FIXED_LENGTH" + " = " + "false");
							log4jParamters_tLogRow_2.append(" | ");
							log4jParamters_tLogRow_2.append("PRINT_CONTENT_WITH_LOG4J" + " = " + "true");
							log4jParamters_tLogRow_2.append(" | ");
							if (log.isDebugEnabled())
								log.debug("tLogRow_2 - " + (log4jParamters_tLogRow_2));
						}
					}
					new BytesLimit65535_tLogRow_2().limitLog4jByte();
				}
				if (enableLogStash) {
					talendJobLog.addCM("tLogRow_2", "tLogRow_2", "tLogRow");
					talendJobLogProcess(globalMap);
					s(currentComponent);
				}

				///////////////////////

				final String OUTPUT_FIELD_SEPARATOR_tLogRow_2 = "|";
				java.io.PrintStream consoleOut_tLogRow_2 = null;

				StringBuilder strBuffer_tLogRow_2 = null;
				int nb_line_tLogRow_2 = 0;
///////////////////////    			

				/**
				 * [tLogRow_2 begin ] stop
				 */

				/**
				 * [tLogCatcher_1 begin ] start
				 */

				sh("tLogCatcher_1");

				s(currentComponent = "tLogCatcher_1");

				int tos_count_tLogCatcher_1 = 0;

				if (log.isDebugEnabled())
					log.debug("tLogCatcher_1 - " + ("Start to work."));
				if (log.isDebugEnabled()) {
					class BytesLimit65535_tLogCatcher_1 {
						public void limitLog4jByte() throws Exception {
							StringBuilder log4jParamters_tLogCatcher_1 = new StringBuilder();
							log4jParamters_tLogCatcher_1.append("Parameters:");
							log4jParamters_tLogCatcher_1.append("CATCH_JAVA_EXCEPTION" + " = " + "true");
							log4jParamters_tLogCatcher_1.append(" | ");
							log4jParamters_tLogCatcher_1.append("CATCH_TDIE" + " = " + "true");
							log4jParamters_tLogCatcher_1.append(" | ");
							log4jParamters_tLogCatcher_1.append("CATCH_TWARN" + " = " + "true");
							log4jParamters_tLogCatcher_1.append(" | ");
							log4jParamters_tLogCatcher_1.append("CATCH_TACTIONFAILURE" + " = " + "true");
							log4jParamters_tLogCatcher_1.append(" | ");
							if (log.isDebugEnabled())
								log.debug("tLogCatcher_1 - " + (log4jParamters_tLogCatcher_1));
						}
					}
					new BytesLimit65535_tLogCatcher_1().limitLog4jByte();
				}
				if (enableLogStash) {
					talendJobLog.addCM("tLogCatcher_1", "tLogCatcher_1", "tLogCatcher");
					talendJobLogProcess(globalMap);
					s(currentComponent);
				}

				try {
					for (LogCatcherUtils.LogCatcherMessage lcm : tLogCatcher_1.getMessages()) {
						row2.type = lcm.getType();
						row2.origin = (lcm.getOrigin() == null || lcm.getOrigin().length() < 1 ? null
								: lcm.getOrigin());
						row2.priority = lcm.getPriority();
						row2.message = lcm.getMessage();
						row2.code = lcm.getCode();

						row2.moment = java.util.Calendar.getInstance().getTime();

						row2.pid = pid;
						row2.root_pid = rootPid;
						row2.father_pid = fatherPid;

						row2.project = projectName;
						row2.job = jobName;
						row2.context = contextStr;

						/**
						 * [tLogCatcher_1 begin ] stop
						 */

						/**
						 * [tLogCatcher_1 main ] start
						 */

						s(currentComponent = "tLogCatcher_1");

						tos_count_tLogCatcher_1++;

						/**
						 * [tLogCatcher_1 main ] stop
						 */

						/**
						 * [tLogCatcher_1 process_data_begin ] start
						 */

						s(currentComponent = "tLogCatcher_1");

						/**
						 * [tLogCatcher_1 process_data_begin ] stop
						 */

						/**
						 * [tLogRow_2 main ] start
						 */

						s(currentComponent = "tLogRow_2");

						if (runStat.update(execStat, enableLogStash, iterateId, 1, 1

								, "row2", "tLogCatcher_1", "tLogCatcher_1", "tLogCatcher", "tLogRow_2", "tLogRow_2",
								"tLogRow"

						)) {
							talendJobLogProcess(globalMap);
						}

						if (log.isTraceEnabled()) {
							log.trace("row2 - " + (row2 == null ? "" : row2.toLogString()));
						}

///////////////////////		

						strBuffer_tLogRow_2 = new StringBuilder();

						if (row2.moment != null) { //

							strBuffer_tLogRow_2.append(FormatterUtils.format_Date(row2.moment, "yyyy-MM-dd HH:mm:ss"));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.pid != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.pid));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.root_pid != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.root_pid));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.father_pid != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.father_pid));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.project != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.project));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.job != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.job));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.context != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.context));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.priority != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.priority));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.type != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.type));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.origin != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.origin));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.message != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.message));

						} //

						strBuffer_tLogRow_2.append("|");

						if (row2.code != null) { //

							strBuffer_tLogRow_2.append(String.valueOf(row2.code));

						} //

						if (globalMap.get("tLogRow_CONSOLE") != null) {
							consoleOut_tLogRow_2 = (java.io.PrintStream) globalMap.get("tLogRow_CONSOLE");
						} else {
							consoleOut_tLogRow_2 = new java.io.PrintStream(
									new java.io.BufferedOutputStream(System.out));
							globalMap.put("tLogRow_CONSOLE", consoleOut_tLogRow_2);
						}
						log.info("tLogRow_2 - Content of row " + (nb_line_tLogRow_2 + 1) + ": "
								+ strBuffer_tLogRow_2.toString());
						consoleOut_tLogRow_2.println(strBuffer_tLogRow_2.toString());
						consoleOut_tLogRow_2.flush();
						nb_line_tLogRow_2++;
//////

//////                    

///////////////////////    			

						tos_count_tLogRow_2++;

						/**
						 * [tLogRow_2 main ] stop
						 */

						/**
						 * [tLogRow_2 process_data_begin ] start
						 */

						s(currentComponent = "tLogRow_2");

						/**
						 * [tLogRow_2 process_data_begin ] stop
						 */

						/**
						 * [tLogRow_2 process_data_end ] start
						 */

						s(currentComponent = "tLogRow_2");

						/**
						 * [tLogRow_2 process_data_end ] stop
						 */

						/**
						 * [tLogCatcher_1 process_data_end ] start
						 */

						s(currentComponent = "tLogCatcher_1");

						/**
						 * [tLogCatcher_1 process_data_end ] stop
						 */

						/**
						 * [tLogCatcher_1 end ] start
						 */

						s(currentComponent = "tLogCatcher_1");

					}
				} catch (Exception e_tLogCatcher_1) {
					globalMap.put("tLogCatcher_1_ERROR_MESSAGE", e_tLogCatcher_1.getMessage());
					logIgnoredError(String.format(
							"tLogCatcher_1 - tLogCatcher failed to process log message(s) due to internal error: %s",
							e_tLogCatcher_1), e_tLogCatcher_1);
				}

				if (log.isDebugEnabled())
					log.debug("tLogCatcher_1 - " + ("Done."));

				ok_Hash.put("tLogCatcher_1", true);
				end_Hash.put("tLogCatcher_1", System.currentTimeMillis());

				/**
				 * [tLogCatcher_1 end ] stop
				 */

				/**
				 * [tLogRow_2 end ] start
				 */

				s(currentComponent = "tLogRow_2");

//////
//////
				globalMap.put("tLogRow_2_NB_LINE", nb_line_tLogRow_2);
				if (log.isInfoEnabled())
					log.info("tLogRow_2 - " + ("Printed row count: ") + (nb_line_tLogRow_2) + ("."));

///////////////////////    			

				if (runStat.updateStatAndLog(execStat, enableLogStash, resourceMap, iterateId, "row2", 2, 0,
						"tLogCatcher_1", "tLogCatcher_1", "tLogCatcher", "tLogRow_2", "tLogRow_2", "tLogRow",
						"output")) {
					talendJobLogProcess(globalMap);
				}

				if (log.isDebugEnabled())
					log.debug("tLogRow_2 - " + ("Done."));

				ok_Hash.put("tLogRow_2", true);
				end_Hash.put("tLogRow_2", System.currentTimeMillis());

				/**
				 * [tLogRow_2 end ] stop
				 */

			} // end the resume

			if (resumeEntryMethodName == null || globalResumeTicket) {
				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tLogCatcher_1:OnSubjobOk", "",
						Thread.currentThread().getId() + "", "", "", "", "", "");
			}

			if (execStat) {
				runStat.updateStatOnConnection("OnSubjobOk1", 0, "ok");
			}

			tDie_1Process(globalMap);

		} catch (java.lang.Exception e) {

			if (!(e instanceof TalendException) && !(e instanceof TDieException)) {
				log.fatal(currentComponent + " " + e.getMessage(), e);
			}

			TalendException te = new TalendException(e, currentComponent, cLabel, globalMap);

			throw te;
		} catch (java.lang.Error error) {

			runStat.stopThreadStat();

			throw error;
		} finally {

			try {

				/**
				 * [tLogCatcher_1 finally ] start
				 */

				s(currentComponent = "tLogCatcher_1");

				/**
				 * [tLogCatcher_1 finally ] stop
				 */

				/**
				 * [tLogRow_2 finally ] start
				 */

				s(currentComponent = "tLogRow_2");

				/**
				 * [tLogRow_2 finally ] stop
				 */

			} catch (java.lang.Exception e) {
				// ignore
			} catch (java.lang.Error error) {
				// ignore
			}
			resourceMap = null;
		}

		globalMap.put("tLogCatcher_1_SUBPROCESS_STATE", 1);
	}

	public void tDie_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
		globalMap.put("tDie_1_SUBPROCESS_STATE", 0);

		final boolean execStat = this.execStat;

		mdc("tDie_1", "OoyfjY_");

		String iterateId = "";

		String currentComponent = "";
		s("none");
		String cLabel = null;
		java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

		try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { // start the resume
				globalResumeTicket = true;

				/**
				 * [tDie_1 begin ] start
				 */

				sh("tDie_1");

				s(currentComponent = "tDie_1");

				int tos_count_tDie_1 = 0;

				if (log.isDebugEnabled())
					log.debug("tDie_1 - " + ("Start to work."));
				if (log.isDebugEnabled()) {
					class BytesLimit65535_tDie_1 {
						public void limitLog4jByte() throws Exception {
							StringBuilder log4jParamters_tDie_1 = new StringBuilder();
							log4jParamters_tDie_1.append("Parameters:");
							log4jParamters_tDie_1.append("MESSAGE" + " = " + "\"the end is near\"");
							log4jParamters_tDie_1.append(" | ");
							log4jParamters_tDie_1.append("CODE" + " = " + "4");
							log4jParamters_tDie_1.append(" | ");
							log4jParamters_tDie_1.append("PRIORITY" + " = " + "5");
							log4jParamters_tDie_1.append(" | ");
							log4jParamters_tDie_1.append("EXIT_JVM" + " = " + "false");
							log4jParamters_tDie_1.append(" | ");
							if (log.isDebugEnabled())
								log.debug("tDie_1 - " + (log4jParamters_tDie_1));
						}
					}
					new BytesLimit65535_tDie_1().limitLog4jByte();
				}
				if (enableLogStash) {
					talendJobLog.addCM("tDie_1", "tDie_1", "tDie");
					talendJobLogProcess(globalMap);
					s(currentComponent);
				}

				/**
				 * [tDie_1 begin ] stop
				 */

				/**
				 * [tDie_1 main ] start
				 */

				s(currentComponent = "tDie_1");

				try {
					tLogCatcher_1.addMessage("tDie", "tDie_1", 5, "the end is near", 4);
					tLogCatcher_1Process(globalMap);

					globalMap.put("tDie_1_DIE_PRIORITY", 5);
					System.err.println("the end is near");

					globalMap.put("tDie_1_DIE_MESSAGE", "the end is near");
					globalMap.put("tDie_1_DIE_MESSAGES", "the end is near");

				} catch (Exception | Error e_tDie_1) {
					globalMap.put("tDie_1_ERROR_MESSAGE", e_tDie_1.getMessage());
					logIgnoredError(
							String.format("tDie_1 - tDie failed to log message due to internal error: %s", e_tDie_1),
							e_tDie_1);
				}

				currentComponent = "tDie_1";
				status = "failure";
				errorCode = new Integer(4);
				globalMap.put("tDie_1_DIE_CODE", errorCode);

				TDieException e_tDie_1 = new TDieException("the end is near");
				String errorMessageTDie_tDie_1 = "tDie_1 - The die message: " + "the end is near";

				log.error(errorMessageTDie_tDie_1, e_tDie_1);

				if (true) {

					if (enableLogStash) {
						talendJobLog.addJobExceptionMessage(currentComponent, cLabel, "the end is near", e_tDie_1);
						talendJobLogProcess(globalMap);
					}

					throw e_tDie_1;
				}

				tos_count_tDie_1++;

				/**
				 * [tDie_1 main ] stop
				 */

				/**
				 * [tDie_1 process_data_begin ] start
				 */

				s(currentComponent = "tDie_1");

				/**
				 * [tDie_1 process_data_begin ] stop
				 */

				/**
				 * [tDie_1 process_data_end ] start
				 */

				s(currentComponent = "tDie_1");

				/**
				 * [tDie_1 process_data_end ] stop
				 */

				/**
				 * [tDie_1 end ] start
				 */

				s(currentComponent = "tDie_1");

				if (log.isDebugEnabled())
					log.debug("tDie_1 - " + ("Done."));

				ok_Hash.put("tDie_1", true);
				end_Hash.put("tDie_1", System.currentTimeMillis());

				/**
				 * [tDie_1 end ] stop
				 */

			} // end the resume

		} catch (java.lang.Exception e) {

			if (!(e instanceof TalendException) && !(e instanceof TDieException)) {
				log.fatal(currentComponent + " " + e.getMessage(), e);
			}

			TalendException te = new TalendException(e, currentComponent, cLabel, globalMap);

			throw te;
		} catch (java.lang.Error error) {

			runStat.stopThreadStat();

			throw error;
		} finally {

			try {

				/**
				 * [tDie_1 finally ] start
				 */

				s(currentComponent = "tDie_1");

				/**
				 * [tDie_1 finally ] stop
				 */

			} catch (java.lang.Exception e) {
				// ignore
			} catch (java.lang.Error error) {
				// ignore
			}
			resourceMap = null;
		}

		globalMap.put("tDie_1_SUBPROCESS_STATE", 1);
	}

	public void talendJobLogProcess(final java.util.Map<String, Object> globalMap) throws TalendException {
		globalMap.put("talendJobLog_SUBPROCESS_STATE", 0);

		final boolean execStat = this.execStat;

		String iterateId = "";

		String currentComponent = "";
		s("none");
		String cLabel = null;
		java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

		try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { // start the resume
				globalResumeTicket = true;

				/**
				 * [talendJobLog begin ] start
				 */

				sh("talendJobLog");

				s(currentComponent = "talendJobLog");

				int tos_count_talendJobLog = 0;

				for (JobStructureCatcherUtils.JobStructureCatcherMessage jcm : talendJobLog.getMessages()) {
					org.talend.job.audit.JobContextBuilder builder_talendJobLog = org.talend.job.audit.JobContextBuilder
							.create().jobName(jcm.job_name).jobId(jcm.job_id).jobVersion(jcm.job_version)
							.custom("process_id", jcm.pid).custom("thread_id", jcm.tid).custom("pid", pid)
							.custom("father_pid", fatherPid).custom("root_pid", rootPid);
					org.talend.logging.audit.Context log_context_talendJobLog = null;

					if (jcm.log_type == JobStructureCatcherUtils.LogType.PERFORMANCE) {
						long timeMS = jcm.end_time - jcm.start_time;
						String duration = String.valueOf(timeMS);

						log_context_talendJobLog = builder_talendJobLog.sourceId(jcm.sourceId)
								.sourceLabel(jcm.sourceLabel).sourceConnectorType(jcm.sourceComponentName)
								.targetId(jcm.targetId).targetLabel(jcm.targetLabel)
								.targetConnectorType(jcm.targetComponentName).connectionName(jcm.current_connector)
								.rows(jcm.row_count).duration(duration).build();
						auditLogger_talendJobLog.flowExecution(log_context_talendJobLog);
					} else if (jcm.log_type == JobStructureCatcherUtils.LogType.JOBSTART) {
						log_context_talendJobLog = builder_talendJobLog.timestamp(jcm.moment).build();
						auditLogger_talendJobLog.jobstart(log_context_talendJobLog);
					} else if (jcm.log_type == JobStructureCatcherUtils.LogType.JOBEND) {
						long timeMS = jcm.end_time - jcm.start_time;
						String duration = String.valueOf(timeMS);

						log_context_talendJobLog = builder_talendJobLog.timestamp(jcm.moment).duration(duration)
								.status(jcm.status).build();
						auditLogger_talendJobLog.jobstop(log_context_talendJobLog);
					} else if (jcm.log_type == JobStructureCatcherUtils.LogType.RUNCOMPONENT) {
						log_context_talendJobLog = builder_talendJobLog.timestamp(jcm.moment)
								.connectorType(jcm.component_name).connectorId(jcm.component_id)
								.connectorLabel(jcm.component_label).build();
						auditLogger_talendJobLog.runcomponent(log_context_talendJobLog);
					} else if (jcm.log_type == JobStructureCatcherUtils.LogType.FLOWINPUT) {// log current component
																							// input line
						long timeMS = jcm.end_time - jcm.start_time;
						String duration = String.valueOf(timeMS);

						log_context_talendJobLog = builder_talendJobLog.connectorType(jcm.component_name)
								.connectorId(jcm.component_id).connectorLabel(jcm.component_label)
								.connectionName(jcm.current_connector).connectionType(jcm.current_connector_type)
								.rows(jcm.total_row_number).duration(duration).build();
						auditLogger_talendJobLog.flowInput(log_context_talendJobLog);
					} else if (jcm.log_type == JobStructureCatcherUtils.LogType.FLOWOUTPUT) {// log current component
																								// output/reject line
						long timeMS = jcm.end_time - jcm.start_time;
						String duration = String.valueOf(timeMS);

						log_context_talendJobLog = builder_talendJobLog.connectorType(jcm.component_name)
								.connectorId(jcm.component_id).connectorLabel(jcm.component_label)
								.connectionName(jcm.current_connector).connectionType(jcm.current_connector_type)
								.rows(jcm.total_row_number).duration(duration).build();
						auditLogger_talendJobLog.flowOutput(log_context_talendJobLog);
					} else if (jcm.log_type == JobStructureCatcherUtils.LogType.JOBERROR) {
						java.lang.Exception e_talendJobLog = jcm.exception;
						if (e_talendJobLog != null) {
							try (java.io.StringWriter sw_talendJobLog = new java.io.StringWriter();
									java.io.PrintWriter pw_talendJobLog = new java.io.PrintWriter(sw_talendJobLog)) {
								e_talendJobLog.printStackTrace(pw_talendJobLog);
								builder_talendJobLog.custom("stacktrace", sw_talendJobLog.getBuffer().substring(0,
										java.lang.Math.min(sw_talendJobLog.getBuffer().length(), 512)));
							}
						}

						if (jcm.extra_info != null) {
							builder_talendJobLog.connectorId(jcm.component_id).custom("extra_info", jcm.extra_info);
						}

						log_context_talendJobLog = builder_talendJobLog
								.connectorType(jcm.component_id.substring(0, jcm.component_id.lastIndexOf('_')))
								.connectorId(jcm.component_id)
								.connectorLabel(jcm.component_label == null ? jcm.component_id : jcm.component_label)
								.build();

						auditLogger_talendJobLog.exception(log_context_talendJobLog);
					}

				}

				/**
				 * [talendJobLog begin ] stop
				 */

				/**
				 * [talendJobLog main ] start
				 */

				s(currentComponent = "talendJobLog");

				tos_count_talendJobLog++;

				/**
				 * [talendJobLog main ] stop
				 */

				/**
				 * [talendJobLog process_data_begin ] start
				 */

				s(currentComponent = "talendJobLog");

				/**
				 * [talendJobLog process_data_begin ] stop
				 */

				/**
				 * [talendJobLog process_data_end ] start
				 */

				s(currentComponent = "talendJobLog");

				/**
				 * [talendJobLog process_data_end ] stop
				 */

				/**
				 * [talendJobLog end ] start
				 */

				s(currentComponent = "talendJobLog");

				ok_Hash.put("talendJobLog", true);
				end_Hash.put("talendJobLog", System.currentTimeMillis());

				/**
				 * [talendJobLog end ] stop
				 */

			} // end the resume

		} catch (java.lang.Exception e) {

			if (!(e instanceof TalendException) && !(e instanceof TDieException)) {
				log.fatal(currentComponent + " " + e.getMessage(), e);
			}

			TalendException te = new TalendException(e, currentComponent, cLabel, globalMap);

			throw te;
		} catch (java.lang.Error error) {

			runStat.stopThreadStat();

			throw error;
		} finally {

			try {

				/**
				 * [talendJobLog finally ] start
				 */

				s(currentComponent = "talendJobLog");

				/**
				 * [talendJobLog finally ] stop
				 */

			} catch (java.lang.Exception e) {
				// ignore
			} catch (java.lang.Error error) {
				// ignore
			}
			resourceMap = null;
		}

		globalMap.put("talendJobLog_SUBPROCESS_STATE", 1);
	}

	public String resuming_logs_dir_path = null;
	public String resuming_checkpoint_path = null;
	public String parent_part_launcher = null;
	private String resumeEntryMethodName = null;
	private boolean globalResumeTicket = false;

	public boolean watch = false;
	// portStats is null, it means don't execute the statistics
	public Integer portStats = null;
	public int portTraces = 4334;
	public String clientHost;
	public String defaultClientHost = "localhost";
	public String contextStr = "Default";
	public boolean isDefaultContext = true;
	public String pid = "0";
	public String rootPid = null;
	public String fatherPid = null;
	public String fatherNode = null;
	public long startTime = 0;
	public boolean isChildJob = false;
	public String log4jLevel = "";

	private boolean enableLogStash;
	private boolean enableLineage;

	private boolean execStat = true;

	private ThreadLocal<java.util.Map<String, String>> threadLocal = new ThreadLocal<java.util.Map<String, String>>() {
		protected java.util.Map<String, String> initialValue() {
			java.util.Map<String, String> threadRunResultMap = new java.util.HashMap<String, String>();
			threadRunResultMap.put("errorCode", null);
			threadRunResultMap.put("status", "");
			return threadRunResultMap;
		};
	};

	protected PropertiesWithType context_param = new PropertiesWithType();
	public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

	public String status = "";

	private final static java.util.Properties jobInfo = new java.util.Properties();
	private final static java.util.Map<String, String> mdcInfo = new java.util.HashMap<>();
	private final static java.util.concurrent.atomic.AtomicLong subJobPidCounter = new java.util.concurrent.atomic.AtomicLong();

	public static void main(String[] args) {
		final Orders_SQLServer_To_Snowflake Orders_SQLServer_To_SnowflakeClass = new Orders_SQLServer_To_Snowflake();
		int exitCode = Orders_SQLServer_To_SnowflakeClass.runJobInTOS(args);
		if (exitCode == 0) {
			log.info("TalendJob: 'Orders_SQLServer_To_Snowflake' - Done.");
		}

		System.exit(exitCode);

	}

	private void getjobInfo() {
		final String TEMPLATE_PATH = "src/main/templates/jobInfo_template.properties";
		final String BUILD_PATH = "../jobInfo.properties";
		final String path = this.getClass().getResource("").getPath();
		if (path.lastIndexOf("target") > 0) {
			final java.io.File templateFile = new java.io.File(
					path.substring(0, path.lastIndexOf("target")).concat(TEMPLATE_PATH));
			if (templateFile.exists()) {
				readJobInfo(templateFile);
				return;
			}
		}
		readJobInfo(new java.io.File(BUILD_PATH));
	}

	private void readJobInfo(java.io.File jobInfoFile) {

		if (jobInfoFile.exists()) {
			try (java.io.InputStream is = new java.io.FileInputStream(jobInfoFile)) {
				jobInfo.load(is);
			} catch (IOException e) {

				log.debug("Read jobInfo.properties file fail: " + e.getMessage());

			}
		}
		log.info(String.format("Project name: %s\tJob name: %s\tGIT Commit ID: %s\tTalend Version: %s", projectName,
				jobName, jobInfo.getProperty("gitCommitId"), "8.0.1.20260724_0953-patch"));

	}

	public String[][] runJob(String[] args) {

		int exitCode = runJobInTOS(args);
		String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };

		return bufferValue;
	}

	public boolean hastBufferOutputComponent() {
		boolean hastBufferOutput = false;

		return hastBufferOutput;
	}

	public int runJobInTOS(String[] args) {
		// reset status
		status = "";

		String lastStr = "";
		for (String arg : args) {
			if (arg.equalsIgnoreCase("--context_param")) {
				lastStr = arg;
			} else if (lastStr.equals("")) {
				evalParam(arg);
			} else {
				evalParam(lastStr + " " + arg);
				lastStr = "";
			}
		}

		enableLogStash = "true".equalsIgnoreCase(System.getProperty("audit.enabled"));

		if (!"".equals(log4jLevel)) {

			if ("trace".equalsIgnoreCase(log4jLevel)) {
				org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
						org.apache.logging.log4j.Level.TRACE);
			} else if ("debug".equalsIgnoreCase(log4jLevel)) {
				org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
						org.apache.logging.log4j.Level.DEBUG);
			} else if ("info".equalsIgnoreCase(log4jLevel)) {
				org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
						org.apache.logging.log4j.Level.INFO);
			} else if ("warn".equalsIgnoreCase(log4jLevel)) {
				org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
						org.apache.logging.log4j.Level.WARN);
			} else if ("error".equalsIgnoreCase(log4jLevel)) {
				org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
						org.apache.logging.log4j.Level.ERROR);
			} else if ("fatal".equalsIgnoreCase(log4jLevel)) {
				org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
						org.apache.logging.log4j.Level.FATAL);
			} else if ("off".equalsIgnoreCase(log4jLevel)) {
				org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(),
						org.apache.logging.log4j.Level.OFF);
			}
			org.apache.logging.log4j.core.config.Configurator
					.setLevel(org.apache.logging.log4j.LogManager.getRootLogger().getName(), log.getLevel());
		}

		getjobInfo();

		log.info("TalendJob: 'Orders_SQLServer_To_Snowflake' - Start.");

		java.util.Set<Object> jobInfoKeys = jobInfo.keySet();
		for (Object jobInfoKey : jobInfoKeys) {
			org.slf4j.MDC.put("_" + jobInfoKey.toString(), jobInfo.get(jobInfoKey).toString());
		}
		org.slf4j.MDC.put("_pid", pid);
		org.slf4j.MDC.put("_rootPid", rootPid);
		org.slf4j.MDC.put("_fatherPid", fatherPid);
		org.slf4j.MDC.put("_projectName", projectName);
		org.slf4j.MDC.put("_startTimestamp", java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC)
				.format(java.time.format.DateTimeFormatter.ISO_INSTANT));
		org.slf4j.MDC.put("_jobRepositoryId", "_MbDe4JpnEfGJXNU8GVdb3A");
		org.slf4j.MDC.put("_compiledAtTimestamp", "2026-08-18T06:57:58.076046800Z");

		java.lang.management.RuntimeMXBean mx = java.lang.management.ManagementFactory.getRuntimeMXBean();
		String[] mxNameTable = mx.getName().split("@"); //$NON-NLS-1$
		if (mxNameTable.length == 2) {
			org.slf4j.MDC.put("_systemPid", mxNameTable[0]);
		} else {
			org.slf4j.MDC.put("_systemPid", String.valueOf(java.lang.Thread.currentThread().getId()));
		}

		final boolean enableCBP = false;
		boolean inOSGi = routines.system.BundleUtils.inOSGi();

		boolean needSendForCBP = false;
		if (!inOSGi) {
			if (org.talend.metrics.CBPClient.getInstanceForCurrentVM() == null) {
				try {
					needSendForCBP = true;

					String jobType = "DI";
					boolean isRooutJob = fatherPid == null ? true : false;
					if (isStandaloneMS || inOSGi) {
						jobType = "DATA_SERVICE";
						// for APPINT cases everything is packaged as single job
						isRooutJob = true;
					} else {
						// print only for DI job for microservice it is already printed
						// for OSGI runtime will take care
						if (jobInfo.isEmpty()) {
							getjobInfo();
						}
					}
					org.talend.metrics.CBPClient.startListenIfNotStarted(enableCBP, isRooutJob, jobType,
							jobInfo.getProperty("artifactId"), true, jobInfo.getProperty("cmdLineVersion"),
							jobInfo.getProperty("patch"), jobInfo.getProperty("date"));
				} catch (java.lang.Exception e) {
					errorCode = 1;
					status = "failure";
					e.printStackTrace();
					return 1;
				}
			}
		}

		if (System.getProperty("tmc.task.payload.path") != null) {
			log.warn("Lineage-producer - The Job artifact do not support dataset and lineage generation.");
		}
		if (enableLogStash) {
			java.util.Properties properties_talendJobLog = new java.util.Properties();
			properties_talendJobLog.setProperty("root.logger", "audit");
			properties_talendJobLog.setProperty("encoding", "UTF-8");
			properties_talendJobLog.setProperty("application.name", "Talend Studio");
			properties_talendJobLog.setProperty("service.name", "Talend Studio Job");
			properties_talendJobLog.setProperty("instance.name", "Talend Studio Job Instance");
			properties_talendJobLog.setProperty("propagate.appender.exceptions", "none");
			properties_talendJobLog.setProperty("log.appender", "file");
			properties_talendJobLog.setProperty("appender.file.path", "audit.json");
			properties_talendJobLog.setProperty("appender.file.maxsize", "52428800");
			properties_talendJobLog.setProperty("appender.file.maxbackup", "20");
			properties_talendJobLog.setProperty("host", "false");

			System.getProperties().stringPropertyNames().stream().filter(it -> it.startsWith("audit.logger."))
					.forEach(key -> properties_talendJobLog.setProperty(key.substring("audit.logger.".length()),
							System.getProperty(key)));

			org.apache.logging.log4j.core.config.Configurator
					.setLevel(properties_talendJobLog.getProperty("root.logger"), org.apache.logging.log4j.Level.DEBUG);

			auditLogger_talendJobLog = org.talend.job.audit.JobEventAuditLoggerFactory
					.createJobAuditLogger(properties_talendJobLog);
		}

		if (clientHost == null) {
			clientHost = defaultClientHost;
		}

		if (pid == null || "0".equals(pid)) {
			pid = TalendString.getAsciiRandomString(6);
		}

		org.slf4j.MDC.put("_pid", pid);

		if (rootPid == null) {
			rootPid = pid;
		}

		org.slf4j.MDC.put("_rootPid", rootPid);

		if (fatherPid == null) {
			fatherPid = pid;
		} else {
			isChildJob = true;
		}
		org.slf4j.MDC.put("_fatherPid", fatherPid);

		if (portStats != null) {
			// portStats = -1; //for testing
			if (portStats < 0 || portStats > 65535) {
				// issue:10869, the portStats is invalid, so this client socket can't open
				System.err.println("The statistics socket port " + portStats + " is invalid.");
				execStat = false;
			}
		} else {
			execStat = false;
		}

		java.util.Dictionary<String, Object> jobProperties = null;
		try {
			if (inOSGi) {
				jobProperties = routines.system.BundleUtils.getJobProperties(jobName);

				if (jobProperties != null && jobProperties.get("context") != null) {
					contextStr = (String) jobProperties.get("context");
				}

				// extract params from caller
				String taskExecutionIdForSOAP = null;
				String jobExecutionIdForSOAP = null;
				boolean skipCBPAuth = false;
				boolean isSoapService = false;
				for (String arg : args) {
					// for SOAP service, the taskExecutionId and jobExecutionId are passed in as
					// context params from Talend Runtime
					if (arg.startsWith("--context_param")
							&& (arg.contains("taskExecutionId") || arg.contains("jobExecutionId"))) {
						String keyValue = arg.replace("--context_param", "");
						String[] parts = keyValue.split("=");
						String[] cleanParts = java.util.Arrays.stream(parts).filter(s -> !s.isEmpty())
								.toArray(String[]::new);
						if (cleanParts.length == 2) {
							String key = cleanParts[0];
							String value = cleanParts[1];
							if ("taskExecutionId".equals(key.trim()) && null != value) {
								taskExecutionIdForSOAP = value.trim();
							} else if ("jobExecutionId".equals(key.trim()) && null != value) {
								jobExecutionIdForSOAP = value.trim();
							}
						}
					} else if (arg.equalsIgnoreCase("--sealCounters=true")) {
						sealCounters = true;
					} else if (arg.equalsIgnoreCase("--skipCBPAuth=true")) {
						skipCBPAuth = true;
					} else if (arg.equalsIgnoreCase("--isSOAPService=true")) {
						isSoapService = true;
						skipCBPAuth = true;
					}
				}
			}

			// first load default key-value pairs from application.properties
			if (isStandaloneMS) {
				context.putAll(this.getDefaultProperties());
			}
			// call job/subjob with an existing context, like: --context=production. if
			// without this parameter, there will use the default context instead.
			java.io.InputStream inContext = Orders_SQLServer_To_Snowflake.class.getClassLoader().getResourceAsStream(
					"ecommercepoc/orders_sqlserver_to_snowflake_0_1/contexts/" + contextStr + ".properties");
			if (inContext == null) {
				inContext = Orders_SQLServer_To_Snowflake.class.getClassLoader()
						.getResourceAsStream("config/contexts/" + contextStr + ".properties");
			}
			if (inContext != null) {
				try {
					// defaultProps is in order to keep the original context value
					if (context != null && context.isEmpty()) {
						defaultProps.load(inContext);
						if (inOSGi && jobProperties != null) {
							java.util.Enumeration<String> keys = jobProperties.keys();
							while (keys.hasMoreElements()) {
								String propKey = keys.nextElement();
								if (defaultProps.containsKey(propKey)) {
									defaultProps.put(propKey, (String) jobProperties.get(propKey));
								}
							}
						}
						context = new ContextProperties(defaultProps);
					}
					if (isStandaloneMS) {
						// override context key-value pairs if provided using --context=contextName
						defaultProps.load(inContext);
						context.putAll(defaultProps);
					}
				} finally {
					inContext.close();
				}
			} else if (!isDefaultContext) {
				// print info and job continue to run, for case: context_param is not empty.
				System.err.println("Could not find the context " + contextStr);
			}
			// override key-value pairs if provided via --config.location=file1.file2 OR
			// --config.additional-location=file1,file2
			if (isStandaloneMS) {
				context.putAll(this.getAdditionalProperties());
			}

			// override key-value pairs if provide via command line like
			// --key1=value1,--key2=value2
			if (!context_param.isEmpty()) {
				context.putAll(context_param);
				// set types for params from parentJobs
				for (Object key : context_param.keySet()) {
					String context_key = key.toString();
					String context_type = context_param.getContextType(context_key);
					context.setContextType(context_key, context_type);

				}
			}

			class ContextProcessing {
				private void processContext_0() {
				}

				public void processAllContext() {
					processContext_0();
				}
			}

			new ContextProcessing().processAllContext();
		} catch (java.io.IOException ie) {
			System.err.println("Could not load context " + contextStr);
			ie.printStackTrace();
		}

		// get context value from parent directly
		if (parentContextMap != null && !parentContextMap.isEmpty()) {
		}

		// Resume: init the resumeUtil
		resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);
		resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);
		resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);

		List<String> parametersToEncrypt = new java.util.ArrayList<String>();
		// Resume: jobStart
		resumeUtil.addLog("JOB_STARTED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "",
				"", "", "", "", ResumeUtil.convertToJsonText(context, ContextProperties.class, parametersToEncrypt));

		org.slf4j.MDC.put("_context", contextStr);
		log.info("TalendJob: 'Orders_SQLServer_To_Snowflake' - Started.");
		java.util.Optional.ofNullable(org.slf4j.MDC.getCopyOfContextMap()).ifPresent(mdcInfo::putAll);

		if (execStat) {
			try {
				runStat.openSocket(!isChildJob);
				runStat.setAllPID(rootPid, fatherPid, pid, jobName);
				runStat.startThreadStat(clientHost, portStats);
				runStat.updateStatOnJob(RunStat.JOBSTART, fatherNode);
			} catch (java.io.IOException ioException) {
				ioException.printStackTrace();
			}
		}

		java.util.concurrent.ConcurrentHashMap<Object, Object> concurrentHashMap = new java.util.concurrent.ConcurrentHashMap<Object, Object>();
		globalMap.put("concurrentHashMap", concurrentHashMap);

		long startUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long endUsedMemory = 0;
		long end = 0;

		startTime = System.currentTimeMillis();

		this.globalResumeTicket = true;// to run tPreJob

		if (enableLogStash) {
			talendJobLog.addJobStartMessage();
			try {
				talendJobLogProcess(globalMap);
			} catch (java.lang.Exception e) {
				e.printStackTrace();
			}
		}

		this.globalResumeTicket = false;// to run others jobs

		try {
			errorCode = null;
			tDBInput_1Process(globalMap);
			if (!"failure".equals(status)) {
				status = "end";
			}
		} catch (TalendException e_tDBInput_1) {
			globalMap.put("tDBInput_1_SUBPROCESS_STATE", -1);

			e_tDBInput_1.printStackTrace();

		}

		this.globalResumeTicket = true;// to run tPostJob

		end = System.currentTimeMillis();

		if (watch) {
			System.out.println((end - startTime) + " milliseconds");
		}

		endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		if (false) {
			System.out.println((endUsedMemory - startUsedMemory)
					+ " bytes memory increase when running : Orders_SQLServer_To_Snowflake");
		}
		if (enableLogStash) {
			talendJobLog.addJobEndMessage(startTime, end, status);
			try {
				talendJobLogProcess(globalMap);
			} catch (java.lang.Exception e) {
				e.printStackTrace();
			}
		}

		if (execStat) {
			runStat.updateStatOnJob(RunStat.JOBEND, fatherNode);
			runStat.stopThreadStat();
		}

		int returnCode = 0;

		if (!inOSGi) {
			if (needSendForCBP && org.talend.metrics.CBPClient.getInstanceForCurrentVM() != null) {
				s("none");
				if (!isChildJob) {
					org.talend.metrics.CBPClient.getInstanceForCurrentVM().setStatus(status);
				}
				org.talend.metrics.CBPClient.getInstanceForCurrentVM().sendData();
			}
		}

		if (errorCode == null) {
			returnCode = status != null && status.equals("failure") ? 1 : 0;
		} else {
			returnCode = errorCode.intValue();
		}
		resumeUtil.addLog("JOB_ENDED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "",
				"" + returnCode, "", "", "");
		resumeUtil.flush();

		org.slf4j.MDC.remove("_subJobName");
		org.slf4j.MDC.remove("_subJobPid");
		org.slf4j.MDC.remove("_systemPid");
		log.info("TalendJob: 'Orders_SQLServer_To_Snowflake' - Finished - status: " + status + " returnCode: "
				+ returnCode);

		return returnCode;

	}

	// only for OSGi env
	public void destroy() {

		// add CBP code for OSGI Executions
		if (sealCounters && jobExecutionId != null) {
			// call to checkAuthorization was made or was being made
			try {
				org.talend.metrics.DataReadTracker.setExecutionId(taskExecutionId, jobExecutionId, false);
				org.talend.metrics.DataReadTracker.sealCounter();
				org.talend.metrics.DataReadTracker.reset();
			} catch (Exception | NoClassDefFoundError e) {
				// ignore
			}
		}

		// check for orphan threads if still alive after undeploy
		for (java.util.Iterator<Thread> it = threadList.iterator(); it.hasNext();) {
			Thread thread = it.next();
			if (thread != null && thread.isAlive()) {
				boolean suppressLogs = Boolean
						.parseBoolean(System.getProperty("thread.terminate.logs.suppress", "false"));
				if (!suppressLogs) {
					System.err.println(
							"Initiating thread cleanup prior to bundle undeployment. This is a precautionary step to ensure no memory leaks.");
					System.err.println("Forcefully interrupting thread with ID = " + thread.getId()
							+ ". This may result in expected errors due to abrupt termination. Please verify if the thread was performing critical operations.");
				}
				thread.interrupt();
			}
			it.remove();
		}
		// end of destroy()
	}

	private java.util.Map<String, Object> getSharedConnections4REST() {
		java.util.Map<String, Object> connections = new java.util.HashMap<String, Object>();

		return connections;
	}

	private void evalParam(String arg) {
		if (arg.startsWith("--resuming_logs_dir_path")) {
			resuming_logs_dir_path = arg.substring(25);
		} else if (arg.startsWith("--resuming_checkpoint_path")) {
			resuming_checkpoint_path = arg.substring(27);
		} else if (arg.startsWith("--parent_part_launcher")) {
			parent_part_launcher = arg.substring(23);
		} else if (arg.startsWith("--watch")) {
			watch = true;
		} else if (arg.startsWith("--stat_port=")) {
			String portStatsStr = arg.substring(12);
			if (portStatsStr != null && !portStatsStr.equals("null")) {
				portStats = Integer.parseInt(portStatsStr);
			}
		} else if (arg.startsWith("--trace_port=")) {
			portTraces = Integer.parseInt(arg.substring(13));
		} else if (arg.startsWith("--client_host=")) {
			clientHost = arg.substring(14);
		} else if (arg.startsWith("--context=")) {
			contextStr = arg.substring(10);
			isDefaultContext = false;
		} else if (arg.startsWith("--father_pid=")) {
			fatherPid = arg.substring(13);
		} else if (arg.startsWith("--root_pid=")) {
			rootPid = arg.substring(11);
		} else if (arg.startsWith("--father_node=")) {
			fatherNode = arg.substring(14);
		} else if (arg.startsWith("--pid=")) {
			pid = arg.substring(6);
		} else if (arg.startsWith("--context_type")) {
			String keyValue = arg.substring(15);
			int index = -1;
			if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
				if (fatherPid == null) {
					context_param.setContextType(keyValue.substring(0, index),
							replaceEscapeChars(keyValue.substring(index + 1)));
				} else { // the subjob won't escape the especial chars
					context_param.setContextType(keyValue.substring(0, index), keyValue.substring(index + 1));
				}

			}

		} else if (arg.startsWith("--context_param")) {
			String keyValue = arg.substring(16);
			int index = -1;
			if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
				if (fatherPid == null) {
					context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
				} else { // the subjob won't escape the especial chars
					context_param.put(keyValue.substring(0, index), keyValue.substring(index + 1));
				}
			}
		} else if (arg.startsWith("--context_file")) {
			String keyValue = arg.substring(15);
			String filePath = new String(java.util.Base64.getDecoder().decode(keyValue));
			java.nio.file.Path contextFile = java.nio.file.Paths.get(filePath);
			try (java.io.BufferedReader reader = java.nio.file.Files.newBufferedReader(contextFile)) {
				String line;
				while ((line = reader.readLine()) != null) {
					int index = -1;
					if ((index = line.indexOf('=')) > -1) {
						if (line.startsWith("--context_param")) {
							if ("id_Password".equals(context_param.getContextType(line.substring(16, index)))) {
								context_param.put(line.substring(16, index),
										routines.system.PasswordEncryptUtil.decryptPassword(line.substring(index + 1)));
							} else {
								context_param.put(line.substring(16, index), line.substring(index + 1));
							}
						} else {// --context_type
							context_param.setContextType(line.substring(15, index), line.substring(index + 1));
						}
					}
				}
			} catch (java.io.IOException e) {
				System.err.println("Could not load the context file: " + filePath);
				e.printStackTrace();
			}
		} else if (arg.startsWith("--log4jLevel=")) {
			log4jLevel = arg.substring(13);
		} else if (arg.startsWith("--audit.enabled") && arg.contains("=")) {// for trunjob call
			final int equal = arg.indexOf('=');
			final String key = arg.substring("--".length(), equal);
			System.setProperty(key, arg.substring(equal + 1));
		}
	}

	private static final String NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY = "<TALEND_NULL>";

	private final String[][] escapeChars = { { "\\\\", "\\" }, { "\\n", "\n" }, { "\\'", "\'" }, { "\\r", "\r" },
			{ "\\f", "\f" }, { "\\b", "\b" }, { "\\t", "\t" } };

	private String replaceEscapeChars(String keyValue) {

		if (keyValue == null || ("").equals(keyValue.trim())) {
			return keyValue;
		}

		StringBuilder result = new StringBuilder();
		int currIndex = 0;
		while (currIndex < keyValue.length()) {
			int index = -1;
			// judege if the left string includes escape chars
			for (String[] strArray : escapeChars) {
				index = keyValue.indexOf(strArray[0], currIndex);
				if (index >= 0) {

					result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0],
							strArray[1]));
					currIndex = index + strArray[0].length();
					break;
				}
			}
			// if the left string doesn't include escape chars, append the left into the
			// result
			if (index < 0) {
				result.append(keyValue.substring(currIndex));
				currIndex = currIndex + keyValue.length();
			}
		}

		return result.toString();
	}

	public Integer getErrorCode() {
		return errorCode;
	}

	public String getStatus() {
		return status;
	}

	ResumeUtil resumeUtil = null;
}
/************************************************************************************************
 * 218267 characters generated by Talend Real-time Big Data Platform on the
 * August 18, 2026, 12:27:58 PM IST
 ************************************************************************************************/