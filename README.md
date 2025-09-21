# Snowflake-Elasticsearch Integration

## Overview

This integration enables automated data collection from Snowflake data warehouse and ingestion into Elasticsearch for monitoring and analytics. The solution consists of a Python script that connects to Snowflake using private key authentication and a Logstash pipeline configuration that orchestrates the data collection process.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Snowflake     │◄───┤ snowflake_       │◄───┤    Logstash     │───►│  Elasticsearch  │
│   Data Warehouse│    │ elastic_updated  │    │   Pipeline      │    │    Cluster      │
│                 │    │      .py         │    │ (snowflake.conf)│    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                │                        │
                           ┌────▼────┐              ┌────▼────┐
                           │ Private │              │Logstash │
                           │   Key   │              │Keystore │
                           │ (.pem)  │              │         │
                           └─────────┘              └─────────┘
```

**Server**: SERVERNAME  
**Location**: `/etc/logstash/scripts/snowflake/`

## Components

### 1. Python Script: `snowflake_elastic_updated.py`

**Purpose**: Connects to Snowflake, executes queries, and returns formatted JSON data  
**Location**: `/etc/logstash/scripts/snowflake/snowflake_elastic_updated.py`  
**Language**: Python 3  
**Authentication Method**: RSA Private Key Authentication (PKCS#8 DER format)

#### Key Features:
- **Secure Authentication**: Uses RSA private key stored in PEM format
- **Multiple Query Support**: Supports various predefined SQL queries
- **Error Handling**: Structured error responses in JSON format
- **Logging**: Comprehensive logging with structured JSON output
- **Connection Management**: Proper connection cleanup and resource management

#### Dependencies:
```bash
pip3 install snowflake-connector-python cryptography
```

### 2. Logstash Configuration: `snowflake.conf`

**Purpose**: Orchestrates script execution and data ingestion to Elasticsearch  
**Location**: `/etc/logstash/conf.d/snowflake.conf`  
**Execution Interval**: Every 1200 seconds (20 minutes)

## Authentication Flow

### Snowflake Authentication Process

1. **Private Key Storage**: RSA private key stored in PEM format at `/etc/logstash/keypair/sf_keypair.pem`
2. **Key Loading**: Script loads PEM key using Python's `cryptography` library
3. **Format Conversion**: PEM key converted to DER format (PKCS#8) as required by Snowflake
4. **Connection**: Snowflake connector uses DER bytes for authentication

```python
# Authentication flow in script
private_key = serialization.load_pem_private_key(pem_data, password=None)
private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)
connection = snow.connect(
    account=account,
    user=user,
    private_key=private_key_der,  # DER format bytes
    warehouse=warehouse
)
```

### Logstash Keystore Integration

Logstash securely manages sensitive information using its built-in keystore:

```bash
# Keystore setup
sudo -u logstash /usr/share/logstash/bin/logstash-keystore add SF_KEYPAIR_PATH
# Stores: /etc/logstash/keypair/sf_keypair.pem

sudo -u logstash /usr/share/logstash/bin/logstash-keystore add ELASTIC_PASSWORD
# Stores: Elasticsearch password
```

**Configuration Reference**:
```ruby
command => "python3 ... --sf_keypair '${SF_KEYPAIR_PATH}' ..."
password => "${ELASTIC_PASSWORD}"
```

## Configuration Details

### Snowflake Connection Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| **Account** | `account_name_us-east-1` | Snowflake account identifier |
| **User** | `APP_ELASTIC_SVC_PRD_RSA` | Service account for data access |
| **Warehouse** | `DEMO_WH` | Compute warehouse for query execution |
| **Database** | `SNOWFLAKE` | Target database (system database) |
| **Authentication** | Private Key | RSA key-pair authentication |

### Available SQL Queries

The script supports multiple predefined queries:

1. **`sql_warehouse_load_history`**: Warehouse performance metrics
   ```sql
   SELECT START_TIME, END_TIME, AVG_RUNNING, AVG_QUEUED_LOAD
   FROM table(information_schema.warehouse_load_history(
       DATE_RANGE_START => TIMEADD(minute, -20, CURRENT_TIMESTAMP()),
       WAREHOUSE_NAME => 'DEMO_WH'
   ));
   ```

2. **`sql_check_version`**: Snowflake version information
   ```sql
   SELECT current_version() AS version;
   ```

3. **`sql_warehouse_usage`**: Warehouse credit usage
   ```sql
   SELECT warehouse_name, start_time, end_time, credits_used
   FROM information_schema.warehouse_metering_history
   WHERE start_time >= TIMEADD(hour, -24, CURRENT_TIMESTAMP())
   ORDER BY start_time DESC LIMIT 100;
   ```

## Logstash Pipeline Flow

### Input Stage
```ruby
input {
  exec {
    command => "python3 -W ignore /etc/logstash/scripts/snowflake/snowflake_elastic_updated.py --user 'APP_ELASTIC_SVC_PRD_RSA' --sf_keypair '${SF_KEYPAIR_PATH}' --account 'account_name_us-east-1' --warehouse 'DEMO_WH' --sql_query 'sql_warehouse_load_history'"
    interval => 1200
    codec => "json_lines"
  }
}
```

**Process**:
1. Logstash executes Python script every 20 minutes
2. Retrieves keypair path from keystore (`${SF_KEYPAIR_PATH}`)
3. Passes parameters to script via command line arguments
4. Expects JSON Lines format output

### Filter Stage
```ruby
filter {
  # Filter out logging messages
  if [@timestamp] and [file][name] {
    drop {}
  }
  
  # Process actual data records
  if [VERSION] or [START_TIME] or [warehouse] {
    mutate {
      add_field => { "[data][status]" => "success" }
      add_tag => ["snowflake_data", "success"]
    }
  }
}
```

**Process**:
1. **Log Filtering**: Drops internal logging messages from script
2. **Data Identification**: Identifies actual Snowflake data records
3. **Field Enhancement**: Adds metadata and tags for routing
4. **Type Conversion**: Converts numeric fields to appropriate types

### Output Stage
```ruby
output {
  if "success" in [tags] {
    elasticsearch {
      hosts => ["https://xxxxxxxxxxxxx.us-east-1.aws.found.io:9243"]
      index => "snowflake-metrics-%{+YYYY.MM.dd}"
      user => "logstash_internal"
      password => "${ELASTIC_PASSWORD}"
    }
  }
}
```

**Process**:
1. Routes successful data to Elasticsearch
2. Uses daily indices (`snowflake-metrics-YYYY.MM.dd`)
3. Authenticates using keystore-stored password

## File Structure

```
/etc/logstash/
├── conf.d/
│   └── snowflake.conf              # Logstash pipeline configuration
├── keypair/
│   └── sf_keypair.pem             # RSA private key (600 permissions)
└── scripts/
    └── snowflake/
        └── snowflake_elastic_updated.py  # Python data collection script
```

## Security Considerations

### File Permissions
```bash
# Private key security
sudo chown logstash:root /etc/logstash/keypair/sf_keypair.pem
sudo chmod 600 /etc/logstash/keypair/sf_keypair.pem

# Script permissions
sudo chown logstash:root /etc/logstash/scripts/snowflake/snowflake_elastic_updated.py
sudo chmod 755 /etc/logstash/scripts/snowflake/snowflake_elastic_updated.py
```

### Keystore Security
- All sensitive values stored in encrypted Logstash keystore
- Keystore accessible only to `logstash` user
- Optional keystore password protection via `LOGSTASH_KEYSTORE_PASS`

### Network Security
- HTTPS connections to both Snowflake and Elasticsearch
- Private key authentication (no passwords in configuration)
- Service account with minimal required permissions

## Installation and Setup

### 1. Install Dependencies
```bash
# System packages
sudo yum install python3 python3-pip

# Python dependencies
sudo pip3 install snowflake-connector-python cryptography
```

### 2. Create Directory Structure
```bash
sudo mkdir -p /etc/logstash/scripts/snowflake
sudo mkdir -p /etc/logstash/keypair
sudo chown -R logstash:root /etc/logstash/scripts/
sudo chown -R logstash:root /etc/logstash/keypair/
```

### 3. Deploy Files
```bash
# Copy Python script
sudo cp snowflake_elastic_updated.py /etc/logstash/scripts/snowflake/
sudo chown logstash:root /etc/logstash/scripts/snowflake/snowflake_elastic_updated.py
sudo chmod 755 /etc/logstash/scripts/snowflake/snowflake_elastic_updated.py

# Deploy private key
sudo cp sf_keypair.pem /etc/logstash/keypair/
sudo chown logstash:root /etc/logstash/keypair/sf_keypair.pem
sudo chmod 600 /etc/logstash/keypair/sf_keypair.pem

# Deploy Logstash configuration
sudo cp snowflake.conf /etc/logstash/conf.d/
sudo chown logstash:root /etc/logstash/conf.d/snowflake.conf
```

### 4. Configure Keystore
```bash
# Add keypair path
sudo -u logstash /usr/share/logstash/bin/logstash-keystore add SF_KEYPAIR_PATH
# Enter: /etc/logstash/keypair/sf_keypair.pem

# Add Elasticsearch password
sudo -u logstash /usr/share/logstash/bin/logstash-keystore add ELASTIC_PASSWORD
# Enter: your-elasticsearch-password

# Verify keystore
sudo -u logstash /usr/share/logstash/bin/logstash-keystore list
```

## Testing and Validation

### 1. Test Script Independently
```bash
sudo -u logstash python3 /etc/logstash/scripts/snowflake/snowflake_elastic_updated.py \
  --user 'APP_ELASTIC_SVC_PRD_RSA' \
  --sf_keypair '/etc/logstash/keypair/sf_keypair.pem' \
  --account 'account_name_us-east-1' \
  --warehouse 'DEMO_WH' \
  --sql_query 'sql_check_version'
```

**Expected Output**:
```json
{"VERSION": "9.28.1", "warehouse": "DEMO_WH", "account": "account_name_us-east-1", "execution_timestamp": "2025-09-18 06:34:08"}
```

### 2. Test Logstash Configuration
```bash
# Validate configuration syntax
sudo -u logstash /usr/share/logstash/bin/logstash --path.settings /etc/logstash -t -f /etc/logstash/conf.d/snowflake.conf

# Start Logstash with configuration
sudo systemctl restart logstash.service
```

### 3. Monitor Execution
```bash
# Monitor Logstash logs
sudo journalctl -u logstash.service -f

# Check Elasticsearch indices
curl -X GET "https://your-elastic-cluster:9243/_cat/indices/snowflake-*?v" \
  -u "logstash_internal:password"
```

## Troubleshooting

### Common Issues

#### 1. Authentication Failures
**Symptom**: `"cannot authenticate to Snowflake"`  
**Solutions**:
- Verify private key format: `openssl rsa -in sf_keypair.pem -check -noout`
- Check Snowflake user configuration: `DESC USER APP_ELASTIC_SVC_PRD_RSA;`
- Validate key permissions: `ls -la /etc/logstash/keypair/sf_keypair.pem`

#### 2. Script Execution Errors
**Symptom**: `"ERROR: Executing snowflake.py script failed"`  
**Solutions**:
- Test script independently with full command line
- Check Python dependencies: `pip3 list | grep snowflake`
- Verify script permissions and ownership

#### 3. JSON Parsing Errors
**Symptom**: `"JSON parse error, original data now in message field"`  
**Solutions**:
- Check script output format
- Verify error handling in script
- Review filter configuration for log message handling

#### 4. Keystore Issues
**Symptom**: Variables not resolving (`${SF_KEYPAIR_PATH}` appears literally)  
**Solutions**:
- Verify keystore entries: `sudo -u logstash /usr/share/logstash/bin/logstash-keystore list`
- Check keystore permissions
- Restart Logstash service after keystore changes

## Data Output Format

### Successful Query Response
```json
{
  "START_TIME": "2025-09-18 06:20:00.000000",
  "END_TIME": "2025-09-18 06:25:00.000000", 
  "AVG_RUNNING": 0.5,
  "AVG_QUEUED_LOAD": 0.2,
  "warehouse": "DEMO_WH",
  "account": "account_name_us-east-1",
  "execution_timestamp": "2025-09-18 06:34:08"
}
```

### Error Response
```json
{
  "error": "database_error",
  "message": "Connection timeout",
  "timestamp": "2025-09-18T06:34:08.123456"
}
```

## Monitoring and Maintenance

### Key Metrics to Monitor
- Script execution success rate
- Query response times
- Data ingestion volume
- Authentication failures
- Elasticsearch indexing errors

### Regular Maintenance Tasks
- **Weekly**: Review error logs and failed executions
- **Monthly**: Rotate and update private keys if required
- **Quarterly**: Review and optimize SQL queries
- **Annually**: Update dependencies and review security configuration

## Contact and Support

For issues or questions regarding this integration:
1. Check the troubleshooting section above
2. Review Logstash and script logs
3. Validate Snowflake connectivity independently
4. Consult Elastic and Snowflake documentation for connector-specific issues

---

**Environment**: Production (SERVERNAME)
