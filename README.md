# ScyllaDB Data Backfill Script

A simple and efficient script for copying data from production to staging ScyllaDB databases.

## 🚀 Features

- **Batch Processing**: Handles millions of records efficiently with configurable batch sizes
- **No Data Loss**: Appends data to STG without clearing existing data
- **Progress Tracking**: Real-time progress updates for large datasets
- **Data Integrity Verification**: Automatic verification of record counts
- **Memory Efficient**: Processes data in configurable batches
- **Error Handling**: Robust error handling and logging
- **Configurable**: Environment-based configuration
- **Clean Logging**: Structured logging with clear status messages

## 📋 Prerequisites

- Python 3.7+
- ScyllaDB/Cassandra cluster access
- Required Python packages (see requirements.txt)

## 🛠️ Installation

1. **Clone or download the script files**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   ```bash
   cp proto_backfill.env.example proto_backfill.env
   # Edit proto_backfill.env with your ScyllaDB credentials
   ```

## ⚙️ Configuration

Edit `proto_backfill.env` with your ScyllaDB credentials:

```env
# Production ScyllaDB
PRD_SCYLLA_CONTACT_POINTS=your_prd_host
PRD_SCYLLA_USERNAME=your_username
PRD_SCYLLA_PASSWORD=your_password
PRD_SCYLLA_KEYSPACE=your_prd_keyspace

# Staging ScyllaDB
STG_SCYLLA_CONTACT_POINTS=your_stg_host
STG_SCYLLA_USERNAME=your_username
STG_SCYLLA_PASSWORD=your_password
STG_SCYLLA_KEYSPACE=your_stg_keyspace
```

## 🚀 Usage

### Basic Usage
```bash
python proto_backfill_main.py
```

### Specify Tables
```bash
python proto_backfill_main.py --tables table1 table2 table3
```

### Set Log Level
```bash
python proto_backfill_main.py --log-level DEBUG
```

### Set Batch Size
```bash
python proto_backfill_main.py --batch-size 10000
```

### Test Connections
```bash
python test_proto_backfill.py
```

## 📊 Default Tables

The script processes these tables by default:
- `logistic_unbundling_details`
- `product_details`
- `shipment_details`
- `sscat_details`
- `supplier_details`
- `supplier_sscat_details`

## 🔍 How It Works

1. **Connect**: Establishes connections to both PRD and STG ScyllaDB
2. **Count**: Gets total record count from PRD tables
3. **Read**: Reads all data from PRD tables
4. **Batch Process**: Writes data to STG in configurable batches
5. **Progress**: Shows real-time progress for large datasets
6. **Verify**: Verifies record counts match between PRD and STG

## ⚙️ Batch Processing

The script is optimized for large datasets:

- **Default Batch Size**: 5,000 records per batch
- **Configurable**: Set via `--batch-size` or `BATCH_SIZE` environment variable
- **Memory Efficient**: Processes data in chunks to avoid memory issues
- **Progress Tracking**: Shows percentage completion for each table
- **No Data Loss**: Appends to STG without clearing existing data

## 📝 Logging

The script provides clear logging with:
- ✅ Success indicators
- ❌ Error indicators
- 📊 Statistics and progress
- 🔍 Detailed error messages

## 🧪 Testing

Run the test script to verify your configuration:
```bash
python test_proto_backfill.py
```

This will:
- Test database connections
- Check table access
- Run a small backfill simulation

## 📁 File Structure

```
rtp-backfill-script/
├── README.md                    # This file
├── proto_backfill_main.py      # Main backfill script
├── test_proto_backfill.py      # Test script
├── scylla_service.py           # ScyllaDB service
├── proto_handler.py            # Data handler
├── proto_backfill.env          # Environment config
├── proto_backfill.env.example  # Config template
└── requirements.txt            # Dependencies
```

## 🚨 Important Notes

- **Data Safety**: The script clears STG tables before writing new data
- **Backup**: Always backup your STG data before running
- **Testing**: Test with a small dataset first
- **Monitoring**: Monitor logs for any errors during execution

## 🆘 Troubleshooting

### Connection Issues
- Verify ScyllaDB credentials in `proto_backfill.env`
- Check network connectivity to ScyllaDB hosts
- Ensure keyspaces exist

### Permission Issues
- Verify user has read access to PRD tables
- Verify user has write access to STG tables

### Data Issues
- Check that PRD tables have data
- Verify table schemas match between PRD and STG

## 📞 Support

For issues or questions:
1. Check the logs for detailed error messages
2. Verify your configuration
3. Test with a single table first
4. Contact your ScyllaDB administrator