# ETL Pipeline

ETL pipeline for transforming unstructured JSON order files into a normalized MySQL relational schema.

## 📁 Project Structure

```
order-etl/
├── extract.py          # Extract raw JSON from local directory
├── transform.py        # Transform and normalize data structures
├── load.py             # Load data into MySQL with transactions
├── main.py             #run etl pipeline
├── notify.py           # notification
├── .env                # Database credentials
├── logs/               # ETL run logs 
└── README.md           # This file
```


### 1. Prerequisites

- Python 3.8+
- MySQL Server installed and running on Windows
- Database schema already created (tables exist)
- gmail app password

### 2. Install Dependencies

```bash
pip install mysql-connector-python
```

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_mysql_username
DB_PASSWORD=your_mysql_password
DB_NAME=your_database_name
```

**Windows CMD:**
```cmd
set DB_HOST=localhost
set DB_PORT=3306
set DB_USER=root
set DB_PASSWORD=yourpassword
set DB_NAME=orders_db
```

```

### 4. Run the ETL Pipeline

**Option A: Command line argument**
```bash
python main.py "C:\path\to\json\files"
```

**Option B: Interactive prompt**
```bash
python main.py
# Then enter the directory path when prompted
```

## 📊 Data Flow

```
JSON Files (Directory)
    ↓
[EXTRACT] - Load raw JSON into Python dicts
    ↓
[TRANSFORM] - Apply normalization & flattening
    ↓
    ├─ Vertical flattening (objects → tables)
    ├─ Array explosion (items, actions)
    ├─ Date/time parsing (ISO 8601 → MySQL)
    ├─ Boolean normalization
    ├─ Address deduplication (hash-based)
    └─ Natural key resolution
    ↓
[LOAD] - Insert into MySQL with FK ordering
    ↓
    ├─ Dimension tables (customers, merchants, drivers, addresses)
    ├─ Fact table (orders)
    └─ Child tables (items, payments, actions, order notes, metadata)
    ↓
MySQL Database (Normalized Schema)
```

## 🗄️ Database Schema

### Dimension Tables
- `customers` - Customer profiles (natural key: phone)
- `merchants` - Merchant/restaurant profiles (natural key: merchant_id)
- `drivers` - Driver profiles (natural key: driver_id)
- `addresses` - Deduplicated addresses (natural key: hash)

### Fact Table
- `orders` - Core order information with FKs to dimensions

### Child/Detail Tables
- `items` - Order line items (1:N with orders)
- `payments` - Payment details (1:1 with orders)
- `tracking` - Tracking information (1:1 with orders)
- `order_actions` - Audit log of order events (1:N with orders)
- `order_notes` - Text notes (1:1 with orders)
- `order_metadata` - Metadata fields (1:1 with orders)


### Fail-Fast Philosophy
- Pipeline stops on first error
- Transaction rolls back completely
- No partial loads
- Clear error messages logged

### Transaction Scope
- 1 json files represents 1 order
- One transaction per ETL run
- All orders committed together or none

### Logging
- All runs logged to `logs/etl_run_YYYYMMDD_HHMMSS.log`
- Console output for real-time monitoring
- Detailed error traces for debugging

## 📝 JSON Structure Expected

```json
{
  "order_id": "ORD-12345",
  "order_number": "12345",
  "order_type": "delivery",
  "order_status": "delivered",
  "created_at": "2020-08-17T14:53:28.122Z",
  "updated_at": "2020-08-17T16:20:00.000Z",
  "customer": {
    "customer_id": "CUST-001",
    "first_name": "John",
    "last_name": "Doe",
    "phone": "+201234567890",
    "email": "john@example.com",
    "is_verified": true
  },
  "merchant": {
    "merchant_id": "MERCH-001",
    "business_name": "Restaurant Name",
    "phone": "+201111111111"
  },
  "driver": {
    "driver_id": "DRV-001",
    "first_name": "Driver",
    "last_name": "Name",
    "phone": "+201222222222"
  },
  "items": [
    {
      "item_id": "ITEM-001",
      "name": "Product",
      "quantity": 2,
      "unit_price": 100.0,
      "total_price": 200.0
    }
  ],
  "pickup_address": {
    "street": "123 Main St",
    "city": "Cairo",
    "country": "Egypt",
    "latitude": 30.0444,
    "longitude": 31.2357
  },
  "dropoff_address": {
    "street": "456 Side St",
    "city": "Cairo",
    "country": "Egypt",
    "latitude": 30.0500,
    "longitude": 31.2400
  },
  "payment": {
    "payment_id": "PAY-001",
    "payment_method": "cash",
    "total_amount": 250.0
  },
  "tracking": {
    "tracker_id": "TRK-001",
    "tracking_url": "https://..."
  },
  "order_actions": [
    {
      "action_id": "ACT-001",
      "action_type": "created",
      "status": "success",
      "timestamp": "2020-08-17T14:53:28.122Z",
      "performed_by": "customer",
      "performed_by_id": "CUST-001"
    }
  ]
}
```

## 🧪 Testing Individual Modules

### Test Extract
```bash
python extract.py
# Enter directory path when prompted
```

### Test Transform
```bash
python transform.py
# Runs with embedded sample data
```

### Test Load (Database Connection)
```bash
python load.py
# Tests database connectivity
```

### Adding New Fields
1. Update `transform.py` transformation functions
2. Ensure corresponding table columns exist
3. Update `load.py` insert/upsert queries
4. No changes needed to extract or orchestration

### Adding New Tables
1. Create transformation function in `transform.py`
2. Add load function in `load.py`
3. Call from `load_transformed_order()` in correct FK order
4. Update README documentation

## ⚠️ Important Notes

- **No localStorage/browser storage** - All processing is server-side Python
- **Windows paths** - Use double backslashes or raw strings: `r"C:\path\to\files"`, so you dont get any unicode error
- **Date format** - All timestamps must be ISO 8601 format
- **Credentials** - NEVER commit `.env` file or hardcode passwords
- **Schema** - All tables must exist before running ETL
- **Batch size** - No artificial limits; processes all files in directory

## 📧 Future Enhancements (Not Implemented)

- this pipline acts as a mold expecting many json files perday so scalabily is supported
- we can use airflow for automation and sechudling
- you can use same logic for cloud services like usind redshift instead of mysql 
- Incremental processing (track processed files)
- Parallel processing for large batches
- Data quality dashboards
- Automated schema validation

## 🐛 Troubleshooting

### "No module named 'mysql.connector'"
```bash
pip install mysql-connector-python
```

### "Access denied for user"
- Check DB_USER and DB_PASSWORD environment variables
- Verify MySQL user has INSERT/UPDATE privileges

### "Table doesn't exist"
- Ensure all 11 tables are created in MySQL before running ETL
- Check DB_NAME points to correct database

### "No JSON files found"
- Verify directory path is correct
- Ensure files have `.json` extension
- Check file permissions on Windows

## 📄 License

Internal use only. Confidential and proprietary.