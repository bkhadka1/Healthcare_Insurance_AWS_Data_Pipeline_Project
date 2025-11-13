# Healthcare Insurance Analytics Pipeline

A serverless data pipeline built on AWS to analyze healthcare insurance data and answer critical business questions about claims, premiums, and customer demographics.

![AWS](https://img.shields.io/badge/AWS-Cloud-orange)
![Glue](https://img.shields.io/badge/AWS-Glue-blue)
![Athena](https://img.shields.io/badge/AWS-Athena-green)
![QuickSight](https://img.shields.io/badge/AWS-QuickSight-purple)
![Python](https://img.shields.io/badge/Python-3.x-yellow)

## 📊 Project Overview

This project implements a complete ETL pipeline to process healthcare insurance data from multiple sources, transform it, and provide actionable insights through SQL analytics and interactive dashboards.

### Business Problem

A healthcare insurance company needed to:
- Track revenue and understand customer behavior
- Analyze claims patterns across diseases, demographics, and geography
- Calculate royalties for past policyholders
- Optimize insurance offerings based on data-driven insights

### Solution

Built a scalable, serverless data pipeline on AWS that:
- ✅ Processes 8 different data sources (claims, patients, hospitals, policies, etc.)
- ✅ Handles data quality issues (column spaces, null values, duplicates)
- ✅ Answers 13 critical business requirements
- ✅ Provides interactive dashboards for stakeholders
- ✅ Costs ~$10-11/month (vs $180+ for traditional data warehouse)

## 🏗️ Architecture

```
Data Sources (8 files)
    ↓
AWS S3 (Data Lake)
    ↓
AWS Glue Crawler (Schema Discovery)
    ↓
AWS Glue ETL (Data Transformation)
    ↓
AWS Athena (SQL Analytics)
    ↓
Amazon QuickSight (Dashboards)
```

## 📁 Dataset

The project uses 8 datasets:

| File | Records | Description |
|------|---------|-------------|
| `claims.json` | 70 | Insurance claim transactions |
| `disease.csv` | 60 | Disease to subgroup mapping |
| `group.csv` | 58 | Insurance companies/groups |
| `grpsubgrp.csv` | 38 | Group-subgroup relationships |
| `hospital.csv` | 20 | Hospital information |
| `Patient_records.csv` | 70 | Patient demographics |
| `subgroup.csv` | 10 | Insurance subgroups with premiums |
| `subscriber.csv` | 100 | Subscriber information |

## 🎯 Business Requirements Answered

1. Which disease has maximum number of claims?
2. Find subscribers having age less than 30 who subscribe any subgroup
3. Find group with maximum subgroups
4. Find hospital serving most number of patients
5. Find subgroups with most claims
6. Total number of rejected claims
7. From where most claims are coming (city)
8. Portion of policies: Government vs Private
9. Average monthly premium paid by subscribers
10. Which group is most profitable
11. Patients below age 18 admitted for cancer
12. Cashless insurance patients with charges ≥ Rs. 50,000
13. Female patients over 40 with knee surgery in past year

## 🛠️ Technologies Used

- **AWS S3** - Data lake storage
- **AWS Glue** - ETL and data catalog
- **AWS Athena** - Serverless SQL analytics
- **Amazon QuickSight** - Interactive dashboards
- **Apache Spark** - Data transformation (via Glue)
- **Python 3** - Glue ETL scripts
- **Parquet** - Optimized columnar storage format

## 📈 Dashboards

### 1. Claims Overview Dashboard
![Claims Overview](https://via.placeholder.com/800x400?text=Claims+Overview+Dashboard)

**Visualizations:**
- Claim Amount by Disease (Horizontal Bar Chart)
- Claim Amount Distribution (Histogram)
- Claim Status Breakdown (Donut Chart)
- Claim Amount by Gender (Bar Chart)

**Key Insights:**
- Stroke has highest total claim amount
- Most claims approved (majority segment)
- Male patients have slightly higher total claims

### 2. Premiums by Group Dashboard
![Premiums by Group](https://via.placeholder.com/800x400?text=Premiums+by+Group+Dashboard)

**Visualizations:**
- Government vs Private Comparison (Bar Chart)
- Top Insurance Companies by Premium (Horizontal Bar Chart)

**Key Insights:**
- Private insurance dominates (7x more than Government)
- Cholamandalam MS leads with 99,000 in premiums
- Clear market leaders identified

## 🚀 Getting Started

### Prerequisites

- AWS Account
- IAM permissions for S3, Glue, Athena, QuickSight
- Basic knowledge of SQL

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/healthcare-insurance-pipeline.git
   cd healthcare-insurance-pipeline
   ```

2. **Upload data to S3**
   - Create S3 bucket: `healthcare-insurance-data`
   - Upload all 8 data files to `s3://healthcare-insurance-data/raw/`

3. **Create IAM Role**
   - Service: AWS Glue
   - Attach policy: `AWSGlueServiceRole`
   - Add inline policy for S3 access (see documentation)

4. **Setup Glue Database**
   - Create database: `healthcare_db`

5. **Run Glue Crawler**
   - Create crawler pointing to S3 raw data
   - Run crawler to discover schemas

6. **Execute Glue ETL Job**
   - Copy `glue_etl_script.py` to Glue script editor
   - Configure job with 2 G.1X workers
   - Run job to clean and transform data

7. **Query with Athena**
   - Configure query result location
   - Run SQL queries from `athena_queries.sql`

8. **Create QuickSight Dashboards**
   - Connect QuickSight to Athena
   - Create visualizations as per documentation

## 📂 Project Structure

```
healthcare-insurance-pipeline/
│
├── data/                          # Sample data files
│   ├── claims.json
│   ├── disease.csv
│   ├── group.csv
│   ├── grpsubgrp.csv
│   ├── hospital.csv
│   ├── Patient_records.csv
│   ├── subgroup.csv
│   └── subscriber.csv
│
├── scripts/
│   └── glue_etl_script.py        # AWS Glue ETL transformation script
│
├── queries/
│   └── athena_queries.sql        # All 13 business requirement queries
│
├── docs/
│   ├── implementation_guide.md   # Detailed step-by-step guide
│   └── architecture_diagram.png  # Architecture visualization
│
└── README.md
```

## 💰 Cost Breakdown

| Service | Usage | Monthly Cost |
|---------|-------|--------------|
| S3 Storage | 10 GB | $0.25 |
| Glue Crawler | 2 runs | $0.88 |
| Glue ETL Job | 1 run | $0.44 |
| Athena Queries | ~10 GB scanned | $0.10 |
| QuickSight | 1 user | $9.00 |
| **Total** | | **~$10-11/month** |

**Compare to Redshift:** $180-360/month minimum

## 🔍 Sample Queries

**Find disease with maximum claims:**
```sql
SELECT 
    disease_name,
    COUNT(*) as total_claims,
    SUM(claim_amount) as total_claim_amount
FROM cleaned_claims
WHERE claim_status != 'Rejected'
GROUP BY disease_name
ORDER BY total_claims DESC
LIMIT 1;
```

**Government vs Private insurance split:**
```sql
SELECT 
    g.grp_type,
    COUNT(DISTINCT s.sub_id) as subscriber_count,
    ROUND(COUNT(DISTINCT s.sub_id) * 100.0 / 
          (SELECT COUNT(DISTINCT sub_id) FROM cleaned_subscriber), 2) as percentage
FROM cleaned_subscriber s
JOIN cleaned_grpsubgrp gs ON s.subgrp_id = gs.subgrp_id
JOIN cleaned_group g ON gs.grp_id = g.grp_id
GROUP BY g.grp_type
ORDER BY subscriber_count DESC;
```

## 📊 Results Summary

- **Total Claims Processed:** 70
- **Approved Claims:** ~40%
- **Rejected Claims:** ~20%
- **Pending Claims:** ~40%
- **Total Claim Amount:** ₹7.2M
- **Top Disease by Claims:** Stroke
- **Private vs Government:** 7:1 ratio
- **Average Monthly Premium:** ₹1,850

## 🎓 What I Learned

- Building serverless data pipelines on AWS
- ETL best practices with AWS Glue and Spark
- Data quality handling (nulls, duplicates, formatting)
- Query optimization with Athena and Parquet
- Creating interactive dashboards with QuickSight
- Cost optimization strategies for AWS services

## 🔧 Troubleshooting

**Issue:** Glue crawler finds 0 tables
- **Solution:** Verify S3 paths, check IAM permissions

**Issue:** Athena query fails with "Column not found"
- **Solution:** Check you're querying `cleaned_` tables, verify column names

**Issue:** QuickSight can't connect to Athena
- **Solution:** Grant QuickSight access to S3 bucket in permissions

See [full documentation](docs/implementation_guide.md) for detailed troubleshooting.

## 📝 Future Enhancements

- [ ] Add automated data quality checks
- [ ] Implement incremental data loading
- [ ] Create Lambda triggers for real-time processing
- [ ] Add email notifications for job failures
- [ ] Expand dashboard with predictive analytics
- [ ] Implement data lineage tracking

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Bikash Khadka**
- GitHub: [@bkhadka1](https://github.com/bkhadka1)
- LinkedIn: [Bikash Khadka](https://www.linkedin.com/in/bkhadka14/)
- Email: bcash2233@gmail.com

## 🙏 Acknowledgments

- AWS Documentation for Glue, Athena, and QuickSight
- Healthcare insurance industry standards and regulations
- Open-source community for tools and best practices

## 📚 References

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Amazon Athena Best Practices](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning.html)
- [QuickSight User Guide](https://docs.aws.amazon.com/quicksight/)

---

⭐ **Star this repo if you find it helpful!**

Made with ❤️ using AWS Serverless Technologies
