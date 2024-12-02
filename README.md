# Octopus Analysis

Octopus Analysis is a data analysis project designed to process and analyze Scopus data using Apache Spark.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/punchanabu/octopus-analysis.git
   cd octopus-analysis
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Add data**
  Copy paste your `scopus` data folder into the `/data/` directory

## Usage

1. **Configure Environment Variables**:
   - Create a `.env` file in the root directory with the necessary environment variables. Refer to the [Configuration](#configuration) section for details.

2. **Run the Application**:
   ```bash
   python main.py
   ```
   This will read and stream data into your Cassandra database.



## Configuration

The application uses environment variables for configuration. Create a `.env` file in the root directory with the following variables:

```env
DB_HOST="cassandra-host"
DB_PORT="cassandra-port"
DB_CLUSTER="cassandra-cluster"
SCOPUS_DATA_PATH="/Users/punpun/Documents/Personal/cedt/dsde-project/octopus-analysis/data/scopus"
```
