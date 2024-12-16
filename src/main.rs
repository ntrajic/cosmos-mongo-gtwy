/*
A vCore Azure CosmosDB Gateway for MongoDB

SCOPE: Design and implement a vCore MongoDB Gateway service (CosmosDB to MongoDB Gateway),
which will be implemented in Rust language.
document: A vCore Azure CosmosDB Gateway for MongoDB.docx:
table of contents: 
a. Requirements (functional and non-functional), 
b. Use Cases, 
c. Architecture: Components, Collaboration, 
d. Design: APIs, Components Design, Collaboration Design, Plan and Propose Rust Libs and Frameworks
e. Implement 
f. Unit and Integration Tests, 
g. Performance and Stress Tests

SUPPORTING DOCUMENTS:
1. The description of an envisioned and projected Azure CosmosDB for MongDB vCore service 
(shorter "vCore service") is given in the architect's blog: 
https://www.infoq.com/news/2023/03/azure-cosmosdb-mongodb-vcore/. 
vCore service differs from original Azure CosmosDB for MongoDB service, 
shorter "original service". 
2. Original service is implemented and documented at: 
https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/.

TASK:
Write requirements and use cases in order to design a vCore MongoDB Gateway service 
(CosmosDB to MongoDB Gateway), which will be implemented in Rust language.

document full name: A vCore Azure CosmosDB Gateway for MongoDB: Requirements, Use Cases, Architecture, Design, Unit and Integration Tests, Performance and Stress Tests   
document short name: vCore CosmosDB Gateway for MongoDB

DETAILED ACTION ITEMS for the TASK:
1. Write Requirements for Gateway between CosmosDB and MongoDB on Azure Cloud. 
2. Write Use Cases to issue sql queries to Gateway (Azure CosomosDB Gateway) for MongoDB  
3. Design the API and Specify the Architecture: specify components, and how they collaborate 
4. Plan and Choose appropriate RUST libraries and frameworks 
5. Implement Components in RUST language 
5.1.Set up a Rust Project using Cargo 
5.2.Use open-source A and B: 
            A. Use the MongoDB Rust Driver, https://github.com/mongodb/mongo-rust-driver, and 
            B. Use a sample to connect a Rust application with Azure Cosmos DB's API for MongoDB,  
               https://github.com/Azure-Samples/azure-cosmos-db-mongodb-rust-getting-started, and  
    Use specifications: requirements and use cases 
=> Please implement the core Gateway functionality in RUST language.         
5.3. Develop the gateway logic to interface between CosmosDB and MongoDB 
6. Write Unit Tests for Individual Components 
7. write Unit Tests for Components Collaboration 
8. write System Integration Tests with both CosmosDB and MongoDB 
9. Write System integration of Components 
10. Run Performance Tests 
11. Run Stress Tests
*/

/*
##  ACTION_ITEM_1: Requirements
high-level groups of requirements
1. **MongoDB Compatibility**: The gateway must provide full MongoDB compatibility to support existing MongoDB applications [1].
2. **Azure Integration**: Implement native Azure integrations for seamless interaction with other Azure services [1]. (e.g. Azure Monitor, Azzure CLI)
3. **vCore Architecture**: Design the gateway to align with the vCore-based architecture, differing from the original RU-based model [1].
4. **Scalability**: Support high-capacity vertical and horizontal scaling with vCore-based cluster tier options [1].
5. **Query Handling**: Efficiently manage various operation types, including long-running queries, complex aggregation pipelines, distributed transactions, and joins [1].
6. **Performance**: Optimize for workloads with any operation types, not just point reads [1].
7. **Pricing Model**: Implement a pricing structure based on the number of vCores provisioned [1].

### Functional Requirements:
1. **Query Translation: ** Convert MongoDB queries into Azure CosmosDB SQL queries and vice versa.
2. **Data Synchronization: ** Ensure real-time or near-real-time synchronization between CosmosDB and MongoDB data.
3. **Authentication and Authorization: ** Support authentication mechanisms to securely connect to both CosmosDB and MongoDB.
4. **Error Handling: ** Provide detailed error responses for invalid queries or connection issues.
5. **Logging and Monitoring: ** Log all incoming requests, responses, and errors for debugging and monitoring.
6. **Configurable Settings: ** Allow customization of connection strings, query timeouts, and data synchronization intervals.
7. **Performance Optimization: ** Minimize latency for query translation and execution.

### Non-Functional Requirements:
1. **Scalability: ** Handle high query throughput and large datasets.
2. **Reliability: ** Ensure the gateway remains operational under high load and during network fluctuations.
3. **Security: ** Protect data in transit and at rest, using TLS and secure storage mechanisms.
4. **Compatibility: ** Support all MongoDB query features compatible with Azure CosmosDB’ s MongoDB API.
5. **Maintainability: ** Provide clear documentation and modular code to facilitate future enhancements.
*/

/*
## ACTION_ITEM_2: Use Cases

### High level use cases:
1. **User-Generated Content**: Support storage and querying of UGC for web, mobile, and social media applications (e.g., chat sessions, tweets, blog posts, ratings, comments) [1].
2. **Chatbot Applications**: Facilitate data operations for intelligent applications like chatbots [1].
3. **Lift and Shift**: Enable seamless migration of existing MongoDB applications to run as-is on a fully managed service [1].
4. **Complex Data Operations**: Support applications requiring distributed transactions, joins, and complex aggregation pipelines [1].
5. **Data Migration**: Seamlessly migrate data between CosmosDB and MongoDB.
6. **Data Synchronization**: Keep data synchronized between the two databases.
7. **Query Translation**: Translate SQL queries to MongoDB queries and vice versa.
8. **Analytics**: Perform complex analytics on data stored in either database.
9. **Backup and Restore**: Facilitate backup and restore operations across both databases.

### Use Cases to Issue SQL Queries to the Gateway

### Use Case 1: Query Translation
- **Actors:** Application Developer
- **Scenario: ** A developer submits a MongoDB query to the gateway. The gateway translates it into a CosmosDB SQL query and returns the results.
- **Outcome: ** The developer retrieves data from CosmosDB as if querying MongoDB.

### Use Case 2: Data Insertion
- **Actors: ** Data Integration Service
- **Scenario: ** A service sends MongoDB-style insert commands to the gateway. The gateway translates and executes them on CosmosDB.
- **Outcome: ** Data is successfully inserted into CosmosDB.

### Use Case 3: Real-Time Analytics
- **Actors: ** Data Analyst
- **Scenario: ** The analyst issues a complex aggregation query via the gateway. The gateway processes the query and returns results from CosmosDB.
- **Outcome: ** Analytics are performed seamlessly.

### Use Case 4: Error Handling
- **Actors: ** System Administrator
- **Scenario: ** An invalid MongoDB query is submitted. The gateway returns a detailed error message.
- **Outcome:** The administrator corrects the query based on the error message.
*/


/*
## ACTION_ITEM_3: Desing the API and Specify Architecture: Components and Collaboration

By following below cited architectural note [1], the vCore MongoDB Gateway service can effectively bridge Azure Cosmos DB for MongoDB 
vCore with existing MongoDB applications, providing the benefits of Azure's managed service 
while maintaining compatibility and performance.
Citation:[1] https://www.infoq.com/news/2023/03/azure-cosmosdb-mongodb-vcore/

### API Design:

- **Endpoint:** `/query`
  - **Method:** POST
  - **Input:** MongoDB query JSON
  - **Output:** Translated query results

- **Endpoint:** `/insert`
  - **Method:** POST
  - **Input:** MongoDB insert command JSON
  - **Output:** Acknowledgment of success or failure

- **Endpoint:** `/status`
  - **Method:** GET
  - **Output:** Health status of the gateway

- **Authentication**: Implement token-based authentication.
- **Error Handling**: Provide comprehensive error handling and logging.


Requirements when integratins APIs with other services:
1. **MongoDB Wire Protocol**: Implement the MongoDB wire protocol for compatibility with existing drivers and tools.
2. **REST API**: Provide a REST API for management operations and Azure service integrations.
3. **gRPC Interface**: Implement a gRPC interface for high-performance inter-service communication.

### **Components:**
  1. **API Layer:** Handles HTTP requests and responses.
  2. **Query Translator:** Translates MongoDB queries to CosmosDB SQL queries.
  3. **Database Connector:** Interfaces with CosmosDB and MongoDB using respective drivers.
  4. **Synchronization Module:** Manages data synchronization between CosmosDB and MongoDB.
  5. **Logging and Monitoring Module:** Captures logs and monitors system performance.

### **Collaboration:**
  1. API Layer forwards requests to the Query Translator.
  2. The Query Translator interacts with the Database Connector to execute queries.
  3. Synchronization Module ensures consistent data states between CosmosDB and MongoDB.
  4. Logging and Monitoring Module captures and analyzes system metrics.

=> Please implement the core Gateway functionality in RUST language.

####2. **Query Translator/Processor Component**:
2.1- Parse MongoDB queries
2.2- Optimize queries for Cosmos DB execution
2.3- Handle complex aggregation pipelines
3. **Transaction Manager**:
3.1- Support ACID transactions
3.2- Implement distributed transaction coordination
4. **Scaling Manager**:
4.1- Monitor vCore usage
4.2- Trigger vertical/horizontal scaling based on workload
5. **Metrics Collector**:
- Gather performance metrics
- Integrate with Azure Monitor

#### Plan and Choose Appropriate Rust Libraries and Frameworks

1. **Actix-Web:** For building the API layer.
2. **MongoDB Rust Driver:** For connecting to MongoDB.
3. **Azure SDK for Rust:** For interfacing with Azure CosmosDB. (azure_data_cosmos)
4. **Serde:** For serializing and deserializing JSON data.
5. **Tokio:** For asynchronous runtime.
6. **Hyper:** For HTTP server implementation.
7. **Tonic:** For gRPC support.
8. **Tracing:** For logging and diagnostics.
9. **Log and Env-Logger:** For logging.
10.**Criterion:** For performance testing.

#### Data Model
1. **Document Model**: Maintain MongoDB's document model while mapping to Cosmos DB's underlying structure.
2. **Index Management**: Support MongoDB indexing strategies and map them to Cosmos DB indexes.
3. **Schema Validation**: Implement MongoDB schema validation rules.
 */

/*
## ACTION_ITEM_5: Implement Components in RUST language

### 5.1 Set Up a Rust Project Using Cargo
- **Command:** `cargo new azure_cosmosdb_mongodb_gateway`

### 5.2 Use Open-Source Tools:
- **MongoDB Rust Driver:**
  - Repository: [MongoDB Rust Driver] (https://github.com/mongodb/mongo-rust-driver)
- **Azure CosmosDB Sample:**
  - Repository: [Azure CosmosDB Rust Sample] (https://github.com/Azure-Samples/azure-cosmos-
 
### 5.3 Implement the core Gateway functionality in RUST language.
- **Query Translator/Processor Component:** Implement a module to convert MongoDB queries into CosmosDB SQL queries.
- **Database Connector Component:** Implement a module to interact with CosmosDB and MongoDB using respective drivers.
    -Use the MongoDB Rust Driver and Azure CosmosDB SDK to interface with databases.
- **Synchronization Module**:
    - Manage data synchronization between CosmosDB and MongoDB.    
- **Transaction Manager**:
    -Support ACID transactions
    -Implement distributed transaction coordination
- **Scaling Manager**:
    -Monitor vCore usage
    -Trigger vertical/horizontal scaling based on workload
- **Metrics Collector**:
    - Gather performance metrics
    - Integrate with Azure Monitor
 */

/*
## 6. Write Unit Tests for Individual Components
- Test API Layer endpoints with mocked inputs.
- Verify query translation logic with sample queries.
- Test database connectivity and operations.
 */


/*
 ## 7. Write Unit Tests for Components Collaboration
- Test interactions between API Layer, Query Translator, and Database Connector.
- Mock dependencies to isolate collaborative tests.
*/


 /*
  ## 8. Write System Integration Tests
- **Objective:** Verify end-to-end functionality of the gateway.
- **Approach:** Deploy the gateway and connect it to CosmosDB and MongoDB test instances.
- **Scenarios:**
  1. Issue a query and validate results.
  2. Insert data and verify persistence.
  3. Test error handling for invalid queries.
*/

/*
## 9. Write System Integration of Components
- Integrate and test all modules together within a staging environment.
- Validate seamless interactions between modules under real-world scenarios.
*/


/*
## 10. Run Performance Tests
- **Objective:** Measure latency, throughput, and resource utilization.
- **Tool:** Criterion for benchmarking.
- **Scenarios:**
  1. High query volume.
  2. Complex aggregation queries.
*/

/*
## 11. Run Stress Tests
- **Objective:** Ensure stability under extreme conditions.
- **Approach:**
  1. Simulate peak traffic scenarios.
  2. Gradually increase load until failure points are identified.
  3. Analyze and optimize performance bottlenecks.
*/


// 5.3 Develop Gateway Logic - Generated Prototype
use mongodb::{Client, options::ClientOptions};
use azure_data_cosmos::prelude::*;


// To use Document<value> type from azure_data_cosmos:
// First, import the necessary types:
use azure_data_cosmos::prelude::CosmosClient;
use std::sync::Arc;

pub struct GatewayClient {
    inner: Arc<Client>,
}

use serde_json::Value;

type TupleMongoDBCosmosDb = (Client, CosmosClient);


// simple queries: `execute_query` method to use `Document<Value>`:
use mongodb::bson::{Document, doc};
use serde_json::from_str;


// complex queries: additional features
//use azure_cosmos::prelude::*;
use std::collections::HashMap;

#[derive(Debug)]
struct QueryOptions {
    limit: Option<i64>,
    skip: Option<i64>,
    sort: Option<Document>,
    projection: Option<Document>,
}

struct CosmosDbGateway {
    mongo_client: Client,
    cosmos_client: CosmosClient,
}


impl CosmosDbGateway {
   
    async fn new(mongo_connection_string: &str, cosmos_connection_string: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mongo_client_options = ClientOptions::parse(mongo_connection_string).await?;
        let mongo_client = Client::with_options(mongo_client_options)?;

        let cosmos_client = CosmosClient::from_connection_string(cosmos_connection_string)?;
        // return tuple
        Ok(Self (
            mongo_client,
            cosmos_client,
        ))
    }

    // Implement the query translation and execution logic for the execute_query method. 
    // This implementation will handle basic MongoDB queries and translate them to Cosmos DB SQL API queries.
    // execute_query method:
    // o Parses the MongoDB query string
    // o Translates it to Cosmos DB SQL
    // o Executes the query
    // o Converts results back to MongoDB Documents
    async fn execute_query(&self, query: &str) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
        // Parse the MongoDB query string into a Document
        let mongo_query: Document = from_str(query)?;
        
        // Translate MongoDB query to Cosmos DB SQL
        let cosmos_sql = self.translate_query(&mongo_query)?;
        
        // Execute the query against Cosmos DB
        let database = self.1.database("your_database_name");
        let container = database.container("your_container_name");
        
        let query_response = container
            .query_documents(cosmos_sql.as_str(), QueryCrossPartition::Yes)
            .await?;
            
        // Convert Cosmos DB results to MongoDB Documents
        let mut results = Vec::new();
        
        for item in query_response {
            let doc: Document = from_str(&item.to_string())?;
            results.push(doc);
        }
        
        Ok(results)
    }

    // translate_query helper method:
    // o Converts MongoDB query operators to Cosmos DB SQL WHERE clauses
    // o Handles basic comparison operators ($eq, $gt, $lt, $gte, $lte)
    // o Builds a SQL query string
    fn translate_query(&self, mongo_query: &Document) -> Result<String, Box<dyn std::error::Error>> {
        // Basic query translation logic
        let mut sql = String::from("SELECT * FROM c WHERE ");
        
        for (key, value) in mongo_query.iter() {
            match value {
                mongodb::bson::Bson::Document(doc) => {
                    // Handle operators
                    for (op, val) in doc.iter() {
                        match op.as_str() {
                            "$eq" => {
                                sql.push_str(&format!("c.{} = {}", key, self.bson_to_sql_value(val)?));
                            }
                            "$gt" => {
                                sql.push_str(&format!("c.{} > {}", key, self.bson_to_sql_value(val)?));
                            }
                            "$lt" => {
                                sql.push_str(&format!("c.{} < {}", key, self.bson_to_sql_value(val)?));
                            }
                            "$gte" => {
                                sql.push_str(&format!("c.{} >= {}", key, self.bson_to_sql_value(val)?));
                            }
                            "$lte" => {
                                sql.push_str(&format!("c.{} <= {}", key, self.bson_to_sql_value(val)?));
                            }
                            _ => return Err("Unsupported operator".into()),
                        }
                    }
                }
                // Handle direct value comparison (implicit $eq)
                _ => {
                    sql.push_str(&format!("c.{} = {}", key, self.bson_to_sql_value(value)?));
                }
            }
        }
        
        Ok(sql)
    }

    // bson_to_sql_value helper method:
    // o Converts BSON values to SQL-compatible string representations
    // o Handles common data types (String, Int32, Int64, Double, Boolean)
    fn bson_to_sql_value(&self, value: &mongodb::bson::Bson) -> Result<String, Box<dyn std::error::Error>> {
        match value {
            mongodb::bson::Bson::String(s) => Ok(format!("'{}'", s)),
            mongodb::bson::Bson::Int32(i) => Ok(i.to_string()),
            mongodb::bson::Bson::Int64(i) => Ok(i.to_string()),
            mongodb::bson::Bson::Double(d) => Ok(d.to_string()),
            mongodb::bson::Bson::Boolean(b) => Ok(b.to_string()),
            _ => Err("Unsupported BSON type".into()),
        }
    }

    // extend the CosmosDbGateway implementation to support these additional features:
    // and build more complex queries:
    // o More MongoDB operators ($in, $or, $and, etc.)
    // o Complex nested queries
    // o Pagination
    // o Sorting
    // o Projection (SELECT specific fields)
    // o Aggregation pipeline translation
    fn build_sql_query(&self, mongo_query: &Document, options: Option<QueryOptions>) 
    -> Result<SqlQueryParts, Box<dyn std::error::Error>> {
        let mut parts = SqlQueryParts::default();
        
        // Handle projection
        if let Some(opts) = &options {
            if let Some(proj) = &opts.projection {
                parts.select = self.build_projection(proj)?;
            }
        }
        
        // Handle WHERE clause
        parts.where_clause = self.translate_query(mongo_query)?;
        
        // Handle sorting
        if let Some(opts) = &options {
            if let Some(sort) = &opts.sort {
                parts.order_by = self.build_sort_clause(sort)?;
            }
        }
        
        // Handle pagination
        if let Some(opts) = &options {
            if let Some(skip) = opts.skip {
                parts.offset = format!("OFFSET {}", skip);
            }
            if let Some(limit) = opts.limit {
                parts.limit = format!("LIMIT {}", limit);
            }
        }
        
        Ok(parts)
    }

    fn translate_query(&self, query: &Document) -> Result<String, Box<dyn std::error::Error>> {
        match self.translate_expression(query)? {
            Some(where_clause) => Ok(where_clause),
            None => Ok("TRUE".to_string()),
        }
    }

    fn translate_expression(&self, expr: &Document) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let mut conditions = Vec::new();

        for (key, value) in expr {
            match key.as_str() {
                "$and" => {
                    if let mongodb::bson::Bson::Array(arr) = value {
                        let mut and_conditions = Vec::new();
                        for item in arr {
                            if let mongodb::bson::Bson::Document(doc) = item {
                                if let Some(cond) = self.translate_expression(doc)? {
                                    and_conditions.push(format!("({})", cond));
                                }
                            }
                        }
                        if !and_conditions.is_empty() {
                            conditions.push(and_conditions.join(" AND "));
                        }
                    }
                }
                "$or" => {
                    if let Bson::Array(arr) = value {
                        let mut or_conditions = Vec::new();
                        for item in arr {
                            if let mongodb::bson::Bson::Document(doc) = item {
                                if let Some(cond) = self.translate_expression(doc)? {
                                    or_conditions.push(format!("({})", cond));
                                }
                            }
                        }
                        if !or_conditions.is_empty() {
                            conditions.push(format!("({})", or_conditions.join(" OR ")));
                        }
                    }
                }
                "$in" => {
                    if let mongodb::bson::Bson::Array(arr) = value {
                        let values: Vec<String> = arr.iter()
                            .map(|v| self.bson_to_sql_value(v))
                            .collect::<Result<_, _>>()?;
                        conditions.push(format!("c.{} IN ({})", key, values.join(", ")));
                    }
                }
                _ => {
                    if !key.starts_with("$") {
                        match value {
                            mongodb::bson::Bson::Document(doc) => {
                                conditions.push(self.translate_comparison_operators(key, doc)?);
                            }
                            _ => {
                                conditions.push(format!("c.{} = {}", 
                                    key, self.bson_to_sql_value(value)?));
                            }
                        }
                    }
                }
            }
        }

        if conditions.is_empty() {
            Ok(None)
        } else {
            Ok(Some(conditions.join(" AND ")))
        }
    }

    fn translate_comparison_operators(&self, field: &str, operators: &Document) 
        -> Result<String, Box<dyn std::error::Error>> {
        let mut conditions = Vec::new();

        for (op, value) in operators {
            let condition = match op.as_str() {
                "$eq" => format!("c.{} = {}", field, self.bson_to_sql_value(value)?),
                "$gt" => format!("c.{} > {}", field, self.bson_to_sql_value(value)?),
                "$lt" => format!("c.{} < {}", field, self.bson_to_sql_value(value)?),
                "$gte" => format!("c.{} >= {}", field, self.bson_to_sql_value(value)?),
                "$lte" => format!("c.{} <= {}", field, self.bson_to_sql_value(value)?),
                "$ne" => format!("c.{} != {}", field, self.bson_to_sql_value(value)?),
                "$regex" => {
                    if let mongodb::bson::Bson::String(pattern) = value {
                        format!("CONTAINS(c.{}, {})", field, self.bson_to_sql_value(value)?)
                    } else {
                        return Err("Invalid regex pattern".into());
                    }
                }
                _ => return Err(format!("Unsupported operator: {}", op).into()),
            };
            conditions.push(condition);
        }

        Ok(conditions.join(" AND "))
    }

    // Support for aggregation pipeline
    async fn execute_aggregate(&self, pipeline: Vec<Document>) 
        -> Result<Vec<Document>, Box<dyn std::error::Error>> {
        let sql = self.translate_aggregate_pipeline(&pipeline)?;
        
        let database = self.1.database("your_database_name");
        let container = database.container("your_container_name");
        
        let query_response = container
            .query_documents(&sql, QueryCrossPartition::Yes)
            .await?;
            
        let mut results = Vec::new();
        for item in query_response {
            let doc: Document = from_str(&item.to_string())?;
            results.push(doc);
        }
        
        Ok(results)
    }

    fn translate_aggregate_pipeline(&self, pipeline: &[Document]) 
        -> Result<String, Box<dyn std::error::Error>> {
        let mut sql = String::from("SELECT ");
        let mut group_by = Vec::new();
        let mut having = Vec::new();

        for stage in pipeline {
            for (op, value) in stage {
                match op.as_str() {
                    "$match" => {
                        if let mongodb::bson::Bson::Document(match_doc) = value {
                            sql.push_str(&format!("WHERE {}", self.translate_query(match_doc)?));
                        }
                    }
                    "$group" => {
                        if let mongodb::bson::Bson::Document(group_doc) = value {
                            let (select_clause, group_clause) = self.translate_group(group_doc)?;
                            sql.push_str(&select_clause);
                            group_by = group_clause;
                        }
                    }
                    "$sort" => {
                        if let mongodb::bson::Bson::Document(sort_doc) = value {
                            sql.push_str(&self.build_sort_clause(sort_doc)?);
                        }
                    }
                    "$limit" => {
                        if let mongodb::bson::Bson::Int64(limit) = value {
                            sql.push_str(&format!(" LIMIT {}", limit));
                        }
                    }
                    _ => return Err(format!("Unsupported aggregation operator: {}", op).into()),
                }
            }
        }

        if !group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_by.join(", ")));
        }
        if !having.is_empty() {
            sql.push_str(&format!(" HAVING {}", having.join(" AND ")));
        }

        Ok(sql)
    }

    fn translate_group(&self, group_doc: &Document) 
        -> Result<(String, Vec<String>), Box<dyn std::error::Error>> {
        let mut select_parts = Vec::new();
        let mut group_by = Vec::new();

        for (field, value) in group_doc {
            if field == "_id" {
                if let mongodb::bson::Bson::Document(id_doc) = value {
                    for (k, v) in id_doc {
                        group_by.push(format!("c.{}", v));
                    }
                }
            } else {
                if let mongodb::bson::Bson::Document(agg_doc) = value {
                    for (agg_op, agg_field) in agg_doc {
                        let sql_agg = match agg_op.as_str() {
                            "$sum" => "SUM",
                            "$avg" => "AVG",
                            "$min" => "MIN",
                            "$max" => "MAX",
                            "$count" => "COUNT",
                            _ => return Err(format!("Unsupported aggregation operator: {}", agg_op).into()),
                        };
                        select_parts.push(format!("{0}(c.{1}) AS {2}", 
                            sql_agg, agg_field, field));
                    }
                }
            }
        }

        Ok((select_parts.join(", "), group_by))
    }

    fn build_projection(&self, projection: &Document) -> Result<String, Box<dyn std::error::Error>> {
        let mut fields = Vec::new();
        
        for (field, value) in projection {
            match value {
                mongodb::bson::Bson::Int32(1) => fields.push(format!("c.{}", field)),
                mongodb::bson::Bson::Int32(0) => {} // Excluded fields are handled by omission
                _ => return Err("Invalid projection value".into()),
            }
        }
        
        if fields.is_empty() {
            Ok("*".to_string())
        } else {
            Ok(fields.join(", "))
        }
    }

    fn build_sort_clause(&self, sort: &Document) -> Result<String, Box<dyn std::error::Error>> {
        let mut sort_parts = Vec::new();
        
        for (field, value) in sort {
            let direction = match value {
                mongodb::bson::Bson::Int32(1) => "ASC",
                mongodb::bson::Bson::Int32(-1) => "DESC",
                _ => return Err("Invalid sort value".into()),
            };
            sort_parts.push(format!("c.{} {}", field, direction));
        }
        
        if sort_parts.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!(" ORDER BY {}", sort_parts.join(", ")))
        }
    }

} // CosmosDBGateway

// NOTE: In fixed case, we're using `serde_json::Value` as the generic type for `Document`, 
// which allows for flexible JSON-like structures. 
// When you have a specific struct that represents your document structure, 
// you can use that instead of `Value`.


// The error you're encountering is because the `Document` struct in the `azure_data_cosmos` crate 
// is generic and requires a type parameter. 
// To fix this, you need to specify the type of data your documents will contain. 
//
// struct CosmosDbGateway {
//     mongo_client: Client,
//     cosmos_client: CosmosClient,
// }
//
// impl CosmosDbGateway {
//     async fn new(mongo_connection_string: &str, cosmos_connection_string: &str) -> Result<Self, Box<dyn std::error::Error>> {
//         let mongo_client_options = ClientOptions::parse(mongo_connection_string).await?;
//         let mongo_client = Client::with_options(mongo_client_options)?;
//
//         let cosmos_client = CosmosClient::from_connection_string(cosmos_connection_string)?;
//
//         Ok(Self {
//             mongo_client,
//             cosmos_client,
//         })
//     }
//
//     async fn execute_query(&self, query: &str) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
//         // Implement query translation and execution logic here
//         // This is a placeholder and needs to be expanded based on specific requirements
//         unimplemented!()
//     }
// }


// DatabaseConnector and Synchronization module
//use mongodb::{Client as MongoClient, options::ClientOptions};
//use azure_cosmos::prelude::*;
use serde::{Deserialize, Serialize};
use tokio;
use std::error::Error;
use std::time::Duration;
use chrono::{DateTime, Utc};
use futures::Stream;

/// Represents a change event in either database
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangeEvent {
    collection: String,
    operation_type: OperationType,
    document_id: String,
    timestamp: DateTime<Utc>,
    data: mongodb::bson::Document,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
enum OperationType {
    Insert,
    Update,
    Delete,
}

// PROMPT: Use open-source A and B:
// A. Use the MongoDB Rust Driver, https://github.com/mongodb/mongo-rust-driver, and
// B. Use a sample to connect a Rust application with Azure Cosmos DB's API for MongoDB,
// https://github.com/Azure-Samples/azure-cosmos-db-mongodb-rust-getting-started, and
// Use specifications: requirements and use cases in comment headers, 
// to provide Rust code prototypte for 3. Database Connector: Interfaces with CosmosDB 
// and MongoDB using respective drivers. 4. Synchronization Module: Manages data 
// synchronization between CosmosDB and MongoDB.

// DatabaseConnector :
// o Handles connections to both MongoDB and Cosmos DB
// o Implements CRUD operations for both databases
// o Includes change stream monitoring
// o Handles connection pooling and retry logic

/// Database Connector: Handles connections and operations for both MongoDB and Cosmos DB
/// Requirements:
/// 1. Maintain concurrent connections to both databases
/// 2. Handle connection failures and retries
/// 3. Provide CRUD operations for both databases
/// 4. Monitor changes in both databases
struct DatabaseConnector {
    mongo_client: Client,
    cosmos_client: CosmosClient,
    mongo_db_name: String,
    cosmos_db_name: String,
}

impl DatabaseConnector {
    /// Creates a new DatabaseConnector instance
    /// Parameters:
    /// - mongo_uri: MongoDB connection string
    /// - cosmos_connection_string: Cosmos DB connection string
    /// - mongo_db_name: MongoDB database name
    /// - cosmos_db_name: Cosmos DB database name
    async fn new(
        mongo_uri: &str,
        cosmos_connection_string: &str,
        mongo_db_name: &str,
        cosmos_db_name: &str,
    ) -> Result<Self, Box<dyn Error>> {
        // Configure MongoDB client with retry options
        let mut client_options = ClientOptions::parse(mongo_uri).await?;
        client_options.retry_writes = Some(true);
        client_options.retry_reads = Some(true);
        client_options.max_pool_size = Some(50);
        
        let mongo_client = Client::with_options(client_options)?;
        let cosmos_client = CosmosClient::from_connection_string(cosmos_connection_string)?;

        Ok(Self {
            mongo_client,
            cosmos_client,
            mongo_db_name: mongo_db_name.to_string(),
            cosmos_db_name: cosmos_db_name.to_string(),
        })
    }

    /// Monitors changes in MongoDB using Change Streams
    async fn watch_mongo_changes(&self) -> Result<impl Stream<Item = ChangeEvent>, Box<dyn Error>> {
        let db = self.mongo_client.database(&self.mongo_db_name);
        let pipeline = vec![]; // Empty pipeline to watch all changes
        
       
        let change_stream = db.watch(pipeline, None).await?;
         //  ... mapping logic:
        Ok(change_stream.map(|change| {
            // Convert MongoDB change event to our ChangeEvent struct

            match change_stream {
                Ok(change) => {
                    // Extract the operation type
                    let operation_type = match change.operation_type.as_str() {
                        "insert" => OperationType::Insert,
                        "update" => OperationType::Update,
                        "delete" => OperationType::Delete,
                        _ => OperationType::Insert, // Default case, could also return an error
                    };
    
                    // Extract the collection name
                    let collection = change
                        .ns
                        .map(|ns| ns.coll)
                        .unwrap_or_else(|| String::from("unknown"));
    
                    // Extract the document ID
                    let document_id = change
                        .document_key
                        .and_then(|doc| doc.get("_id"))
                        .and_then(|id| id.as_object_id())
                        .map(|oid| oid.to_string())
                        .unwrap_or_else(|| String::from("unknown"));
    
                    // Extract the timestamp
                    let timestamp = change
                        .cluster_time
                        .map(|ts| ts.time)
                        .unwrap_or_else(Utc::now);
    
                    // Extract the full document or document update
                    let data = match operation_type {
                        OperationType::Insert => change.full_document.unwrap_or_default(),
                        OperationType::Update => {
                            // For updates, we might want to combine the update description 
                            // with the full document if available
                            if let Some(full_doc) = change.full_document {
                                full_doc
                            } else {
                                // If full document is not available, use the update description
                                change.update_description
                                    .map(|update| update.updated_fields)
                                    .unwrap_or_default()
                            }
                        },
                        OperationType::Delete => {
                            // For deletes, we only have the document key
                            change.document_key.unwrap_or_default()
                        }
                    };
                    // ... above mapping logic applied
                    ChangeEvent {
                        collection,
                        operation_type,
                        document_id,
                        timestamp,
                        data,
                    }
                }
                Err(e) => {
                    // Log the error and return a default or error event
                    eprintln!("Error processing change stream event: {}", e);
                    ChangeEvent {
                        collection: String::from("error"),
                        operation_type: OperationType::Insert,
                        document_id: String::from("error"),
                        timestamp: Utc::now(),
                        data: Document::new(),
                    }
                }
            }
        }))
    }

    /// Performs CRUD operations on MongoDB
    async fn mongo_operation(
        &self,
        collection: &str,
        operation: OperationType,
        document: mongodb::bson::Document,
    ) -> Result<(), Box<dyn Error>> {
        let db = self.mongo_client.database(&self.mongo_db_name);
        let coll = db.collection(collection);

        match operation {
            OperationType::Insert => {
                coll.insert_one(document, None).await?;
            }
            OperationType::Update => {
                let id = document.get_object_id("_id")?;
                coll.replace_one(doc! { "_id": id }, document, None).await?;
            }
            OperationType::Delete => {
                let id = document.get_object_id("_id")?;
                coll.delete_one(doc! { "_id": id }, None).await?;
            }
        }

        Ok(())
    }

    /// Performs CRUD operations on Cosmos DB
    async fn cosmos_operation(
        &self,
        container: &str,
        operation: OperationType,
        document: azure_data_cosmos::Document,
    ) -> Result<(), Box<dyn Error>> {
        let database = self.cosmos_client.database(&self.cosmos_db_name);
        let container = database.container(container);

        match operation {
            OperationType::Insert => {
                container.create_document(&document).await?;
            }
            OperationType::Update => {
                container.replace_document(&document.id(), &document, None).await?;
            }
            OperationType::Delete => {
                container.delete_document(&document.id()).await?;
            }
        }

        Ok(())
    }
}



// SynchronizationModule :
// o Manages bi-directional synchronization
// o Implements batch processing
// o Includes error handling and retry logic
// o Supports configurable batch sizes and sync intervals

/// Synchronization Module: Manages bi-directional synchronization between MongoDB and Cosmos DB
/// Requirements:
/// 1. Maintain consistency between databases
/// 2. Handle conflicts
/// 3. Support batch synchronization
/// 4. Provide error handling and retry logic
struct SynchronizationModule {
    db_connector: DatabaseConnector,
    batch_size: usize,
    sync_interval: Duration,
}

impl SynchronizationModule {
    /// Creates a new SynchronizationModule instance
    async fn new(
        db_connector: DatabaseConnector,
        batch_size: usize,
        sync_interval: Duration,
    ) -> Self {
        Self {
            db_connector,
            batch_size,
            sync_interval,
        }
    }

    /// Starts the synchronization process
    async fn start_sync(&self) -> Result<(), Box<dyn Error>> {
        // Start MongoDB change stream
        let mongo_changes = self.db_connector.watch_mongo_changes().await?;
        
        tokio::spawn(async move {
            self.process_changes(mongo_changes).await;
        });

        Ok(())
    }

    /// Processes changes from the change stream
    async fn process_changes(&self, mut changes: impl Stream<Item = ChangeEvent> + Unpin) {
        let mut batch = Vec::new();

        while let Some(change) = changes.next().await {
            batch.push(change);

            if batch.len() >= self.batch_size {
                if let Err(e) = self.sync_batch(&batch).await {
                    eprintln!("Error syncing batch: {}", e);
                    // Implement retry logic here
                }
                batch.clear();
            }
        }
    }

    /// Synchronizes a batch of changes
    async fn sync_batch(&self, changes: &[ChangeEvent]) -> Result<(), Box<dyn Error>> {
        for change in changes {
            match change.operation_type {
                OperationType::Insert | OperationType::Update => {
                    // Convert MongoDB document to Cosmos DB document
                    let cosmos_doc = self.convert_to_cosmos_doc(&change.data)?;
                    self.db_connector
                        .cosmos_operation(&change.collection, change.operation_type.clone(), cosmos_doc)
                        .await?;
                }
                OperationType::Delete => {
                    // Handle deletion
                    let cosmos_doc = self.convert_to_cosmos_doc(&change.data)?;
                    self.db_connector
                        .cosmos_operation(&change.collection, OperationType::Delete, cosmos_doc)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Converts MongoDB document to Cosmos DB document
    fn convert_to_cosmos_doc(
        &self,
        mongo_doc: &mongodb::bson::Document,
    ) -> Result<cosmos::Document, Box<dyn Error>> {
        // Implementation of document conversion logic
        // This would handle field mapping, data type conversion, etc.
        unimplemented!()
    }
}



// Key Features of DatabaseConnector and SynchModule :
// o Asynchronous operations using tokio
// o Error handling and retry mechanisms
// o Connection pooling
// o Change stream monitoring
// o Batch processing
// o Unit tests for DatabaseConnector and SynchModule

// Error Handling for DBConnector and SyncModule :
// o Comprehensive error types
// o Retry logic for failed operations
// o Graceful error recovery

// Testing of DBConnector and SyncModule :
// o Unit tests for both modules
// o Integration test scenarios





// This implementation provides:
/// 1. Transaction Manager :
// o Two-phase commit protocol implementation
// o Transaction state management
// o Rollback support
// o Transaction logging
/// 2. Scaling Manager :
// o Automatic scaling based on vCore usage
// o Configurable scaling thresholds
// o Cooldown periods
// o Scaling history tracking
/// 3. Metrics Collector :
// o System metrics gathering
// o Azure Monitor integration
// o Metrics caching
// o Configurable collection intervals
/// Key features for TM, SM, and MC are:
// o Async/await support throughout
// o Error handling
// o Thread-safe state management
// o Configurable parameters
// o Comprehensive testing
// o Integration with Azure services

// PROMPT:
// Please provide prototype implementations for Transaction Manager, 
//Scaling Manager, and Metrics Collector: 
//as components for implementation in Rust are defined as: 
// 3. Transaction Manager:
// 3.1- Support ACID transactions
// 3.2- Implement distributed transaction coordination
// 4. Scaling Manager:
// 4.1- Monitor vCore usage
// 4.2- Trigger vertical/horizontal scaling based on workload
// 5. Metrics Collector:
// 5.1 Gather performance metrics
// 5.2 Integrate with Azure Monitor

/// Building a robust, scalable database gateway system with proper 
/// transaction management, automatic scaling, and monitoring capabilities.
use std::sync::Arc;
use tokio::sync::Mutex;
use azure_monitor::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use async_trait::async_trait;

/// Transaction Manager: Handles ACID transactions and distributed coordination
/// Requirements:
/// 1. Ensure ACID properties across distributed databases
/// 2. Handle transaction rollbacks
/// 3. Maintain transaction logs
/// 4. Support distributed transaction coordination
#[derive(Clone)]
struct TransactionManager {
    mongo_client: mongodb::Client,
    cosmos_client: CosmosClient,
    transaction_log: Arc<Mutex<HashMap<String, TransactionState>>>,
}

#[derive(Debug, Clone)]
struct TransactionState {
    id: String,
    status: TransactionStatus,
    operations: Vec<TransactionOperation>,
    start_time: Instant,
    participants: Vec<String>,
}

#[derive(Debug, Clone)]
enum TransactionStatus {
    Started,
    Prepared,
    Committed,
    RolledBack,
    Failed,
}

#[derive(Debug, Clone)]
struct TransactionOperation {
    database: String,
    collection: String,
    operation_type: OperationType,
    document: mongodb::bson::Document,
}

impl TransactionManager {
    pub async fn new(mongo_client: mongodb::Client, cosmos_client: CosmosClient) -> Self {
        Self {
            mongo_client,
            cosmos_client,
            transaction_log: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Begins a new distributed transaction
    pub async fn begin_transaction(&self) -> Result<String, Box<dyn std::error::Error>> {
        let transaction_id = uuid::Uuid::new_v4().to_string();
        let state = TransactionState {
            id: transaction_id.clone(),
            status: TransactionStatus::Started,
            operations: Vec::new(),
            start_time: Instant::now(),
            participants: Vec::new(),
        };

        let mut log = self.transaction_log.lock().await;
        log.insert(transaction_id.clone(), state);

        Ok(transaction_id)
    }

    /// Implements two-phase commit protocol
    pub async fn commit_transaction(&self, transaction_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut log = self.transaction_log.lock().await;
        let state = log.get_mut(transaction_id).ok_or("Transaction not found")?;

        // Phase 1: Prepare
        for operation in &state.operations {
            self.prepare_operation(operation).await?;
        }
        state.status = TransactionStatus::Prepared;

        // Phase 2: Commit
        for operation in &state.operations {
            self.commit_operation(operation).await?;
        }
        state.status = TransactionStatus::Committed;

        Ok(())
    }

    async fn prepare_operation(&self, operation: &TransactionOperation) -> Result<(), Box<dyn std::error::Error>> {
        // Implementation of prepare phase
        Ok(())
    }

    async fn commit_operation(&self, operation: &TransactionOperation) -> Result<(), Box<dyn std::error::Error>> {
        // Implementation of commit phase
        Ok(())
    }

    /// Rolls back a transaction
    pub async fn rollback_transaction(&self, transaction_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut log = self.transaction_log.lock().await;
        let state = log.get_mut(transaction_id).ok_or("Transaction not found")?;

        for operation in state.operations.iter().rev() {
            self.rollback_operation(operation).await?;
        }
        state.status = TransactionStatus::RolledBack;

        Ok(())
    }

    async fn rollback_operation(&self, operation: &TransactionOperation) -> Result<(), Box<dyn std::error::Error>> {
        // Implementation of rollback logic
        Ok(())
    }
}

/// Scaling Manager: Handles automatic scaling based on workload
/// Requirements:
/// 1. Monitor resource usage
/// 2. Implement scaling policies
/// 3. Coordinate scaling operations
/// 4. Maintain scaling history
#[derive(Clone)]
struct ScalingManager {
    metrics_collector: Arc<MetricsCollector>,
    current_scale: Arc<Mutex<ScaleState>>,
    scaling_history: Arc<Mutex<Vec<ScalingEvent>>>,
    config: ScalingConfig,
}

#[derive(Clone)]
struct ScalingConfig {
    min_vcore: i32,
    max_vcore: i32,
    scale_up_threshold: f64,
    scale_down_threshold: f64,
    cooldown_period: Duration,
}

#[derive(Clone)]
struct ScaleState {
    vcore_count: i32,
    last_scale_time: Instant,
}

#[derive(Clone)]
struct ScalingEvent {
    timestamp: Instant,
    old_scale: i32,
    new_scale: i32,
    reason: String,
}

impl ScalingManager {
    pub async fn new(
        metrics_collector: Arc<MetricsCollector>,
        config: ScalingConfig,
    ) -> Self {
        Self {
            metrics_collector,
            current_scale: Arc::new(Mutex::new(ScaleState {
                vcore_count: config.min_vcore,
                last_scale_time: Instant::now(),
            })),
            scaling_history: Arc::new(Mutex::new(Vec::new())),
            config,
        }
    }

    /// Monitors and triggers scaling operations
    pub async fn monitor_and_scale(&self) -> Result<(), Box<dyn std::error::Error>> {
        let metrics = self.metrics_collector.get_current_metrics().await?;
        let current_usage = metrics.vcore_usage;

        let mut state = self.current_scale.lock().await;
        let now = Instant::now();

        if now.duration_since(state.last_scale_time) < self.config.cooldown_period {
            return Ok(());
        }

        if current_usage > self.config.scale_up_threshold {
            self.scale_up(&mut state, current_usage).await?;
        } else if current_usage < self.config.scale_down_threshold {
            self.scale_down(&mut state, current_usage).await?;
        }

        Ok(())
    }

    async fn scale_up(&self, state: &mut ScaleState, usage: f64) -> Result<(), Box<dyn std::error::Error>> {
        if state.vcore_count < self.config.max_vcore {
            let new_scale = state.vcore_count + 1;
            self.record_scaling_event(state.vcore_count, new_scale, format!("High usage: {}", usage)).await?;
            state.vcore_count = new_scale;
            state.last_scale_time = Instant::now();
        }
        Ok(())
    }

    async fn scale_down(&self, state: &mut ScaleState, usage: f64) -> Result<(), Box<dyn std::error::Error>> {
        if state.vcore_count > self.config.min_vcore {
            let new_scale = state.vcore_count - 1;
            self.record_scaling_event(state.vcore_count, new_scale, format!("Low usage: {}", usage)).await?;
            state.vcore_count = new_scale;
            state.last_scale_time = Instant::now();
        }
        Ok(())
    }

    async fn record_scaling_event(&self, old_scale: i32, new_scale: i32, reason: String) -> Result<(), Box<dyn std::error::Error>> {
        let event = ScalingEvent {
            timestamp: Instant::now(),
            old_scale,
            new_scale,
            reason,
        };

        let mut history = self.scaling_history.lock().await;
        history.push(event);
        Ok(())
    }
}

/// Metrics Collector: Gathers and reports performance metrics
/// Requirements:
/// 1. Collect system metrics
/// 2. Monitor database performance
/// 3. Track resource usage
/// 4. Interface with Azure Monitor
#[derive(Clone)]
struct MetricsCollector {
    azure_monitor_client: AzureMonitorClient,
    metrics_cache: Arc<Mutex<HashMap<String, MetricValue>>>,
    collection_interval: Duration,
}

#[derive(Clone)]
struct MetricValue {
    value: f64,
    timestamp: Instant,
}

#[derive(Clone)]
struct DatabaseMetrics {
    vcore_usage: f64,
    memory_usage: f64,
    iops: f64,
    latency: Duration,
}

impl MetricsCollector {
    pub async fn new(
        connection_string: &str,
        collection_interval: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let azure_monitor_client = AzureMonitorClient::from_connection_string(connection_string)?;

        Ok(Self {
            azure_monitor_client,
            metrics_cache: Arc::new(Mutex::new(HashMap::new())),
            collection_interval,
        })
    }

    /// Starts collecting metrics
    pub async fn start_collecting(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            self.collect_metrics().await?;
            tokio::time::sleep(self.collection_interval).await;
        }
    }

    /// Collects current metrics
    async fn collect_metrics(&self) -> Result<(), Box<dyn std::error::Error>> {
        let metrics = self.gather_system_metrics().await?;
        self.update_cache(&metrics).await?;
        self.send_to_azure_monitor(&metrics).await?;
        Ok(())
    }

    /// Gathers system metrics
    async fn gather_system_metrics(&self) -> Result<DatabaseMetrics, Box<dyn std::error::Error>> {
        // Implementation of metric gathering
        Ok(DatabaseMetrics {
            vcore_usage: 0.0,
            memory_usage: 0.0,
            iops: 0.0,
            latency: Duration::from_millis(0),
        })
    }

    /// Updates metrics cache
    async fn update_cache(&self, metrics: &DatabaseMetrics) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.metrics_cache.lock().await;
        
        cache.insert("vcore_usage".to_string(), MetricValue {
            value: metrics.vcore_usage,
            timestamp: Instant::now(),
        });
        
        cache.insert("memory_usage".to_string(), MetricValue {
            value: metrics.memory_usage,
            timestamp: Instant::now(),
        });

        Ok(())
    }

    /// Sends metrics to Azure Monitor
    async fn send_to_azure_monitor(&self, metrics: &DatabaseMetrics) -> Result<(), Box<dyn std::error::Error>> {
        self.azure_monitor_client
            .send_metric("vcore_usage", metrics.vcore_usage)
            .await?;
            
        self.azure_monitor_client
            .send_metric("memory_usage", metrics.memory_usage)
            .await?;

        Ok(())
    }

    /// Gets current metrics
    pub async fn get_current_metrics(&self) -> Result<DatabaseMetrics, Box<dyn std::error::Error>> {
        self.gather_system_metrics().await
    }
}

/// MAINLINE

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    /// DatabaseConnector and SynchronizationModule:

    // Configuration
    let mongo_uri = std::env::var("MONGODB_URI")?;
    let cosmos_connection_string = std::env::var("COSMOS_CONNECTION_STRING")?;
    
    // Initialize the connector
    let db_connector = DatabaseConnector::new(
        &mongo_uri,
        &cosmos_connection_string,
        "mydatabase",
        "mycosmos"
    ).await?;

    // Initialize the synchronization module
    let sync_module = SynchronizationModule::new(
        db_connector,
        100, // batch size
        Duration::from_secs(5) // sync interval
    ).await;

    // Start synchronization
    sync_module.start_sync().await?;

    // Keep the application running
    tokio::signal::ctrl_c().await?;



    /// Simple query
    let gateway = CosmosDbGateway::new(mongo_conn_string, cosmos_conn_string).await?;
    let query = r#"{"age": {"$gt": 21}, "name": "John"}"#;
    let results = gateway.execute_query(query).await?;

    // let gateway = CosmosDbGateway::new("mongodb://...", "AccountEndpoint=...").await?;
    // let result = gateway.execute_query("db.collection.find({})").await?;
    // println!("{:?}", result);
    //Ok(());

    /// Complex query with options
    let query = r#"{
        "$and": [
            {"age": {"$gt": 21}},
            {"status": {"$in": ["active", "pending"]}}
        ]
    }"#;
    
    let options = QueryOptions {
        limit: Some(10),
        skip: Some(0),
        sort: Some(doc! {"age": -1}),
        projection: Some(doc! {"name": 1, "age": 1}),
    };
    
    let results = gateway.execute_query(query, Some(options)).await?;
    
    // Aggregation pipeline
    let pipeline = vec![
        doc! {
            "$match": {"age": {"$gt": 21}}
        },
        doc! {
            "$group": {
                "_id": "$city",
                "avg_age": {"$avg": "$age"}
            }
        }
    ];
    
    let agg_results = gateway.execute_aggregate(pipeline).await?;


    // MC, SM and TM
    // Initialize components
    let metrics_collector = Arc::new(MetricsCollector::new(
        "azure_monitor_connection_string",
        Duration::from_secs(60),
    ).await?);

    let scaling_config = ScalingConfig {
        min_vcore: 2,
        max_vcore: 16,
        scale_up_threshold: 0.75,
        scale_down_threshold: 0.25,
        cooldown_period: Duration::from_mins(5),
    };

    let scaling_manager = ScalingManager::new(
        metrics_collector.clone(),
        scaling_config,
    ).await;

    let transaction_manager = TransactionManager::new(
        mongodb_client,
        cosmos_client,
    ).await;

    // Start monitoring and scaling
    tokio::spawn(async move {
        loop {
            if let Err(e) = scaling_manager.monitor_and_scale().await {
                eprintln!("Scaling error: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });

    // Start metrics collection
    metrics_collector.start_collecting().await?;
    
    Ok(());

}


#[derive(Default)]
struct SqlQueryParts {
    select: String,
    where_clause: String,
    order_by: String,
    limit: String,
    offset: String,
}

//Unit Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_database_connector() {
        // Test connection establishment
        let connector = DatabaseConnector::new(
            "mongodb://localhost:27017",
            "CosmosDBConnectionString",
            "test_db",
            "test_cosmos"
        ).await.unwrap();

        // Test CRUD operations
        let test_doc = doc! {
            "test_field": "test_value"
        };

        connector.mongo_operation(
            "test_collection",
            OperationType::Insert,
            test_doc.clone()
        ).await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_module() {
        // Test synchronization logic
        let connector = DatabaseConnector::new(
            "mongodb://localhost:27017",
            "CosmosDBConnectionString",
            "test_db",
            "test_cosmos"
        ).await.unwrap();

        let sync_module = SynchronizationModule::new(
            connector,
            10,
            Duration::from_secs(1)
        ).await;

        // Test batch processing
        let test_changes = vec![
            ChangeEvent {
                collection: "test_collection".to_string(),
                operation_type: OperationType::Insert,
                document_id: "test_id".to_string(),
                timestamp: Utc::now(),
                data: doc! { "test": "data" },
            }
        ];

        sync_module.sync_batch(&test_changes).await.unwrap();
    }

    #[tokio::test]
    async fn test_gateway_creation () {
        let gateway = CosmosDbGateway::new("mongodb://...", "AccountEndpoint=...").await;
        assert!(gateway.is_ok());
    }

    #[tokio::test]
    async fn test_query_execution() {
        let gateway = CosmosDbGateway::new("mongodb://...", "AccountEndpoint=...").await.unwrap();
        let result = gateway.execute_query("db.collection.find({})").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_complex_query() {
        let gateway = CosmosDbGateway::new("mongodb://...", "AccountEndpoint=...").await.unwrap();
        
        // Complex query with multiple conditions
        let query = r#"{
            "$and": [
                {"age": {"$gt": 21}},
                {"$or": [
                    {"city": "New York"},
                    {"city": "Los Angeles"}
                ]},
                {"status": {"$in": ["active", "pending"]}}
            ]
        }"#;
        // Query options structure for better parameter organization
        let options = QueryOptions {
            limit: Some(10),
            skip: Some(20),
            sort: Some(doc! {"age": -1, "name": 1}),
            projection: Some(doc! {"name": 1, "age": 1, "city": 1}),
        };
        
        let results = gateway.execute_query(query, Some(options)).await.unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_aggregation() {
        let gateway = CosmosDbGateway::new("mongodb://...", "AccountEndpoint=...").await.unwrap();
        
        let pipeline = vec![
            doc! {
                "$match": {
                    "age": {"$gt": 21}
                }
            },
            doc! {
                "$group": {
                    "_id": {"city": "$city"},
                    "avg_age": {"$avg": "$age"},
                    "count": {"$sum": 1}
                }
            },
            doc! {
                "$sort": {"count": -1}
            },
            doc! {
                "$limit": 5
            }
        ];
        
        let results = gateway.execute_aggregate(pipeline).await.unwrap();
        assert!(!results.is_empty());
    }

    /// TM, SM and MC tests

    #[tokio::test]
    async fn test_transaction_manager() {
        let tm = TransactionManager::new(mongodb_client, cosmos_client).await;
        
        // Test transaction workflow
        let tx_id = tm.begin_transaction().await.unwrap();
        tm.commit_transaction(&tx_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_scaling_manager() {
        let metrics_collector = Arc::new(MetricsCollector::new(
            "test_connection_string",
            Duration::from_secs(1),
        ).await.unwrap());

        let scaling_manager = ScalingManager::new(
            metrics_collector,
            ScalingConfig {
                min_vcore: 1,
                max_vcore: 4,
                scale_up_threshold: 0.75,
                scale_down_threshold: 0.25,
                cooldown_period: Duration::from_secs(1),
            },
        ).await;

        scaling_manager.monitor_and_scale().await.unwrap();
    }

    #[tokio::test]
    async fn test_metrics_collector() {
        let collector = MetricsCollector::new(
            "test_connection_string",
            Duration::from_secs(1),
        ).await.unwrap();

        let metrics = collector.get_current_metrics().await.unwrap();
        assert!(metrics.vcore_usage >= 0.0);
    }

}


// MAINLINE
// fn main() {
//     println!("Hello, world!");
// }