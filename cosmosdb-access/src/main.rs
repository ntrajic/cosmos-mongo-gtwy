// Querying: The example shows how to perform a basic query using query_documents and iterate through the results 
// using a stream. This is much more efficient than loading all documents into memory at once.
use azure_data_cosmos::prelude::*;
use futures::StreamExt;
use serde_json;

async fn connect_and_query(
    client: CosmosClient,
    database_name: String,
    container_name: String
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database = client.database_client(database_name.clone());
    let container = database.collection_client(container_name.clone());

    // Example: Querying all documents
    let query = "SELECT * FROM c";
    let mut query_results = container.query_documents(query).into_stream::<serde_json::Value>();

    while let Some(result) = query_results.next().await {
        match result {
            Ok(response) => {
                for doc in response.documents() {
                    println!("Document: {:?}", doc);
                }
            }
            Err(err) => {
                eprintln!("Error querying documents: {}", err);
                return Err(err.into());
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Replace with your Cosmos DB credentials
    let account = std::env::var("COSMOS_ACCOUNT").expect("COSMOS_ACCOUNT must be set");
    let key = std::env::var("COSMOS_KEY").expect("COSMOS_KEY must be set");

    let authorization_token = AuthorizationToken::primary_key(&key)?;
    let client = CosmosClient::new(account, authorization_token);

    let database_name = "your_database_name".to_string();
    let container_name = "your_container_name".to_string();

    connect_and_query(client, database_name, container_name).await?;

    Ok(())
}

// ```
// Key changes:
// 1. Changed `container_client` to `collection_client` as the method name has been updated in the latest version of the library[5].
// 2. Modified the client creation process:
//    - Create an `AuthorizationToken` using the primary key.
//    - Use this token to create the `CosmosClient`[5].
// 3. Removed the `?` operator after `CosmosClient::new` as it doesn't return a `Result`[5].
// 4. The `futures::StreamExt` import is now used and necessary for the `next()` method on the stream.
// These changes should resolve the compilation errors you encountered. Make sure to set the `COSMOS_ACCOUNT` and `COSMOS_KEY` environment variables before running the program[1][5].
// Citations:
// [1] https://dev.to/mindflavor/exploring-azure-cosmosdb-with-rust-part-1-1ki7
// [2] https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.cosmosclient.createandinitializeasync?view=azure-dotnet
// [3] https://stackoverflow.com/questions/78286960/how-to-retrieve-document-using-azure-data-cosmos-in-rust
// [4] https://microsoftlearning.github.io/dp-420-cosmos-db-dev/instructions/07-sdk-batch.html
// [5] https://docs.rs/azure_data_cosmos/latest/azure_data_cosmos/
// [6] https://github.com/SylvestreG/cosmos-client
// [7] https://github.com/Azure/azure-sdk-for-rust/issues/1834/linked_closing_reference
// [8] https://learn.microsoft.com/en-us/javascript/api/@azure/cosmos/cosmosclient?view=azure-node-latest

// OUT:
// cargo run
// Finished dev [unoptimized + debuginfo] target(s) in 0.28sn 0.28s
// Running `C:\SRC\Rust\00CosmosMongoGTWY\cosmosdb-access\target\debug\cosmosdb-access.exe`
// thread 'main' panicked at src\main.rs:39:51:
// COSMOS_ACCOUNT must be set: NotPresent
// note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
// error: process didn't exit successfully: `C:\SRC\Rust\00CosmosMongoGTWY\cosmosdb-access\target\debug\cosmosdb-access.exe` (exit code: 101)




