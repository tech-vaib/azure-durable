🧪 To Run Locally:

    Install required packages:
    
  pip install azure-storage-blob pymongo python-dotenv
    
    2.     Set your Azure Storage connection string:

    Either pass it directly in the code (connection_str = "...")

    Or set it as an environment variable:
    
    export AzureWebJobsStorage="DefaultEndpointsProtocol=...;"
    
    3. Run it:
    python blob_helper.py


-----

you can trigger this using:
POST /api/HttpStart
Authorization: Bearer valid_token_example
Content-Type: application/json

{
  "container_name": "my-container",
  "prefix1": "data/raw/",
  "prefix2": "data/processed/"
}

----

To Run Locally
func start

Ensure you're in the root folder and have installed:
npm install -g azure-functions-core-tools@4
pip install -r requirements.txt

