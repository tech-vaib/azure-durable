import azure.functions as func
import azure.durable_functions as df

# This orchestrator receives blob info and bearer token and triggers the activity

def orchestrator_function(context: df.DurableOrchestrationContext):
    inputs = context.get_input()
    return context.call_activity("ProcessBlobActivity", inputs)

main = df.Orchestrator.create(orchestrator_function)

