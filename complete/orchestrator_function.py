import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    inputs = context.get_input()
    yield context.call_activity("ValidateToken", inputs)

    if inputs.get("process_type") == "full":
        files = yield context.call_activity("ProcessFile", inputs)

        chunk_tasks = [context.call_activity("ChunkFile", {
            "container": inputs["container"],
            "file_path": file
        }) for file in files]
        chunk_results = yield context.task_all(chunk_tasks)

        write_tasks = [
            context.call_activity("WriteToDB", res)
            for res in chunk_results
        ]
        yield context.task_all(write_tasks)

    elif inputs.get("process_type") == "chunks_only":
        # Just chunking without DB writes
        files = yield context.call_activity("ProcessFile", inputs)
        chunk_tasks = [context.call_activity("ChunkFile", {
            "container": inputs["container"],
            "file_path": file
        }) for file in files]
        yield context.task_all(chunk_tasks)

    return "Durable pipeline complete"

main = df.Orchestrator.create(orchestrator_function)

