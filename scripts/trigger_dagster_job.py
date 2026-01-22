#!/usr/bin/env python
"""
Trigger a Dagster job via GraphQL API.
"""
import sys
import requests
import json

DAGSTER_URL = "http://localhost:13000/graphql"

def trigger_job(job_name: str, run_config: dict = None) -> dict:
    """Trigger a Dagster job and return the result."""
    mutation = """
    mutation LaunchRun($jobName: String!, $runConfigData: RunConfigData) {
        launchRun(executionParams: {
            selector: {
                jobName: $jobName,
                repositoryName: "__repository__",
                repositoryLocationName: "unified_mlops_platform"
            }
            runConfigData: $runConfigData
        }) {
            __typename
            ... on LaunchRunSuccess {
                run { runId status }
            }
            ... on PythonError {
                message
                stack
            }
            ... on RunConfigValidationInvalid {
                errors { message reason }
            }
            ... on InvalidSubsetError {
                message
            }
        }
    }
    """

    variables = {"jobName": job_name}
    if run_config:
        variables["runConfigData"] = run_config

    response = requests.post(
        DAGSTER_URL,
        json={"query": mutation, "variables": variables}
    )

    return response.json()


def wait_for_run(run_id: str, timeout: int = 300) -> dict:
    """Wait for a run to complete."""
    import time

    query = """
    query RunStatus($runId: ID!) {
        runOrError(runId: $runId) {
            __typename
            ... on Run {
                runId
                status
            }
            ... on PythonError {
                message
            }
        }
    }
    """

    start_time = time.time()
    while time.time() - start_time < timeout:
        response = requests.post(
            DAGSTER_URL,
            json={"query": query, "variables": {"runId": run_id}}
        )

        result = response.json()
        run_data = result.get("data", {}).get("runOrError", {})
        status = run_data.get("status", "UNKNOWN")

        if status in ["SUCCESS", "FAILURE", "CANCELED"]:
            return {"status": status, "run_id": run_id}

        print(f"  Run {run_id[:8]}... status: {status}")
        time.sleep(5)

    return {"status": "TIMEOUT", "run_id": run_id}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python trigger_dagster_job.py <job_name> [--wait]")
        sys.exit(1)

    job_name = sys.argv[1]
    wait = "--wait" in sys.argv

    print(f"Triggering job: {job_name}")
    result = trigger_job(job_name)

    if "errors" in result:
        print(f"Error: {result['errors']}")
        sys.exit(1)

    launch_result = result.get("data", {}).get("launchRun", {})
    typename = launch_result.get("__typename", "")

    if typename == "LaunchRunSuccess":
        run_id = launch_result["run"]["runId"]
        print(f"✅ Triggered: {run_id}")

        if wait:
            print("Waiting for completion...")
            final_result = wait_for_run(run_id)
            print(f"Final status: {final_result['status']}")
            sys.exit(0 if final_result['status'] == "SUCCESS" else 1)
    else:
        print(f"Failed: {launch_result}")
        sys.exit(1)
