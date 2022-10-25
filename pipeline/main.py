from subflows import ingest_source_data
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from datetime import timedelta
from _settings import PROJECT_NAME, INGEST_SOURCE_DATA


@flow(
    name=PROJECT_NAME,
    version="test 1.0",
    task_runner=SequentialTaskRunner(),
    # description="Project {name} Data Pipeline".format(name=settings.project),
    # retries= 2 number of times to retry on flow run failure.
    # retry_delay_seconds number of seconds to wait before retrying the flow after failure
    # timeout_seconds number of seconds indicating a maximum runtime for the flow
    # validate_parameters = True parameters passed to flows are validated by Pydantic
    # version version string for the flow
)
def main_flow():
    """pipeline main flow

    Parameters
    ----------
    config : dict
        project yaml file
    """
    logger = get_run_logger()
    logger.info(
        f"""
-----------------------------------------------------------------------------------------------------------------------------------------------
                                        {PROJECT_NAME} Main Flow Started
-----------------------------------------------------------------------------------------------------------------------------------------------
        """
    )
    ingest_source_data(ingest_config=INGEST_SOURCE_DATA)

    logger.info(
        f"""
-----------------------------------------------------------------------------------------------------------------------------------------------
                                        {PROJECT_NAME} Main Flow Finished
-----------------------------------------------------------------------------------------------------------------------------------------------
        """
    )


if __name__ == "__main__":
    main_flow()
