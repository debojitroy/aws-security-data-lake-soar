import typing
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda as _lambda,
    aws_s3 as s3,
    App, Duration, Stack
)
from constructs import Construct


class AwsSecurityDataLakeSoarStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Lambda Handlers Definitions

        # submit_lambda = _lambda.Function(self, 'submitLambda',
        #                                  handler='lambda_function.lambda_handler',
        #                                  runtime=typing.cast(_lambda.Runtime, _lambda.Runtime.PYTHON_3_11),
        #                                  code=_lambda.Code.from_asset('lambdas/submit'))

        # Success State for workflow
        success_state = sfn.Succeed(self, "Success State")

        # Fail State for workflow
        fail_state = sfn.Fail(self, "Fail State")

        # Start the Query Execution
        start_query_execution_job = tasks.AthenaStartQueryExecution(self, "Start Athena Query",
                                                                    query_string=sfn.JsonPath.string_at(
                                                                        "$.queryString"),
                                                                    query_execution_context=tasks.QueryExecutionContext(
                                                                        database_name=sfn.JsonPath.string_at(
                                                                            "$.database")
                                                                    ),
                                                                    result_configuration=tasks.ResultConfiguration(
                                                                        encryption_configuration=tasks.EncryptionConfiguration(
                                                                            encryption_option=tasks.EncryptionOption.S3_MANAGED
                                                                        ),
                                                                        output_location=s3.Location(
                                                                            bucket_name="sdl-athena-query-results",
                                                                            object_key="guardduty"
                                                                        )
                                                                    ))

        # Get the Query Execution Status
        get_query_execution_status = tasks.AthenaGetQueryExecution(self, "Get Athena Query Status",
                                                                   query_execution_id=sfn.JsonPath.string_at(
                                                                       "$.QueryExecutionId"))

        # Get Query Execution Results
        get_query_execution_results = tasks.AthenaGetQueryResults(self, "Get Athena Query Results",
                                                                  query_execution_id=sfn.JsonPath.string_at(
                                                                      "$.QueryExecutionId"))

        # Wait for 30 seconds
        wait_state = sfn.Wait(self, "Wait for Query Completion",
                              time=sfn.WaitTime.duration(Duration.seconds(30))).next(get_query_execution_status)

        # Choice to compare Execution Status
        choice = sfn.Choice(self, "Check Query Status")

        choice.when(sfn.Condition.string_equals("$.QueryExecution.Status.State", "FAILED"), fail_state)
        choice.when(sfn.Condition.string_equals("$.QueryExecution.Status.State", "SUCCEEDED"),
                    get_query_execution_results)
        choice.otherwise(wait_state)

        # Workflow Definition
        workflow_definition = start_query_execution_job \
            .next(get_query_execution_status) \
            .next(choice)

        # Workflow Execution
        workflow_execution = sfn.StateMachine(self, "StateMachine-SDL-SOAR",
                                              definition=workflow_definition,
                                              timeout=Duration.minutes(5))
