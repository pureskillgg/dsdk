import json

import boto3
import pandas as pd


class SagemakerEndpoint:
    def __init__(self, *, model, log):
        self.model_name = model["model_name"]
        self.model_parameters = model.get("parameters", {})
        self.columns = self.model_parameters.get("columns", [])
        self._endpoint_name = model["endpoint_name"]
        self._content_type = model["req_type"]
        self._res_type = model["res_type"]
        self._res_key = model.get("res_key", "data")
        self._log = log.bind(
            client="sagemaker_endpoint",
            endpoint_name=self._endpoint_name,
            model_name=self.model_name,
        )

    def invoke(self, dataframe):
        self._log.info("Invoke: Start")
        runtime = boto3.client("runtime.sagemaker")
        final_data = self._format_data(dataframe)
        response = runtime.invoke_endpoint(
            EndpointName=self._endpoint_name,
            ContentType=self._content_type,
            Body=final_data,
        )

        return self._transform_sagemaker_output(response)

    def _format_data(self, dataframe):
        if self._content_type == "text/csv":
            final_data = dataframe[self.columns].to_csv(index=False, header=False)
        else:
            raise Exception(f"Unknown content type {self._content_type}")
        return final_data

    def _transform_sagemaker_output(self, response):
        if self._res_type == "application/json":
            results = json.loads(response["Body"].read().decode())
            return pd.DataFrame.from_dict(results.get(self._res_key))

        raise Exception(f"Unknown res type {self._res_type}")
