import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


class ESConnect:
    def __init__(self, url: str):
        self._cnx_info = {
            "es.nodes.wan.only": "true"
        }

    def write(self, es_index: str, df_source: DataFrame):
        try:
            df_source.write.format(
                "org.elasticsearch.spark.sql"
            ).option("es.mapping.id", "code").mode("append").save(path=es_index)
        except Exception as e:
            raise e

    def select(self, spark: SparkSession, es_index: str, df: DataFrame, field_value_dict: dict):
        breakpoint()
        es_query = self.query_builder(df, field_value_dict)
        try:
            df = spark.read.format("org.elasticsearch.spark.sql").options(**self._cnx_info).option("es.query",
                                                                                                   es_query).load(
                es_index)
            return df
        except Exception as e:
            raise e

    def get_field_values_from_df(self, df: DataFrame, col_name: str):
        try:
            field_values = [row[col_name] for row in
                            df.filter(col(col_name).isNotNull()).select(col_name).distinct().collect()]
            return field_values
        except Exception as e:
            raise e

    def should_clause_builder(self, field, selected_field_value):
        should_clauses = []
        for val in selected_field_value:
            should_clauses.append({"match": {field: val}})
        return should_clauses

    def query_builder(self, df: DataFrame, field_value_dict: dict):
        try:
            breakpoint()
            must_clauses = []
            for field, value in field_value_dict.items():
                if isinstance(value, list):
                    selected_field_value = value
                    should_clauses = self.should_clause_builder(field, selected_field_value)
                    must_clauses.append({"bool": {"should": should_clauses, "minimum_should_match": 1}})
                elif value == "*":
                    selected_field_value = self.get_field_values_from_df(df, field)
                    should_clauses = self.should_clause_builder(field, selected_field_value)
                    must_clauses.append({"bool": {"should": should_clauses, "minimum_should_match": 1}})
                else:
                    must_clauses.append({"match": {"field": value}})

            query = {
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                }
            }
            return json.dumps(query)
        except Exception as e:
            raise e
