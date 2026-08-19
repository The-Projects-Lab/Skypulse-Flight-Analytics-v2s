import os
import tempfile

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


os.environ.setdefault(
    "SPARK_LOCAL_IP",
    "127.0.0.1"
)

os.environ.setdefault(
    "SPARK_LOCAL_HOSTNAME",
    "localhost"
)


def create_spark_session(
    app_name: str = "SkyPulse Aviation Analytics",
    enable_delta: bool = True
):
    """
    Create a local SparkSession configured with Delta Lake.
    """

    ivy_cache = os.path.join(
        tempfile.gettempdir(),
        "skypulse-spark-ivy"
    )

    os.makedirs(
        ivy_cache,
        exist_ok=True
    )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.driver.host",
            "127.0.0.1"
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1"
        )
        .config(
            "spark.jars.ivy",
            ivy_cache
        )
    )

    if enable_delta:

        builder = (
            builder
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )

        spark = configure_spark_with_delta_pip(
            builder
        ).getOrCreate()

    else:

        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark
