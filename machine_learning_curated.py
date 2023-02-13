import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated with accelerometer data
CustomerCuratedwithaccelerometerdata_node1676137272373 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake/machine_learning/customer_curated_data/"],
            "recurse": True,
        },
        transformation_ctx="CustomerCuratedwithaccelerometerdata_node1676137272373",
    )
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Renamed keys for Join to do privacy filter
RenamedkeysforJointodoprivacyfilter_node1676250924960 = RenameField.apply(
    frame=CustomerCuratedwithaccelerometerdata_node1676137272373,
    old_name="serialNumber",
    new_name="cc_serialNumber",
    transformation_ctx="RenamedkeysforJointodoprivacyfilter_node1676250924960",
)

# Script generated for node Join to do privacy filter
Jointodoprivacyfilter_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforJointodoprivacyfilter_node1676250924960,
    keys1=["serialNumber"],
    keys2=["cc_serialNumber"],
    transformation_ctx="Jointodoprivacyfilter_node2",
)

# Script generated for node Aggregate
Aggregate_node1676240308041 = sparkAggregate(
    glueContext,
    parentFrame=Jointodoprivacyfilter_node2,
    groups=["timeStamp"],
    aggs=[["sensorReadingTime", "sum"], ["x", "sum"], ["y", "sum"], ["z", "sum"]],
    transformation_ctx="Aggregate_node1676240308041",
)

# Script generated for node machine learning Curated
machinelearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1676240308041,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machinelearningCurated_node3",
)

job.commit()
