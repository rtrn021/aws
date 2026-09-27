# # import sys
# # from awsglue.transforms import *
# # from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# # from awsglue.context import GlueContext
# # from awsglue.job import Job
#
# args = getResolvedOptions(sys.argv, ["JOB_NAME"])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args["JOB_NAME"], args)
#
# # Script generated for node S3 bucket
# S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
#     format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
#     connection_type="s3",
#     format="csv",
#     connection_options={"paths": ["s3://rt-stag/csv/pokemon.csv"], "recurse": True},
#     transformation_ctx="S3bucket_node1",
# )
#
# # Script generated for node ApplyMapping
# ApplyMapping_node2 = ApplyMapping.apply(
#     frame=S3bucket_node1,
#     mappings=[
#         ("col0", "long", "col0", "long"),
#         ("col1", "string", "col1", "string"),
#         ("col2", "string", "col2", "string"),
#         ("col3", "string", "col3", "string"),
#         ("col4", "long", "col4", "long"),
#         ("col5", "long", "col5", "long"),
#         ("col6", "long", "col6", "long"),
#         ("col7", "long", "col7", "long"),
#         ("col8", "long", "col8", "long"),
#         ("col9", "long", "col9", "long"),
#         ("col10", "long", "col10", "long"),
#         ("col11", "boolean", "col11", "boolean"),
#     ],
#     transformation_ctx="ApplyMapping_node2",
# )
#
# # Script generated for node S3 bucket
# S3bucket_node3 = glueContext.getSink(
#     path="s3://rt-stag/output/",
#     connection_type="s3",
#     updateBehavior="UPDATE_IN_DATABASE",
#     partitionKeys=[],
#     enableUpdateCatalog=True,
#     transformation_ctx="S3bucket_node3",
# )
# S3bucket_node3.setCatalogInfo(catalogDatabase="db1", catalogTableName="pokemon")
# S3bucket_node3.setFormat("glueparquet")
# S3bucket_node3.writeFrame(ApplyMapping_node2)
# job.commit()