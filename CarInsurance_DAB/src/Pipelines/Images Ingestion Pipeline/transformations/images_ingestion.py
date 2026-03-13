from pyspark import pipelines as dp

@dp.table(
    name="`smart-claims-dev`.`01_bronze`.training_images",
    comment="Raw accident training image ingested from Azure Blob", 
    table_properties={"quality": "bronze"}
)
def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "BINARYFILE")
        .load(f"/Volumes/smart-claims-dev/00_landing/training_imgs"))
    

@dp.table(
    name="claim_images_meta",
    comment="Raw accident claim images metadata ingested from Azure Blob", 
    table_properties={"quality": "bronze"}
)
def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"/Volumes/smart-claims-dev/00_landing/claims/claims/metadata/"))
