# puffin-tools

# Building puffin-tools
puffin-tools is a standard Maven project. Simply run the following command from the project root directory:
```
./mvnw clean install -DskipTests
```

# Running puffin-tools
Run puffin-tools with `path` option:
```
target/puffin-tools-*-executable.jar --path path-to-puffin-file
```

Example of Puffin file with Apache DataSketches Theta:
```
=== properties ===
{created-by=Trino version testversion}

=== blob ===
type: apache-datasketches-theta-v1
inputFields: [1]
snapshotId: 3906904963261072048
sequenceNumber: 1
offset: 4
length: 69
compressionCodec: zstd
properties: {ndv=5}
ndv: 5
```

Example of Puffin file with deletion vector:
```
=== properties ===
{created-by=Trino version testversion}

=== blob ===
type: deletion-vector-v1
inputFields: [2147483645]
snapshotId: -1
sequenceNumber: -1
offset: 4
length: 46
compressionCodec: null
properties: {referenced-data-file=file:/var/folders/9s/_zwn4r_n2_9bp0krllp1pl3c0000gp/T/iceberg_query_runner3834650462397639736/tpch/region-7f7f14068e024ec8a73c07388a1231ce/data/20250912_112723_00023_x6wvw-0c0bfe2a-8447-4e4c-b60a-af1b6c40913f.parquet, cardinality=3}
deletedRows: [0, 2, 4]
```
